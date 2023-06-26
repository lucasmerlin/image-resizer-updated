'use strict';

var env, stream, util, client, bucket;

env    = require('../../config/environment_vars');
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
stream = require('stream');
util   = require('util');

try {
  // create an AWS S3 client with the config data
  client = new S3Client({
    credentials: {
      accessKeyId: process.env.S3_KEY,
      secretAccessKey: process.env.S3_SECRET,
      region: env.AWS_REGION,
    },
    endpoint: env.AWS_ENDPOINT,
    s3ForcePathStyle: true,
    signatureVersion: 'v4',
  });
  bucket = env.S3_BUCKET;
} catch(e) {
  console.error('Error creating S3 client:', e);
}


function s3Stream(image){
  /* jshint validthis:true */
  if (!(this instanceof s3Stream)){
    return new s3Stream(image);
  }
  stream.Readable.call(this, { objectMode : true });
  this.image = image;
  this.ended = false;
}

util.inherits(s3Stream, stream.Readable);

s3Stream.prototype._read = function(){
  var _this = this;

  if ( this.ended ){ return; }

  // pass through if there is an error on the image object
  if (this.image.isError()){
    this.ended = true;
    this.push(this.image);
    return this.push(null);
  }

  // Set the AWS options
  var awsOptions = {
    Bucket: bucket,
    Key: this.image.path.replace(/^\//,'')
  };

  const command = new GetObjectCommand(awsOptions);

  this.image.log.time('s3');

  client.send(command).then(data => {
    _this.image.contents = data.Body;
    _this.image.originalContentLength = data.Body.length;
  }).catch(err => {
    _this.image.error = err;
  }).finally(() => {
    _this.image.log.timeEnd('s3');
    _this.ended = true;
    _this.push(_this.image);
    _this.push(null);
  });
};


module.exports = s3Stream;
