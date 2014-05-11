/**
 * Module dependencies
 */

var Writable = require('stream').Writable;
var _ = require('lodash');
var path = require('path');
var Concatable = require('concat-stream');


var mockfs = [
  {path: '/foo'},
  {path: '/foo/bar'},
  {path: '/foo/baz'},
  {path: '/foo/baz/br.jpg', contents: 'blah'},
  {path: '/foo/baz/buz.png', contents: 'blah2'},
  /* e.g.
  {
    path: '/foo/bar',
    contents: 'j49q2GHDAOH$)QHASD)JQ@$KF)Q$CMD)VNCÅÎÍ˝‚›ÓÔÍÎÅÎÓ‚Œ€‹›'
  }
  // (if no 'contents', this is a directory.)
  */
];

/**
 * skipper-antagonist
 *
 * Fake adapter for receiving streams of file streams. Simulates a slow drain which will always be slower than incoming file uploads.  This provides a worst-case-scenario test case to ensure backpressure mechanisms are functioning properly, helping to protect us against memory overflow issues with streaming multipart file uploads via Skipper.
 *
 * Uses a mock filesystem to store files in RAM
 * i.e. it's pretend, like fantasy football or barbies or D&D or something.
 *
 * By the way, don't use this adapter for real things unless you know what you're doing.
 * You probably want to be using something like disk, S3, Swift (OpenStack), azure, dropbox, box.net, etc for real-world use cases.  This adapter is for testing.  That said,
 * if you want to store binary blobs in memory, go for it- I make no guarantees about this
 * being efficient or anything like that.
 *
 * @param  {Object} options
 * @return {Object}
 */

module.exports = function AnagonisticAdapter (options) {
  options = options || {};

  var adapter = {

    rm: function (filepath, cb){

      // INCLUDES grandchildren and other descendants
      // AND `filepath` ITSELF!
      var DESCENDANTS_EXPR = new RegExp('^' + toStripTrailingSlash()(filepath));

      mockfs = _(mockfs)
      .reject(function (file) {
        return ifMatch(DESCENDANTS_EXPR)(toDereference('path')(file));
      })
      .valueOf();

      if (!cb){
        throw new Error('streaming usage of ls() not yet supported for this adapter.');
      }
      else cb();
    },

    ls: function (dirpath, cb) {

      // Like `ls` on the command line:
      // Do not include grandchildren and other descendants
      // Also do not include the directory itself.
      // Only its direct children (directories and files).
      var CHILDFILES_EXPR = new RegExp('^' + toStripTrailingSlash()(dirpath) + '/[^/]+$');

      var matchingFilenames = _(mockfs)
      .map(toDereference('path'))
      .filter(ifMatch(CHILDFILES_EXPR))
      .map(toBasename())
      .valueOf();

      if (!cb){
        throw new Error('streaming usage of ls() not yet supported for this adapter.');
      }
      else cb(null, matchingFilenames);
    },

    read: function (filepath, cb) {
      var err = null;

      var file = _(mockfs)
      .where({path: toStripTrailingSlash()(filepath) })
      .first();
      if (!file) {
        err = new Error('Cannot read from path "'+filepath+'"- that\'s a directory!  Maybe you meant to use `ls()`?');
        err.code = 'ENOENT';
      }

      var contents = toDereference('contents')(file);
      if (typeof contents === 'undefined') {
        err = new Error('Cannot read from path "'+filepath+'"- that\'s a directory!  Maybe you meant to use `ls()`?');
        err.code = 'EISDIR';
      }

      if (!cb){
        throw new Error('streaming usage of ls() not yet supported for this adapter.');
      }
      else cb(err, contents);
    },

    /**
     * @param  {String} filepath
     * @param  {String} encoding
     * @return {Readable}
     * @api private
     */
    createWriteStream: function (filepath, encoding) {
      return Concatable(function whenFinished (contents) {
        // Reserve 'undefined' for folders-- coerce it to 'null' instead.
        if (typeof contents === 'undefined') {
          contents = null;
        }
        _(mockfs).reject({path: filepath});
        mockfs.push({path: filepath, contents: contents});
      });
    },

    receive: Receiver
  };

  return adapter;


  /**
   * A simple receiver for Skipper that writes Upstreams to
   * disk at the configured path.
   *
   * Includes a garbage-collection mechanism for failed
   * uploads.
   *
   * @param  {Object} options
   * @return {Stream.Writable}
   */
  function Receiver (options) {
    options = options || {};

    _.defaults(options, {

      // By default, create new files on disk
      // using their uploaded filenames.
      // (no overwrite-checking is performed!!)
      saveAs: function (__newFile) {
        return __newFile.filename;
      },

      // By default, upload files to `/`
      dirname: '/'
    });

    var receiver__ = Writable({
      objectMode: true
    });

    // This `_write` method is invoked each time a new file is received
    // from the Readable stream (Upstream) which is pumping filestreams
    // into this receiver.  (filename === `__newFile.filename`).
    receiver__._write = function onFile(__newFile, encoding, done) {

      // Determine location where file should be written:
      var dirPath = path.resolve(options.dirname);
      var filename = options.filename || options.saveAs(__newFile);
      var filePath = path.join(dirPath, filename);

      // Garbage-collect the bytes that were already written for this file.
      // (called when a read or write error occurs)
      function gc(err) {
        // console.log('************** Garbage collecting file `' + __newFile.filename + '` located @ ' + filePath + '...');
        adapter.rm(filePath, function (gcErr) {
          if (gcErr) return done([err].concat([gcErr]));
          else return done();
        });
      }

      // Write file stream
      var outs = adapter.createWriteStream(filePath, encoding);
      __newFile.on('error', function (err) {
        // console.log('***** READ error on file ' + __newFile.filename, '::', err);
      });
      outs.on('error', function failedToWriteFile(err) {
        // console.log('Error on output stream- garbage collecting unfinished uploads...');
        return gc(err);
      });
      outs.on('finish', function successfullyWroteFile() {
        done();
      });
      __newFile.pipe(outs);

    };

    return receiver__;
  } // </Receiver>


};









// just indulging my lisp-y sensibilities for a moment...
function toDereference (key) {
  return function _dereference (obj) {
    return obj[key];
  };
}
function ifMatch (matchExpr) {
  return function (str) {
    return str.match(matchExpr);
  };
}
function toBasename () {
  return function (somePath) {
    return path.basename(somePath);
  };
}
function toStripTrailingSlash () {
  return function (somePath) {
    return somePath.replace(/\/$/, '');
  };
}
