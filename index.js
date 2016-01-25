var hypercore = require('hypercore')
var fs = require('fs')
var bulk = require('bulk-write-stream')
var rabin = require('rabin')
var path = require('path')
var deltas = require('delta-list')
var from = require('from2')
var pump = require('pump')
var storage = require('./lib/storage')
var messages = require('./lib/messages')

module.exports = Hyperdrive

function Hyperdrive (db) {
  if (!(this instanceof Hyperdrive)) return new Hyperdrive(db)

  this.db = db
  this.core = hypercore(db, {
    storage: storage
  })
}

Hyperdrive.prototype.createPeerStream = function () {
  return this.core.createPeerStream()
}

Hyperdrive.prototype.get = function (id, folder) {
  return new Archive(this, folder, id)
}

Hyperdrive.prototype.add = function (folder) {
  return new Archive(this, folder, null)
}

function Archive (drive, folder, id) {
  var self = this

  this.id = id
  this.directory = folder
  this.core = drive.core
  this.entries = 0

  if (!id) {
    this.feed = this.core.add({filename: null})
  } else {
    this.feed = this.core.get(id)
  }

  this.feed.once('update', onupdate)

  function onupdate () {
    self.entries = self.feed.blocks
  }
}

Archive.prototype.ready = function (cb) {
  this.feed.ready(cb)
}

Archive.prototype.select = function (i, cb) {
  if (!cb) cb = noop
  var self = this
  this.get(i, function (err, data) {
    if (err) return cb(err)
    if (!data || !data.link) return cb(null)
    var feed = self.core.get(data.link.id, {
      filename: path.join(self.directory, data.name)
    })
    feed.open(cb)
  })
}

Archive.prototype.deselect = function (i, cb) {
  throw new Error('not yet implemented')
}

Archive.prototype.get = function (i, cb) {
  this.feed.get(i, function (err, data) {
    if (err) return cb(err)
    if (!data) return cb(null, null)
    cb(null, messages.Entry.decode(data))
  })
}

Archive.prototype.finalize = function (cb) {
  if (!cb) cb = noop
  var self = this
  this.feed.finalize(function (err) {
    if (err) return cb(err)
    self.id = self.feed.id
    self.entries = self.feed.blocks
    cb()
  })
}

Archive.prototype.createEntryStream = function (opts) {
  if (!opts) opts = {}
  var start = opts.start || 0
  var limit = opts.limit || Infinity
  var self = this
  return from.obj(read)

  function read (size, cb) {
    if (limit-- === 0) return cb(null, null)
    self.get(start++, cb)
  }
}

Archive.prototype.createFileCursor = function (i, opts) {
  throw new Error('not yet implemented')
}

Archive.prototype.createFileReadStream = function (i, opts) { // TODO: expose random access stuff
  if (!opts) opts = {}
  var start = opts.start || 0
  var limit = opts.limit || Infinity
  var self = this
  var feed = null
  return from.obj(read)

  function read (size, cb) {
    if (feed) {
      if (limit-- === 0) return cb(null, null)
      feed.get(start++, cb)
      return
    }

    self.get(i, function (err, entry) {
      if (err) return cb(err)
      if (!entry.link) return cb(null, null)
      feed = self.core.get(entry.link.id)
      limit = Math.min(limit, entry.link.blocks - entry.link.index.length)
      read(0, cb)
    })
  }
}

Archive.prototype.createFileWriteStream = function (entry) {
  throw new Error('not yet implemented')
}

Archive.prototype.append = function (entry, cb) {
  throw new Error('not yet implemented')
}

Archive.prototype.appendFile = function (filename, cb) {
  if (!cb) cb = noop

  var self = this
  var feed = this.core.add({filename: path.join(this.directory, filename)})

  pump(
    fs.createReadStream(filename),
    rabin(),
    bulk(write, end),
    cb
  )

  return feed

  function write (buffers, cb) {
    feed.append(buffers, cb)
  }

  function end (cb) {
    feed.finalize(function () {
      self.feed.append(messages.Entry.encode({
        name: filename,
        link: {
          id: feed.id,
          blocks: feed.blocks,
          index: deltas.pack(feed._storage._index)
        }
      }), done)
    })

    function done (err) {
      if (err) return cb(err)
      self.entries++
      cb(null)
    }
  }
}

function noop () {}

var drive = Hyperdrive(require('memdb')())

var archive = drive.add('.')

archive.appendFile('index.js', function (err) {
  if (err) throw err
  console.log(archive.entries)
  // archive.get(0, console.log)
  // archive.createEntryStream().on('data', console.log)
  archive.select(0)
  archive.get(0, console.log)
  archive.createFileReadStream(0).on('data', console.log)
})

