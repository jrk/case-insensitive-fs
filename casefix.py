#!/usr/bin/env python

from __future__ import with_statement

import logging

from errno import EACCES
from os.path import realpath
from sys import argv, exit
from threading import Lock

import os

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class CaseInsensitiveLoopback(LoggingMixIn, Operations):
    def __init__(self, root):
        self.root = realpath(root)
        self.rwlock = Lock()

    def __call__(self, op, path, *args):
        path = self._find(self.root + path)
        return super(CaseInsensitiveLoopback, self).__call__(op, path, *args)

    def access(self, path, mode):
        path = self._find(path)
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    def chmod(self, path, mode):
        path = self._find(path)
        return os.chmod(path, mode)

    def chown(self, path, uid, gid):
        path = self._find(path)
        return os.chown(path, uid, gid)

    def _find(self, path):
        """Search for a version of this file using case-insensitive semantics"""
        try:
            print "Searching for '%s'" % path
            os.stat(path)
            print "  found!"
            return path
        except OSError:
            print "  not found!"
            segs = path.split(os.path.sep)
            found_parent = os.path.sep
            assert segs[0] == '' # expect a leading /
            for i in range(1, len(segs)): # start after leading /
                try:
                    parent = os.path.sep.join(segs[:i+1])
                    print "  searching parent", i, parent
                    os.stat(parent)
                    print "    found"
                    found_parent = parent
                except OSError:
                    print "    NOT found"
                    break

            # does the found_parent dir contain a differently-cased version of the requested path?
            print found_parent, segs[i], os.listdir(found_parent)
            candidates = [f for f in os.listdir(found_parent) if f.lower() == segs[i].lower()]
            print '  Candidates:', candidates
            if candidates:
                if len(candidates) > 1:
                    self.log.warn('Case ambiguity: %s%s{%s}' % (found_parent, os.path.sep, ','.join(candidates)))
                segs[i] = candidates[0]
                path = os.path.sep.join(segs)
                if i < (len(segs)-1):
                    print 'recursing', i, len(segs)-1
                    path = self._find(path) # recursively search with the new case-corrected path segment
                else:
                    print 'not recursing', i, len(segs)-1
            print path

            # returns path unmodified if we were unable to find case-corrected candidates.
            # expects underlying command implementations to handle file-not-found correctly if so.
            return path

    def create(self, path, mode):
        path = self._find(path)
        return os.open(path, os.O_WRONLY | os.O_CREAT, mode)

    def flush(self, path, fh):
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        return os.fsync(fh)

    def getattr(self, path, fh=None):
        path = self._find(path)
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    getxattr = None

    def link(self, target, source):
        target = self._find(target)
        source = self._find(source)
        return os.link(source, target)

    listxattr = None

    def mkdir(self, path, mode):
        path = self._find(path)
        return os.mkdir(path, mode)

    def mknod(self, path, mode, dev):
        path = self._find(path)
        os.mknod(path, mode, dev)

    def open(self, path, flags):
        path = self._find(path)
        return os.open(path, flags)

    def read(self, path, size, offset, fh):
        path = self._find(path)
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        path = self._find(path)
        return ['.', '..'] + os.listdir(path)

    def readlink(self, path):
        path = self._find(path)
        return os.readlink(path)

    def release(self, path, fh):
        return os.close(fh)

    def rename(self, old, new):
        old = self._find(old)
        new = self._find(self.root + new)
        return os.rename(old, new)

    def rmdir(self, path):
        path = self._find(path)
        return os.rmdir(path)

    def statfs(self, path):
        path = self._find(path)
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def symlink(self, target, source):
        target = self._find(target)
        source = self._find(source)
        return os.symlink(source, target)

    def truncate(self, path, length, fh=None):
        path = self._find(path)
        with open(path, 'r+') as f:
            f.truncate(length)

    def unlink(self, path):
        path = self._find(path)
        os.unlink(path)

    def utimens(self, path, times=None):
        path = self._find(path)
        return os.utime(path, times)

    def write(self, path, data, offset, fh):
        path = self._find(path)
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    # logging.getLogger().setLevel(logging.DEBUG)
    LoggingMixIn.log.addHandler(logging.StreamHandler())

    fuse = FUSE(CaseInsensitiveLoopback(argv[1]), argv[2], foreground=True)
