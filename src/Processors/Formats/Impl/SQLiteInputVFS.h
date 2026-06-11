#pragma once

#include "config.h"

#if USE_SQLITE

#include <cstddef>
#include <exception>
#include <string>

namespace DB
{

class SeekableReadBuffer;

/// A read source for the VFS: a seekable buffer holding a SQLite database and its total size.
/// The buffer must support random access (seek); the size is queried via xFileSize.
struct SQLiteReadSource
{
    SeekableReadBuffer * buf;
    size_t size;

    /// SQLite C callbacks cannot propagate C++ exceptions, so when reading from the buffer
    /// throws, the VFS saves the exception here and returns an I/O error code to SQLite.
    /// The caller of the SQLite API should rethrow it to report the original error.
    std::exception_ptr exception;
};

/// Name of the SQLite VFS that reads a database from a ClickHouse SeekableReadBuffer
/// instead of a file on disk. Pass it as the last argument of sqlite3_open_v2.
extern const char * const sqlite_read_vfs_name;

/// Registers the read-only VFS. Thread-safe; the actual registration happens exactly once.
void initSQLiteReadVFS();

/// Encodes a SQLiteReadSource pointer into the file name understood by the VFS.
/// The pointer is passed as a portable lowercase hex string (no platform-dependent
/// pointer formatting), and decoded back in the VFS xOpen callback.
/// The pointer is non-const because the VFS saves exceptions into the source.
std::string encodeSQLiteVFSFileName(SQLiteReadSource * source);

}

#endif
