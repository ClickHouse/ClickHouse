#pragma once

#include "config.h"

#if USE_SQLITE

#include <string>

namespace arrow::io { class RandomAccessFile; }

namespace DB
{

/// Name of the SQLite VFS that reads a database from an arrow::io::RandomAccessFile
/// instead of a file on disk. Pass it as the last argument of sqlite3_open_v2.
extern const char * const sqlite_read_vfs_name;

/// Registers the read-only VFS. Thread-safe; the actual registration happens exactly once.
void initSQLiteReadVFS();

/// Encodes a RandomAccessFile pointer into the file name understood by the VFS.
/// The pointer is passed as a portable lowercase hex string (no platform-dependent
/// pointer formatting), and decoded back in the VFS xOpen callback.
std::string encodeSQLiteVFSFileName(arrow::io::RandomAccessFile * file);

}

#endif
