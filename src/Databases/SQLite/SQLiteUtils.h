#pragma once

#include "config.h"

#if USE_SQLITE
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <sqlite3.h>

#include <string_view>


namespace DB
{

using SQLitePtr = std::shared_ptr<sqlite3>;

/// Quote an SQLite identifier with strict backquotes. Embedded backquotes are doubled, while every other byte stays literal.
String quoteSQLiteIdentifier(std::string_view identifier);

/// How `openSQLiteDB` opens the database file.
enum class SQLiteOpenMode : uint8_t
{
    /// `SQLITE_OPEN_READONLY`: for connections that only read data or metadata (scans, schema fetches, table
    /// discovery). A read must not modify the file as a side effect: a read-write connection would roll back a hot
    /// journal left by a crashed writer and delete it, while a read-only one fails with `SQLITE_READONLY_ROLLBACK`
    /// instead. A missing file is never created.
    ReadOnly,
    /// `SQLITE_OPEN_READWRITE`: for connections that write. A missing file is not created, so that a still-missing
    /// file surfaces an error instead of silently fabricating an empty database.
    ReadWrite,
    /// `SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE`: a missing database file is created (as `sqlite3_open` does).
    ReadWriteCreate,
};

SQLitePtr openSQLiteDB(const String & database_path, ContextPtr context, bool throw_on_error, SQLiteOpenMode mode);

}

#endif
