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

/// `allow_create` controls whether a missing database file is implicitly created (as `sqlite3_open` does).
/// Pass `false` when reopening a persisted table whose file was unavailable at load time, so that a still-missing
/// file surfaces an error instead of silently fabricating an empty database.
SQLitePtr openSQLiteDB(const String & database_path, ContextPtr context, bool throw_on_error = true, bool allow_create = true);

}

#endif
