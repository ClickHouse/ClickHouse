#pragma once

#include "config.h"

#if USE_SQLITE
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <sqlite3.h>


namespace DB
{

using SQLitePtr = std::shared_ptr<sqlite3>;

SQLitePtr openSQLiteDB(const String & database_path, ContextPtr context, bool throw_on_error = true);

/// Quotes an SQLite identifier (a column or table name) by wrapping it in double quotes and
/// doubling any embedded double quotes, so that arbitrary names can be safely embedded in SQL.
String quoteSQLiteIdentifier(const String & name);

}

#endif
