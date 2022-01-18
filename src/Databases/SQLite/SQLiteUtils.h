#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_SQLITE
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <sqlite3.h> // Y_IGNORE


namespace DB
{

using SQLitePtr = std::shared_ptr<sqlite3>;

SQLitePtr openSQLiteDB(const String & database_path, ContextPtr context, bool throw_on_error = true);

}

#endif
