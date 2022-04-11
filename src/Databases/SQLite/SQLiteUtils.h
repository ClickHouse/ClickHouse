#pragma once

#include "config_core.h"

#if USE_SQLITE
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <sqlite3.h>


namespace DB
{

using SQLitePtr = std::shared_ptr<sqlite3>;

SQLitePtr openSQLiteDB(const String & database_path, ContextPtr context, bool throw_on_error = true);

}

#endif
