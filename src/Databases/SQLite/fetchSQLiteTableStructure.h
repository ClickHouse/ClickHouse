#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_SQLITE

#include <Storages/StorageSQLite.h>
#include <sqlite3.h> // Y_IGNORE


namespace DB
{
std::shared_ptr<NamesAndTypesList> fetchSQLiteTableStructure(sqlite3 * connection,
                                                             const String & sqlite_table_name);
}

#endif
