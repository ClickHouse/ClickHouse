#pragma once

#include "config_core.h"

#if USE_SQLITE

#include <Storages/StorageSQLite.h>
#include <sqlite3.h>


namespace DB
{
std::shared_ptr<NamesAndTypesList> fetchSQLiteTableStructure(sqlite3 * connection,
                                                             const String & sqlite_table_name);
}

#endif
