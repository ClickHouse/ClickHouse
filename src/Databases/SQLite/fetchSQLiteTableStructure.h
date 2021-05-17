#pragma once

#include <sqlite3.h>

#include <Storages/StorageSQLite.h>

namespace DB
{
std::shared_ptr<NamesAndTypesList> fetchSQLiteTableStructure(sqlite3 * connection, const String & sqlite_table_name /* , bool use_nulls */);

}
