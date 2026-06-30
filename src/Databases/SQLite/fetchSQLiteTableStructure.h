#pragma once

#include "config.h"

#if USE_SQLITE

#include <Storages/StorageSQLite.h>
#include <sqlite3.h>

#include <unordered_set>


namespace DB
{
/// Returns the structure of a SQLite table. Generated columns (which `SELECT *` returns) are kept in
/// the structure so that they remain readable, but SQLite rejects explicit writes into them. When
/// `out_generated_columns` is provided, the names of the generated columns are collected into it so
/// that the write path can omit them from the explicit insert column list.
std::shared_ptr<NamesAndTypesList> fetchSQLiteTableStructure(sqlite3 * connection,
                                                             const String & sqlite_table_name,
                                                             std::unordered_set<String> * out_generated_columns = nullptr);
}

#endif
