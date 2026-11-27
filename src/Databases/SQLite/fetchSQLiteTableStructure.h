#pragma once

#include "config.h"

#if USE_SQLITE

#include <Storages/StorageSQLite.h>
#include <DataTypes/IDataType.h>
#include <sqlite3.h>


namespace DB
{
std::shared_ptr<NamesAndTypesList> fetchSQLiteTableStructure(sqlite3 * connection,
                                                             const String & sqlite_table_name);

/// Convert a SQLite declared column type (e.g. "INTEGER", "REAL", "TEXT") to a ClickHouse data type.
DataTypePtr convertSQLiteDataType(String type);
}

#endif
