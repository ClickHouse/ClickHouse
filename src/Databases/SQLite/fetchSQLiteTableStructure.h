#pragma once

#include "config.h"

#if USE_SQLITE

#include <Storages/ColumnsDescription.h>
#include <DataTypes/IDataType.h>
#include <sqlite3.h>

#include <optional>


namespace DB
{
/// Returns the structure of a SQLite table, or `std::nullopt` if the table has no columns.
///
/// Generated columns (which `SELECT *` returns) are kept in the structure so they remain readable,
/// but they are marked `MATERIALIZED`: SQLite computes them, so ClickHouse must treat them as
/// non-insertable. An explicit write into a generated column is then rejected, and an insert without
/// a column list targets only the base columns - matching SQLite's own insert contract.
std::optional<ColumnsDescription> fetchSQLiteTableStructure(sqlite3 * connection, const String & sqlite_table_name);

/// Convert a SQLite declared column type (e.g. "INTEGER", "REAL", "TEXT") to a ClickHouse data type.
DataTypePtr convertSQLiteDataType(String type);
}

#endif
