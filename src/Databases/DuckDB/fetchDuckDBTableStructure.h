#pragma once

#include "config.h"

#if USE_DUCKDB

#include <Storages/StorageSQLite.h>
#include <duckdb.hpp>


namespace DB
{
std::shared_ptr<NamesAndTypesList> fetchDuckDBTableStructure(duckdb::DuckDB * connection,
                                                             const String & duckdb_table_name);
}

#endif
