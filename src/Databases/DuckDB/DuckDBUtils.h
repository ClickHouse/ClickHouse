#pragma once

#include "config.h"

#if USE_DUCKDB
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <duckdb.hpp>


namespace DB
{

using DuckDBPtr = std::shared_ptr<duckdb::DuckDB>;

DuckDBPtr openDuckDB(const String & database_path, ContextPtr context, bool throw_on_error = true);

String quoteIdentifierDuckDB(const String & identifier);
String quoteStringDuckDB(const String & identifier);

}

#endif
