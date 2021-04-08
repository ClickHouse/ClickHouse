#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Core/PostgreSQL/PostgreSQLConnection.h>
#include <Core/NamesAndTypes.h>


namespace DB
{

std::unordered_set<std::string> fetchPostgreSQLTablesList(pqxx::connection & connection);

struct PostgreSQLTableStructure
{
    std::shared_ptr<NamesAndTypesList> columns;
    std::shared_ptr<NamesAndTypesList> primary_key_columns;
};

using PostgreSQLTableStructurePtr = std::unique_ptr<PostgreSQLTableStructure>;

PostgreSQLTableStructure fetchPostgreSQLTableStructure(
    pqxx::connection & connection, const String & postgres_table_name, bool use_nulls, bool with_primary_key = false);

template<typename T>
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
    std::shared_ptr<T> tx, const String & postgres_table_name, bool use_nulls, bool with_primary_key = false);

}

#endif
