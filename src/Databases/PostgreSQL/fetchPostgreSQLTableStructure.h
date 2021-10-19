#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Core/PostgreSQL/ConnectionHolder.h>
#include <Core/NamesAndTypes.h>


namespace DB
{

struct PostgreSQLTableStructure
{
    std::shared_ptr<NamesAndTypesList> columns = nullptr;
    std::shared_ptr<NamesAndTypesList> primary_key_columns = nullptr;
    std::shared_ptr<NamesAndTypesList> replica_identity_columns = nullptr;
};

using PostgreSQLTableStructurePtr = std::unique_ptr<PostgreSQLTableStructure>;

std::set<String> fetchPostgreSQLTablesList(pqxx::connection & connection, const String & postgres_schema);

PostgreSQLTableStructure fetchPostgreSQLTableStructure(
    pqxx::connection & connection, const String & postgres_table_name, bool use_nulls = true);

template<typename T>
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
    T & tx, const String & postgres_table_name, bool use_nulls = true,
    bool with_primary_key = false, bool with_replica_identity_index = false);

template<typename T>
std::set<String> fetchPostgreSQLTablesList(T & tx, const String & postgres_schema);

}

#endif
