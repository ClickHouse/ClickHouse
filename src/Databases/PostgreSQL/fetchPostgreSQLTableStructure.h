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

std::set<std::string> fetchPostgreSQLTablesList(pqxx::connection & connection);

PostgreSQLTableStructure fetchPostgreSQLTableStructure(
    pqxx::connection & connection, const String & postgres_table, const String & postgres_schema, bool use_nulls = true);

template<typename T>
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
    T & tx, const String & postgres_table, const String & postgres_schema, bool use_nulls = true,
    bool with_primary_key = false, bool with_replica_identity_index = false);

template<typename T>
std::set<std::string> fetchPostgreSQLTablesList(T & tx);

}

#endif
