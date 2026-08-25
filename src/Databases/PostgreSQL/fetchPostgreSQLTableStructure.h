#pragma once

#include "config.h"

#if USE_LIBPQXX
#include <Core/PostgreSQL/ConnectionHolder.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>

#include <functional>


namespace DB
{

/// Convert a PostgreSQL type (as returned by `format_type`, e.g. "integer", "numeric(10,2)", "text[]")
/// to a ClickHouse data type. `recheck_array` is called when an array's dimensions could not be
/// determined from `dimensions` and have to be rechecked separately.
DataTypePtr convertPostgreSQLDataType(String & type, std::function<void()> recheck_array, bool is_nullable = false, uint16_t dimensions = 0);

struct PostgreSQLTableStructure
{
    struct PGAttribute
    {
        Int32 atttypid;
        Int32 atttypmod;
        Int32 attnum;
        bool atthasdef;
        char attgenerated;
        std::string attr_def;
    };
    using Attributes = std::unordered_map<std::string, PGAttribute>;

    struct ColumnsInfo
    {
        NamesAndTypesList columns;
        Attributes attributes;
        std::vector<std::string> names;
        ColumnsInfo(NamesAndTypesList && columns_, Attributes && attributes_) : columns(columns_), attributes(attributes_) {}
    };
    using ColumnsInfoPtr = std::shared_ptr<ColumnsInfo>;

    ColumnsInfoPtr physical_columns;
    ColumnsInfoPtr primary_key_columns;
    ColumnsInfoPtr replica_identity_columns;
};

using PostgreSQLTableStructurePtr = std::unique_ptr<PostgreSQLTableStructure>;

/// We need order for materialized version.
std::set<String> fetchPostgreSQLTablesList(pqxx::connection & connection, const String & postgres_schema);

PostgreSQLTableStructure fetchPostgreSQLTableStructure(
    pqxx::connection & connection, const String & postgres_table, const String & postgres_schema, bool use_nulls = true);

template<typename T>
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
    T & tx, const String & postgres_table, const String & postgres_schema, bool use_nulls = true,
    bool with_primary_key = false, bool with_replica_identity_index = false, const Strings & columns = {});

template<typename T>
std::set<String> fetchPostgreSQLTablesList(T & tx, const String & postgres_schema);

}

#endif
