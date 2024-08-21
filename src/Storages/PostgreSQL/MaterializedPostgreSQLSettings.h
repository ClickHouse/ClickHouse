#pragma once

#include "config.h"

#if USE_LIBPQXX
#include <Core/BaseSettings.h>


namespace DB
{
    class ASTStorage;


#define LIST_OF_MATERIALIZED_POSTGRESQL_SETTINGS(M, ALIAS) \
    M(UInt64, materialized_postgresql_max_block_size, 65536, "Number of row collected before flushing data into table.", 0) \
    M(String, materialized_postgresql_tables_list, "", "List of tables for MaterializedPostgreSQL database engine", 0) \
    M(String, materialized_postgresql_schema_list, "", "List of schemas for MaterializedPostgreSQL database engine", 0) \
    M(String, materialized_postgresql_replication_slot, "", "A user-created replication slot", 0) \
    M(String, materialized_postgresql_snapshot, "", "User provided snapshot in case he manages replication slots himself", 0) \
    M(String, materialized_postgresql_schema, "", "PostgreSQL schema", 0) \
    M(Bool, materialized_postgresql_tables_list_with_schema, false, \
        "Consider by default that if there is a dot in tables list 'name.name', " \
        "then the first name is postgres schema and second is postgres table. This setting is needed to allow table names with dots", 0) \
    M(UInt64, materialized_postgresql_backoff_min_ms, 200, "Poll backoff start point", 0) \
    M(UInt64, materialized_postgresql_backoff_max_ms, 10000, "Poll backoff max point", 0) \
    M(UInt64, materialized_postgresql_backoff_factor, 2, "Poll backoff factor", 0) \

DECLARE_SETTINGS_TRAITS(MaterializedPostgreSQLSettingsTraits, LIST_OF_MATERIALIZED_POSTGRESQL_SETTINGS)

struct MaterializedPostgreSQLSettings : public BaseSettings<MaterializedPostgreSQLSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}

#endif
