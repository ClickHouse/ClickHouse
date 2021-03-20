#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Core/BaseSettings.h>


namespace DB
{
    class ASTStorage;


#define LIST_OF_POSTGRESQL_REPLICA_SETTINGS(M) \
    M(UInt64, postgresql_replica_max_block_size, 65536, "Number of row collected before flushing data into table.", 0) \
    M(String, postgresql_replica_tables_list, "", "List of tables for MaterializePostgreSQL database engine", 0) \
    M(Bool, postgresql_replica_allow_minimal_ddl, 0, "Allow to track minimal possible ddl. By default, table after ddl will get into a skip list", 0) \

DECLARE_SETTINGS_TRAITS(MaterializePostgreSQLSettingsTraits, LIST_OF_POSTGRESQL_REPLICA_SETTINGS)

struct MaterializePostgreSQLSettings : public BaseSettings<MaterializePostgreSQLSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}

#endif
