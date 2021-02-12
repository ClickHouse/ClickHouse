#pragma once

#include <Core/BaseSettings.h>

namespace DB
{
    class ASTStorage;


#define LIST_OF_POSTGRESQL_REPLICA_SETTINGS(M) \
    M(UInt64, postgresql_max_block_size, 0, "Number of row collected before flushing data into table.", 0) \
    M(String, postgresql_tables_list, "", "List of tables for PostgreSQLReplica database engine", 0) \

DECLARE_SETTINGS_TRAITS(PostgreSQLReplicaSettingsTraits, LIST_OF_POSTGRESQL_REPLICA_SETTINGS)

struct PostgreSQLReplicaSettings : public BaseSettings<PostgreSQLReplicaSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
