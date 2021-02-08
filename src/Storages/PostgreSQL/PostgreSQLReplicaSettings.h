#pragma once

#include <Core/BaseSettings.h>

namespace DB
{
    class ASTStorage;


#define LIST_OF_POSTGRESQL_REPLICA_SETTINGS(M) \
    M(String, postgresql_replication_slot_name, "", "PostgreSQL replication slot name.", 0) \
    M(String, postgresql_publication_name, "", "PostgreSQL publication name.", 0) \
    M(UInt64, postgresql_max_block_size, 0, "Number of row collected before flushing data into table.", 0) \

DECLARE_SETTINGS_TRAITS(PostgreSQLReplicaSettingsTraits, LIST_OF_POSTGRESQL_REPLICA_SETTINGS)

struct PostgreSQLReplicaSettings : public BaseSettings<PostgreSQLReplicaSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
