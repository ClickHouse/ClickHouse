#pragma once
#include <Core/Defines.h>
#include <Core/BaseSettings.h>

namespace DB
{

class ASTStorage;

#define LIST_OF_DATABASE_REPLICATED_SETTINGS(M, ALIAS) \
    M(Float,  max_broken_tables_ratio, 1, "Do not recover replica automatically if the ratio of staled tables to all tables is greater", 0) \
    M(UInt64, max_replication_lag_to_enqueue, 50, "Replica will throw exception on attempt to execute query if its replication lag greater", 0) \
    M(UInt64, wait_entry_commited_timeout_sec, 3600, "Replicas will try to cancel query if timeout exceed, but initiator host has not executed it yet", 0) \
    M(String, collection_name, "", "A name of a collection defined in server's config where all info for cluster authentication is defined", 0) \
    M(Bool, check_consistency, true, "Check consistency of local metadata and metadata in Keeper, do replica recovery on inconsistency", 0) \
    M(UInt64, max_retries_before_automatic_recovery, 100, "Max number of attempts to execute a queue entry before marking replica as lost recovering it from snapshot (0 means infinite)", 0) \


DECLARE_SETTINGS_TRAITS(DatabaseReplicatedSettingsTraits, LIST_OF_DATABASE_REPLICATED_SETTINGS)


struct DatabaseReplicatedSettings : public BaseSettings<DatabaseReplicatedSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
