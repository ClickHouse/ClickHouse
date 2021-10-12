#pragma once
#include <Core/Defines.h>
#include <Core/BaseSettings.h>

namespace DB
{

class ASTStorage;

#define LIST_OF_DATABASE_REPLICATED_SETTINGS(M) \
    M(Float, max_broken_tables_ratio, 0.5, "Do not recover replica automatically if the ratio of staled tables to all tables is greater", 0) \
    M(UInt64, max_replication_lag_to_enqueue, 10, "Replica will throw exception on attempt to execute query if its replication lag greater", 0) \
    M(UInt64, wait_entry_commited_timeout_sec, 3600, "Replicas will try to cancel query if timeout exceed, but initiator host has not executed it yet", 0) \

DECLARE_SETTINGS_TRAITS(DatabaseReplicatedSettingsTraits, LIST_OF_DATABASE_REPLICATED_SETTINGS)


/** Settings for the MaterializeMySQL database engine.
  * Could be loaded from a CREATE DATABASE query (SETTINGS clause).
  */
struct DatabaseReplicatedSettings : public BaseSettings<DatabaseReplicatedSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
