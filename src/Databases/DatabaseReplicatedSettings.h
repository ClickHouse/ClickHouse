#pragma once
#include <Core/Defines.h>
#include <Core/BaseSettings.h>

namespace DB
{

class ASTStorage;

#define CONTEMPORARY_DATABASE_REPLICATED_SETTINGS(M) \
    M(Float,  max_broken_tables_ratio, 0.5, "Do not recover replica automatically if the ratio of staled tables to all tables is greater", 0) \
    M(UInt64, max_replication_lag_to_enqueue, 10, "Replica will throw exception on attempt to execute query if its replication lag greater", 0) \
    M(UInt64, wait_entry_commited_timeout_sec, 3600, "Replicas will try to cancel query if timeout exceed, but initiator host has not executed it yet", 0) \
    M(String, collection_name, "", "A name of a collection defined in server's config where all info for cluster authentication is defined", 0) \

#define MAKE_OBSOLETE_REPLICATED_DATABASE_SETTING(M, TYPE, NAME, DEFAULT) \
    M(TYPE, NAME, DEFAULT, "Obsolete setting, does nothing.", BaseSettingsHelpers::Flags::OBSOLETE)

#define OBSOLETE_DATABASE_REPLICATED_SETTINGS(M) \
    /** Obsolete settings that do nothing but left for compatibility reasons. Remove each one after half a year of obsolescence. */ \
    MAKE_OBSOLETE_REPLICATED_DATABASE_SETTING(M, String, cluster_username, "") \
    MAKE_OBSOLETE_REPLICATED_DATABASE_SETTING(M, String, cluster_password, "") \
    MAKE_OBSOLETE_REPLICATED_DATABASE_SETTING(M, Bool, cluster_secure_connection, false) \

#define LIST_OF_DATABASE_REPLICATED_SETTINGS(M)    \
    CONTEMPORARY_DATABASE_REPLICATED_SETTINGS(M)   \
    OBSOLETE_DATABASE_REPLICATED_SETTINGS(M)       \

DECLARE_SETTINGS_TRAITS(DatabaseReplicatedSettingsTraits, LIST_OF_DATABASE_REPLICATED_SETTINGS)


struct DatabaseReplicatedSettings : public BaseSettings<DatabaseReplicatedSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
