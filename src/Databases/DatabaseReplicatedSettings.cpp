#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Databases/DatabaseReplicatedSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_SETTING;
}

#define LIST_OF_DATABASE_REPLICATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Float,  max_broken_tables_ratio, 1, "Do not recover replica automatically if the ratio of staled tables to all tables is greater", 0) \
    DECLARE(UInt64, max_replication_lag_to_enqueue, 50, "Replica will throw exception on attempt to execute query if its replication lag greater", 0) \
    DECLARE(UInt64, wait_entry_commited_timeout_sec, 3600, "Replicas will try to cancel query if timeout exceed, but initiator host has not executed it yet", 0) \
    DECLARE(String, collection_name, "", "A name of a collection defined in server's config where all info for cluster authentication is defined", 0) \
    DECLARE(Bool, check_consistency, true, "Check consistency of local metadata and metadata in Keeper, do replica recovery on inconsistency", 0) \
    DECLARE(UInt64, max_retries_before_automatic_recovery, 10, "Max number of attempts to execute a queue entry before marking replica as lost recovering it from snapshot (0 means infinite)", 0) \
    DECLARE(Bool, allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views, false, "If enabled, when processing DDLs in Replicated databases, it skips creating and exchanging DDLs of the temporary tables of refreshable materialized views if possible", 0) \
    DECLARE(NonZeroUInt64, logs_to_keep, 1000, "Default number of logs to keep in ZooKeeper for Replicated database.", 0) \
    DECLARE(String, default_replica_path, "/clickhouse/databases/{uuid}", "The path to the database in ZooKeeper. Used during database creation if arguments are omitted.", 0) \
    DECLARE(String, default_replica_shard_name, "{shard}", "The shard name of the replica in the database. Used during database creation if arguments are omitted.", 0) \
    DECLARE(String, default_replica_name, "{replica}", "The name of the replica in the database. Used during database creation if arguments are omitted.", 0) \

DECLARE_SETTINGS_TRAITS(DatabaseReplicatedSettingsTraits, LIST_OF_DATABASE_REPLICATED_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(DatabaseReplicatedSettingsTraits, LIST_OF_DATABASE_REPLICATED_SETTINGS)

struct DatabaseReplicatedSettingsImpl : public BaseSettings<DatabaseReplicatedSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) \
    DatabaseReplicatedSettings##TYPE NAME = &DatabaseReplicatedSettingsImpl ::NAME;

namespace DatabaseReplicatedSetting
{
LIST_OF_DATABASE_REPLICATED_SETTINGS(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
}

#undef INITIALIZE_SETTING_EXTERN

DatabaseReplicatedSettings::DatabaseReplicatedSettings() : impl(std::make_unique<DatabaseReplicatedSettingsImpl>())
{
}

DatabaseReplicatedSettings::DatabaseReplicatedSettings(const DatabaseReplicatedSettings & settings)
    : impl(std::make_unique<DatabaseReplicatedSettingsImpl>(*settings.impl))
{
}

DatabaseReplicatedSettings::DatabaseReplicatedSettings(DatabaseReplicatedSettings && settings) noexcept
    : impl(std::make_unique<DatabaseReplicatedSettingsImpl>(std::move(*settings.impl)))
{
}

DatabaseReplicatedSettings::~DatabaseReplicatedSettings() = default;

DATABASE_REPLICATED_SETTINGS_SUPPORTED_TYPES(DatabaseReplicatedSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void DatabaseReplicatedSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        impl->applyChanges(storage_def.settings->changes);
        return;
    }

    auto settings_ast = std::make_shared<ASTSetQuery>();
    settings_ast->is_standalone = false;
    storage_def.set(storage_def.settings, settings_ast);
}

void DatabaseReplicatedSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (const String & key : config_keys)
            impl->set(key, config.getString(config_elem + "." + key));
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in DatabaseReplicated config");
        throw;
    }
}

String DatabaseReplicatedSettings::toString() const
{
    return impl->toString();
}
}
