#include <Common/logger_useful.h>
#include <Common/SettingsChanges.h>
#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Databases/DatabaseReplicatedSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_SETTING;
}

#define LIST_OF_DATABASE_REPLICATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Float, max_broken_tables_ratio, 1, "Do not recover replica automatically if the ratio of staled tables to all tables is greater", 0) \
    DECLARE(NonZeroUInt64, max_replication_lag_to_enqueue, 50, "Replica will throw exception on attempt to execute query if its replication lag greater. Must be greater than 0", 0) \
    DECLARE(UInt64, wait_entry_commited_timeout_sec, 3600, "Replicas will try to cancel query if timeout exceed, but initiator host has not executed it yet", 0) \
    DECLARE(String, collection_name, "", "A name of a collection defined in server's config where all info for cluster authentication is defined", 0) \
    DECLARE(Bool, check_consistency, true, "Check consistency of local metadata and metadata in Keeper, do replica recovery on inconsistency", 0) \
    DECLARE(UInt64, max_retries_before_automatic_recovery, 10, "Max number of attempts to execute a queue entry before marking replica as lost recovering it from snapshot (0 means infinite)", 0) \
    DECLARE(Bool, allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views, false, "If enabled, when processing DDLs in Replicated databases, it skips creating and exchanging DDLs of the temporary tables of refreshable materialized views if possible", 0) \
    DECLARE(NonZeroUInt32, logs_to_keep, 1000, "Default number of logs to keep in ZooKeeper for Replicated database. Bounded by the DDL log counter, which is 32-bit, so the value must not exceed 4294967295.", 0) \
    DECLARE(String, default_replica_path, "/clickhouse/databases/{uuid}", "The path to the database in ZooKeeper. Used during database creation if arguments are omitted.", 0) \
    DECLARE(String, default_replica_shard_name, "{shard}", "The shard name of the replica in the database. Used during database creation if arguments are omitted.", 0) \
    DECLARE(String, default_replica_name, "{replica}", "The name of the replica in the database. Used during database creation if arguments are omitted.", 0) \
    DECLARE(Bool, internal_replication, false, "Whether a Distributed table created with the cluster of this Replicated database will send data to one of replicas (internal replication means that cluster's replicas do replication by themselves) or to all replicas (no internal replication means that the Distributed table will send the inserted data to all of the replicas)", 0) \

DECLARE_SETTINGS_TRAITS(DatabaseReplicatedSettingsTraits, LIST_OF_DATABASE_REPLICATED_SETTINGS, DATABASE_REPLICATED_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(DatabaseReplicatedSettingsTraits, LIST_OF_DATABASE_REPLICATED_SETTINGS, DatabaseReplicatedSettings, DatabaseReplicatedSetting)

DatabaseReplicatedSettings::DatabaseReplicatedSettings() : impl(std::make_unique<DatabaseReplicatedSettingsImpl>())
{
}

DatabaseReplicatedSettings::DatabaseReplicatedSettings(const DatabaseReplicatedSettings & settings)
    : impl(std::make_unique<DatabaseReplicatedSettingsImpl>(*settings.impl))
{
}

DatabaseReplicatedSettings::DatabaseReplicatedSettings(DatabaseReplicatedSettings && settings) noexcept = default;

DatabaseReplicatedSettings::~DatabaseReplicatedSettings() = default;

DATABASE_REPLICATED_SETTINGS_SUPPORTED_TYPES(DatabaseReplicatedSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

namespace
{
constexpr UInt64 MAX_LOGS_TO_KEEP = std::numeric_limits<UInt32>::max();

/// `logs_to_keep` is `NonZeroUInt32` because the quantity it is compared against is 32-bit by domain:
/// `max_log_ptr` is derived from the Keeper node value, so it's inherently 32-bit. Any value greater
/// than `UInt32::max` is effectively "Keep all the log entries".
///
/// We reject definitions the user supplies now (`CREATE` and full-syntax `ATTACH`) with a
/// `logs_to_keep` setting value out of [1; UInt32::max] range.
/// However, metadata written by an older server, and old server configs, may still contain values
/// greater than `UInt32::max`; when such a value is replayed (server startup, short-syntax `ATTACH`,
/// RESTORE) or read from the config, we clamp it to `UInt32::max` with a corresponding warning
/// message in the logs. Metadata files/server config remain intact.
void checkOrClampLogsToKeep(Field & logs_to_keep, bool clamp_on_overflow)
{
    /// Convert through the type the setting had before it was narrowed, so that everything an
    /// older server accepted is still accepted here and only the range check is new.
    SettingFieldNonZeroUInt64 wide_value;
    wide_value = logs_to_keep;
    if (wide_value.value <= MAX_LOGS_TO_KEEP)
        return;

    if (!clamp_on_overflow)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting `logs_to_keep` of a Replicated database must not exceed {}, got {}. "
            "The DDL log counter is 32-bit, so a larger value cannot take effect",
            MAX_LOGS_TO_KEEP,
            wide_value.value);

    LOG_WARNING(
        getLogger("DatabaseReplicatedSettings"),
        "Setting `logs_to_keep` of a Replicated database is {}, which exceeds the maximum of {}, so {} is used "
        "instead. The DDL log counter is 32-bit, so the value never took effect as written. The setting origin is "
        "left unchanged",
        wide_value.value,
        MAX_LOGS_TO_KEEP,
        MAX_LOGS_TO_KEEP);

    logs_to_keep = MAX_LOGS_TO_KEEP;
}

}

void DatabaseReplicatedSettings::loadFromQuery(ASTStorage & storage_def, bool loading_from_existing_metadata)
{
    if (storage_def.settings)
    {
        /// A copy, because clamping must not reach the AST the metadata file is written back from.
        SettingsChanges changes = storage_def.settings->changes;
        for (auto & change : changes)
        {
            /// The shorthand form carries no value of its own; `applyChange` rejects it for a non-Bool
            /// setting, and that is the error the operator should see.
            if (change.name != "logs_to_keep" || change.shorthand)
                continue;

            checkOrClampLogsToKeep(change.value, loading_from_existing_metadata /* clamp_on_overflow */);
        }

        impl->applyChanges(changes);
        return;
    }

    auto settings_ast = make_intrusive<ASTSetQuery>();
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
        {
            /// `logs_to_keep` used to be UInt64, some old configs may contain values greater than
            /// `UInt32::max`. Clamp them to `UInt32::max`.
            if (key == "logs_to_keep")
            {
                Field logs_to_keep = config.getString(config_elem + "." + key);
                checkOrClampLogsToKeep(logs_to_keep, true /* clamp_on_overflow */);
                impl->set(key, logs_to_keep);
            }
            else
            {
                impl->set(key, config.getString(config_elem + "." + key));
            }
        }
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
