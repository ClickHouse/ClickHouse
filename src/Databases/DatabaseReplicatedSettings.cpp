#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Databases/DatabaseReplicatedSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

#define LIST_OF_DATABASE_REPLICATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Float,  max_broken_tables_ratio, 1, "Do not recover replica automatically if the ratio of staled tables to all tables is greater", 0) \
    DECLARE(UInt64, max_replication_lag_to_enqueue, 50, "Replica will throw exception on attempt to execute query if its replication lag greater", 0) \
    DECLARE(UInt64, wait_entry_commited_timeout_sec, 3600, "Replicas will try to cancel query if timeout exceed, but initiator host has not executed it yet", 0) \
    DECLARE(String, collection_name, "", "A name of a collection defined in server's config where all info for cluster authentication is defined", 0) \
    DECLARE(Bool, check_consistency, true, "Check consistency of local metadata and metadata in Keeper, do replica recovery on inconsistency", 0) \
    DECLARE(UInt64, max_retries_before_automatic_recovery, 100, "Max number of attempts to execute a queue entry before marking replica as lost recovering it from snapshot (0 means infinite)", 0) \

DECLARE_SETTINGS_TRAITS(DatabaseReplicatedSettingsTraits, LIST_OF_DATABASE_REPLICATED_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(DatabaseReplicatedSettingsTraits, LIST_OF_DATABASE_REPLICATED_SETTINGS)

struct DatabaseReplicatedSettingsImpl : public BaseSettings<DatabaseReplicatedSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    DatabaseReplicatedSettings##TYPE NAME = &DatabaseReplicatedSettingsImpl ::NAME;

namespace DatabaseReplicatedSetting
{
LIST_OF_DATABASE_REPLICATED_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
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

}
