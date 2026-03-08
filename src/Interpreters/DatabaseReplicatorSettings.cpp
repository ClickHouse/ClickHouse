#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Interpreters/DatabaseReplicatorSettings.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_SETTING;
}

#define LIST_OF_DATABASE_REPLICATOR_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, zookeeper_path, "/clickhouse/database_catalog", "The path to the database replicator in ZooKeeper.", 0) \
    DECLARE(String, shard_name, "{shard}", "The shard name of the replica.", 0) \
    DECLARE(String, replica_name, "{replica}", "The name of the replica.", 0) \

DECLARE_SETTINGS_TRAITS(DatabaseReplicatorSettingsTraits, LIST_OF_DATABASE_REPLICATOR_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(DatabaseReplicatorSettingsTraits, LIST_OF_DATABASE_REPLICATOR_SETTINGS)

struct DatabaseReplicatorSettingsImpl : public BaseSettings<DatabaseReplicatorSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) \
    DatabaseReplicatorSettings##TYPE NAME = &DatabaseReplicatorSettingsImpl ::NAME;

namespace DatabaseReplicatorSetting
{
LIST_OF_DATABASE_REPLICATOR_SETTINGS(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
}

#undef INITIALIZE_SETTING_EXTERN

DatabaseReplicatorSettings::DatabaseReplicatorSettings() : impl(std::make_unique<DatabaseReplicatorSettingsImpl>())
{
}

DatabaseReplicatorSettings::DatabaseReplicatorSettings(const DatabaseReplicatorSettings & settings)
    : impl(std::make_unique<DatabaseReplicatorSettingsImpl>(*settings.impl))
    , ddl_replicator_settings(settings.ddl_replicator_settings)
{
}

DatabaseReplicatorSettings::DatabaseReplicatorSettings(DatabaseReplicatorSettings && settings) noexcept
    : impl(std::make_unique<DatabaseReplicatorSettingsImpl>(std::move(*settings.impl)))
    , ddl_replicator_settings(std::move(settings.ddl_replicator_settings))
{
}

DatabaseReplicatorSettings::~DatabaseReplicatorSettings() = default;

DATABASE_REPLICATOR_SETTINGS_SUPPORTED_TYPES(DatabaseReplicatorSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void DatabaseReplicatorSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (const String & key : config_keys)
        {
            const String value = config.getString(config_elem + "." + key);
            if (DDLReplicatorSettings::hasBuiltin(key))
                ddl_replicator_settings.set(key, value);
            else
                impl->set(key, value);
        }
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in DatabaseReplicator config");
        throw;
    }
}

String DatabaseReplicatorSettings::toString() const
{
    return impl->toString();
}
}
