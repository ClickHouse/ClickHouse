#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>
#include <Interpreters/DDLReplicatorSettings.h>


namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
}
namespace DB
{
class ASTStorage;
struct DatabaseReplicatedSettingsImpl;

/// List of available types supported in ReplicatedSettings object
#define DATABASE_REPLICATED_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, String)

DATABASE_REPLICATED_SETTINGS_SUPPORTED_TYPES(DatabaseReplicatedSettings, DECLARE_SETTING_TRAIT)

struct DatabaseReplicatedSettings
{
    DatabaseReplicatedSettings();
    DatabaseReplicatedSettings(const DatabaseReplicatedSettings & settings);
    DatabaseReplicatedSettings(DatabaseReplicatedSettings && settings) noexcept;
    ~DatabaseReplicatedSettings();

    DATABASE_REPLICATED_SETTINGS_SUPPORTED_TYPES(DatabaseReplicatedSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    /// Access the embedded DDL replicator settings.
    const DDLReplicatorSettings & getDDLReplicatorSettings() const { return ddl_replicator_settings; }
    DDLReplicatorSettings & getDDLReplicatorSettings() { return ddl_replicator_settings; }

    String toString() const;

private:
    std::unique_ptr<DatabaseReplicatedSettingsImpl> impl;
    DDLReplicatorSettings ddl_replicator_settings;
};

}
