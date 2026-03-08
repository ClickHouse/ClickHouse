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
struct DatabaseReplicatorSettingsImpl;

/// List of available types supported in ReplicatedSettings object
#define DATABASE_REPLICATOR_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, String)

DATABASE_REPLICATOR_SETTINGS_SUPPORTED_TYPES(DatabaseReplicatorSettings, DECLARE_SETTING_TRAIT)

struct DatabaseReplicatorSettings
{
    DatabaseReplicatorSettings();
    DatabaseReplicatorSettings(const DatabaseReplicatorSettings & settings);
    DatabaseReplicatorSettings(DatabaseReplicatorSettings && settings) noexcept;
    ~DatabaseReplicatorSettings();

    DATABASE_REPLICATOR_SETTINGS_SUPPORTED_TYPES(DatabaseReplicatorSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    /// Access the embedded DDL replicator settings.
    const DDLReplicatorSettings & getDDLReplicatorSettings() const { return ddl_replicator_settings; }
    DDLReplicatorSettings & getDDLReplicatorSettings() { return ddl_replicator_settings; }

    String toString() const;

private:
    std::unique_ptr<DatabaseReplicatorSettingsImpl> impl;
    DDLReplicatorSettings ddl_replicator_settings;
};

}
