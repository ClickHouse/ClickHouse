#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>

namespace DB
{
class ASTStorage;
struct DatabaseReplicatedSettingsImpl;

/// List of available types supported in ReplicatedSettings object
#define DATABASE_REPLICATED_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64)

DATABASE_REPLICATED_SETTINGS_SUPPORTED_TYPES(DatabaseReplicatedSettings, DECLARE_SETTING_TRAIT)

struct DatabaseReplicatedSettings
{
    DatabaseReplicatedSettings();
    DatabaseReplicatedSettings(const DatabaseReplicatedSettings & settings);
    DatabaseReplicatedSettings(DatabaseReplicatedSettings && settings) noexcept;
    ~DatabaseReplicatedSettings();

    DATABASE_REPLICATED_SETTINGS_SUPPORTED_TYPES(DatabaseReplicatedSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);

private:
    std::unique_ptr<DatabaseReplicatedSettingsImpl> impl;
};

}
