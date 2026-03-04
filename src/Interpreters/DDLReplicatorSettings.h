#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsChangesHistory.h>
#include <Core/SettingsFields.h>


namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
}
namespace DB
{
struct DDLReplicatorSettingsImpl;
struct SettingChange;

/// List of available types supported in DDLReplicatorSettings object
#define DDL_REPLICATOR_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, NonZeroUInt64)

DDL_REPLICATOR_SETTINGS_SUPPORTED_TYPES(DDLReplicatorSettings, DECLARE_SETTING_TRAIT)

struct DDLReplicatorSettings
{
    DDLReplicatorSettings();
    DDLReplicatorSettings(const DDLReplicatorSettings & settings);
    DDLReplicatorSettings(DDLReplicatorSettings && settings) noexcept;
    ~DDLReplicatorSettings();

    DDL_REPLICATOR_SETTINGS_SUPPORTED_TYPES(DDLReplicatorSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    /// Check whether the given name is a built-in DDLReplicator setting.
    static bool hasBuiltin(std::string_view name);

    /// Apply a single SettingChange or a batch.
    void applyChange(const SettingChange & change);

    /// Set a setting by name and string value.
    void set(const String & name, const String & value);

    String toString() const;

private:
    std::unique_ptr<DDLReplicatorSettingsImpl> impl;
};

}
