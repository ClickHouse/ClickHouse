#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Storages/SettingDescription.h>

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
class ASTStorage;
class SettingsChanges;
struct DistributedSettingsImpl;
struct Settings;

/// List of available types supported in DistributedSettings object
#define DISTRIBUTED_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Milliseconds) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, SkipUnavailableShardsMode)

DISTRIBUTED_SETTINGS_SUPPORTED_TYPES(DistributedSettings, DECLARE_SETTING_TRAIT)

/** Settings for the Distributed family of engines.
  */
struct DistributedSettings
{
    DistributedSettings();
    DistributedSettings(const DistributedSettings & settings);
    DistributedSettings(DistributedSettings && settings) noexcept;
    ~DistributedSettings();

    DISTRIBUTED_SETTINGS_SUPPORTED_TYPES(DistributedSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
    void loadFromQuery(ASTStorage & storage_def);
    void applyChanges(const SettingsChanges & changes);
    /// Fills the `background_insert_*` settings the table does not state from the `distributed_background_insert_*`
    /// query settings, as a new table does.
    void applyBackgroundInsertDefaults(const Settings & query_settings);

    static bool hasBuiltin(std::string_view name);
    SettingDescriptions enumerateSettings() const;
    /// For `system.engine_settings`: the server-level instance a new table starts from, with the `background_insert_*`
    /// settings filled from the global context, which the engine's creator reads them from.
    static SettingDescriptions enumerateEngineSettings(ContextPtr context);

private:
    std::unique_ptr<DistributedSettingsImpl> impl;
};

}
