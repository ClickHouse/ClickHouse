#pragma once

#include <Storages/TableSetting.h>

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Interpreters/Context_fwd.h>

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
class ASTStorage;
class SettingsChanges;
struct DistributedSettingsImpl;

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

    static bool hasBuiltin(std::string_view name);
    /// Every setting of this instance, for `system.table_settings`. The caller refines `origin`.
    TableSettings enumerateSettings() const;
    /// The engine's own settings, for `system.engine_settings`.
    static TableSettings enumerateEngineSettings(ContextPtr context);

private:
    std::unique_ptr<DistributedSettingsImpl> impl;
};

}
