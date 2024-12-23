#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
class ASTStorage;
struct DistributedSettingsImpl;

/// List of available types supported in DistributedSettings object
#define DISTRIBUTED_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Milliseconds) \
    M(CLASS_NAME, UInt64)

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

private:
    std::unique_ptr<DistributedSettingsImpl> impl;
};

}
