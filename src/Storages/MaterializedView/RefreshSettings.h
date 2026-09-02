#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>

namespace DB
{
struct RefreshSettingsImpl;
class SettingsChanges;

/// List of available types supported in RabbitMQSettings object
#define REFRESH_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Int64) \
    M(CLASS_NAME, UInt64)

REFRESH_SETTINGS_SUPPORTED_TYPES(RefreshSettings, DECLARE_SETTING_TRAIT)

struct RefreshSettings
{
    RefreshSettings();
    RefreshSettings(const RefreshSettings & settings);
    RefreshSettings(RefreshSettings && settings) noexcept;
    ~RefreshSettings();

    RefreshSettings & operator=(const RefreshSettings & other);

    REFRESH_SETTINGS_SUPPORTED_TYPES(RefreshSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void applyChanges(const SettingsChanges & changes);

private:
    std::unique_ptr<RefreshSettingsImpl> impl;
};
}
