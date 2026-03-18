#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{
class Field;
struct ServerSettingColumnsParams;
struct ServerSettingsImpl;

/// List of available types supported in ServerSettings object
#define SERVER_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Double) \
    M(CLASS_NAME, GroupArrayActionWhenLimitReached) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, Int32) \
    M(CLASS_NAME, Seconds) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt32) \
    M(CLASS_NAME, UInt64)

SERVER_SETTINGS_SUPPORTED_TYPES(ServerSettings, DECLARE_SETTING_TRAIT)

struct ServerSettings
{
    enum class ChangeableWithoutRestart : uint8_t
    {
        No,
        IncreaseOnly,
        DecreaseOnly,
        Yes
    };

    ServerSettings();
    ServerSettings(const ServerSettings & settings);
    ~ServerSettings();

    void set(std::string_view name, const Field & value);

    void loadSettingsFromConfig(const Poco::Util::AbstractConfiguration & config);

    SERVER_SETTINGS_SUPPORTED_TYPES(ServerSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void dumpToSystemServerSettingsColumns(ServerSettingColumnsParams & params) const;

private:
    std::unique_ptr<ServerSettingsImpl> impl;
};
}
