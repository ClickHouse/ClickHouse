#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Interpreters/Context_fwd.h>

#include <optional>

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
    M(CLASS_NAME, NonZeroUInt64) \
    M(CLASS_NAME, Int32) \
    M(CLASS_NAME, Seconds) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt32) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, UInt64Auto) \
    M(CLASS_NAME, InsertDeduplicationVersions) \


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

    Field get(std::string_view name) const;

    void set(std::string_view name, const Field & value);

    void loadSettingsFromConfig(const Poco::Util::AbstractConfiguration & config);

    SERVER_SETTINGS_SUPPORTED_TYPES(ServerSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void dumpToSystemServerSettingsColumns(ServerSettingColumnsParams & params) const;

    /// Some server settings can be changed without a restart (e.g. memory and cache limits, thread pool sizes).
    /// When this happens, the live value held by the component diverges from the value stored in `*this`,
    /// which still reflects only what was last loaded from the config.
    /// This function returns the live value (as a string) for such settings, or `std::nullopt` when the setting
    /// has no live-value override and the configured value in `*this` is authoritative.
    std::optional<String> tryGetLiveValueAsString(ContextPtr context, std::string_view name) const;

private:
    std::unique_ptr<ServerSettingsImpl> impl;
};
}
