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

    /// Check that all top-level keys in the config are known server settings or known config sections.
    /// Throws an exception if an unknown key is found (unless skip_check_for_incorrect_settings is set,
    /// either in `config` itself or via `skip_check`).
    ///
    /// `config` is the file-only config (not the layered one), so that command-line options and
    /// Poco-internal layers are not mistaken for unknown top-level config keys. Because the escape
    /// hatch `skip_check_for_incorrect_settings` can also be supplied from the command line (which is
    /// not present in the file-only config), the caller passes its value resolved from the layered
    /// config in `skip_check`, so the escape hatch works from every supported source.
    static void checkUnknownSettings(const Poco::Util::AbstractConfiguration & config, const String & config_path, bool skip_check);

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
