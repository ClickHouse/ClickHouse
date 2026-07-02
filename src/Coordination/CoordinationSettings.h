#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <IO/WriteBufferFromString.h>

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
}

namespace DB
{
struct CoordinationSettingsImpl;

/// List of available types supported in CoordinationSettings object
#define COORDINATION_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, LogsLevel) \
    M(CLASS_NAME, Milliseconds) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, NonZeroUInt64)

COORDINATION_SETTINGS_SUPPORTED_TYPES(CoordinationSettings, DECLARE_SETTING_TRAIT)

struct CoordinationSettings
{
    CoordinationSettings();
    CoordinationSettings(const CoordinationSettings & settings);
    ~CoordinationSettings();

    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
    void dump(WriteBufferFromOwnString & buf) const;
    void updateHotReloadableSettings(const CoordinationSettings & new_settings);

    COORDINATION_SETTINGS_SUPPORTED_TYPES(CoordinationSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    uint64_t version = 0;

private:
    friend struct KeeperConfigurationAndSettings;

    std::unique_ptr<CoordinationSettingsImpl> impl;
};

using CoordinationSettingsPtr = std::shared_ptr<CoordinationSettings>;

/// Non-"settings" parts of keeper server configuration (server identity, ports, etc.).
/// CoordinationSettings live separately in KeeperContext.
/// (What's the difference between "settings" and "configuration"? In this case, there's not much.
///  Fields of KeeperConfiguration are taken from keeper_server.* nodes of the config, while
///  elements of CoordinationSettings are taken from keeper_server.coordination_settings.* .
///  And CoordinationSettings are usually equal across all servers, while KeeperConfiguration are different.)
/// Not to be confused with KeeperServerConfig (which contains the list of all servers in the cluster)
/// and KeeperConfigurationWrapper.
struct KeeperConfiguration
{
    static constexpr int NOT_EXIST = -1;
    static const String DEFAULT_FOUR_LETTER_WORD_CMD;

    KeeperConfiguration();
    int server_id;

    bool enable_ipv6;
    int tcp_port;
    int tcp_port_secure;

    String four_letter_word_allow_list;

    String super_digest;

    bool standalone_keeper;

    void dump(WriteBufferFromOwnString & buf) const;
    static std::shared_ptr<KeeperConfiguration> loadFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_);
};

using KeeperConfigurationPtr = std::shared_ptr<KeeperConfiguration>;

}
