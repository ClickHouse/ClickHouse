#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
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
struct CoordinationSettingsImpl;
class WriteBufferFromOwnString;

/// List of available types supported in CoordinationSettings object
#define COORDINATION_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, LogsLevel) \
    M(CLASS_NAME, Milliseconds) \
    M(CLASS_NAME, UInt64)

COORDINATION_SETTINGS_SUPPORTED_TYPES(CoordinationSettings, DECLARE_SETTING_TRAIT)

struct CoordinationSettings
{
    CoordinationSettings();
    CoordinationSettings(const CoordinationSettings & settings);
    ~CoordinationSettings();

    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    COORDINATION_SETTINGS_SUPPORTED_TYPES(CoordinationSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

private:
    std::unique_ptr<CoordinationSettingsImpl> impl;
};

using CoordinationSettingsPtr = std::shared_ptr<CoordinationSettings>;

/// Coordination settings + some other parts of keeper configuration
/// which are not stored in settings. Allows to dump configuration
/// with 4lw commands.
struct KeeperConfigurationAndSettings
{
    static constexpr int NOT_EXIST = -1;
    static const String DEFAULT_FOUR_LETTER_WORD_CMD;

    KeeperConfigurationAndSettings();
    int server_id;

    bool enable_ipv6;
    int tcp_port;
    int tcp_port_secure;

    String four_letter_word_allow_list;

    String super_digest;

    bool standalone_keeper;
    CoordinationSettings coordination_settings;

    void dump(WriteBufferFromOwnString & buf) const;
    static std::shared_ptr<KeeperConfigurationAndSettings> loadFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_);
};

using KeeperConfigurationAndSettingsPtr = std::shared_ptr<KeeperConfigurationAndSettings>;

}
