#pragma once

#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Coordination/KeeperConstants.h>

namespace DB
{

enum KeeperFeatureFlag
{
    FILTERED_LIST = 0,
    MULTI_READ,
    CHECK_NOT_EXISTS,
};

static inline constexpr std::array all_keeper_feature_flags
{
    KeeperFeatureFlag::FILTERED_LIST,
    KeeperFeatureFlag::MULTI_READ,
    KeeperFeatureFlag::CHECK_NOT_EXISTS,
};

DECLARE_SETTING_ENUM(KeeperFeatureFlag);

class KeeperFeatureFlags
{
public:
    KeeperFeatureFlags();

    explicit KeeperFeatureFlags(std::string feature_flags_);

    /// backwards compatibility
    void fromApiVersion(KeeperApiVersion keeper_api_version);

    bool isEnabled(KeeperFeatureFlag feature) const;

    void setFeatureFlags(std::string feature_flags_);
    const std::string & getFeatureFlags() const;

    void enableFeatureFlag(KeeperFeatureFlag feature);
    void disableFeatureFlag(KeeperFeatureFlag feature);

    void logFlags(Poco::Logger * log) const;
private:
    std::string feature_flags;
};

}
