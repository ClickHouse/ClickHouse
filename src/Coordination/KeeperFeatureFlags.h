#pragma once

#include <Coordination/KeeperConstants.h>

namespace DB
{

/// these values cannot be reordered or removed, only new values can be added
enum class KeeperFeatureFlag : size_t
{
    FILTERED_LIST = 0,
    MULTI_READ,
    CHECK_NOT_EXISTS,
    CREATE_IF_NOT_EXISTS,
    REMOVE_RECURSIVE,
};

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

    void logFlags(LoggerPtr log) const;
private:
    std::string feature_flags;
};

}
