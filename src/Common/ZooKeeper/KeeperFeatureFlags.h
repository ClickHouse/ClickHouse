#pragma once

#include <Coordination/KeeperConstants.h>
#include <Core/LogsLevel.h>

#include <memory>

namespace Poco
{
class Logger;
}

using LoggerPtr = std::shared_ptr<Poco::Logger>;

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
    MULTI_WATCHES,
    CHECK_STAT,
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

    void logFlags(LoggerPtr log, DB::LogsLevel log_level = DB::LogsLevel::information) const;
private:
    std::string feature_flags;
};

}
