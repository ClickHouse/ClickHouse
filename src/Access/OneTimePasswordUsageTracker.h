#pragma once

#include <map>
#include <mutex>
#include <string_view>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Logger.h>
#include <Common/ZooKeeper/Common.h>

namespace DB
{

struct OneTimePasswordSecret;

/// Enforces single use of one-time passwords (RFC 6238, Section 5.2): a code is accepted at most once,
/// and codes for time steps at or before the last accepted one are rejected.
/// By default the accepted codes are tracked per server process. In strict mode they are tracked in Keeper,
/// so a code accepted by one server is rejected by every server sharing the same Keeper, and after a restart.
class OneTimePasswordUsageTracker
{
public:
    OneTimePasswordUsageTracker();

    /// Strict mode: the accepted codes are stored under `keeper_path_`, one node per user, named by a hash of the user name.
    OneTimePasswordUsageTracker(zkutil::GetZooKeeper get_zookeeper_, String keeper_path_);

    /// Returns false if the code is not valid now or has already been used. Call it once per successful
    /// authentication, after all other credentials are verified. In strict mode, throws if Keeper is unavailable.
    bool tryConsume(const String & user_name, std::string_view code, const OneTimePasswordSecret & secret);

private:
    /// The accepted time step is kept as the timestamp of its start rather than as its number,
    /// so that the entries stay comparable if `period` of the user is changed.
    struct Entry
    {
        UInt64 time_step_start;
        UInt64 expires_at;
    };

    bool tryConsumeInKeeper(const String & user_name, UInt64 time_step_start);

    const zkutil::GetZooKeeper get_zookeeper;
    const String keeper_path;
    const LoggerPtr log;

    std::mutex mutex;
    std::map<String, Entry> last_accepted TSA_GUARDED_BY(mutex);
};

}
