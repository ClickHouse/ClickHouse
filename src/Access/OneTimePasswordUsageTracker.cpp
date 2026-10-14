#include <Access/OneTimePasswordUsageTracker.h>

#include <ctime>
#include <Access/AuthenticationData.h>
#include <Access/Common/OneTimePassword.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <boost/algorithm/hex.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

OneTimePasswordUsageTracker::OneTimePasswordUsageTracker()
    : log(getLogger("OneTimePasswordUsageTracker"))
{
}

OneTimePasswordUsageTracker::OneTimePasswordUsageTracker(zkutil::GetZooKeeper get_zookeeper_, String keeper_path_)
    : get_zookeeper(std::move(get_zookeeper_))
    , keeper_path(std::move(keeper_path_))
    , log(getLogger("OneTimePasswordUsageTracker"))
{
    if (keeper_path.empty() || keeper_path.front() != '/')
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `totp_strict_mode_keeper_path` must be an absolute path, got '{}'", keeper_path);
    if (keeper_path.back() == '/')
        keeper_path.pop_back();
}

bool OneTimePasswordUsageTracker::tryConsume(const String & user_name, std::string_view code, const OneTimePasswordSecret & secret)
{
    const auto current_time = static_cast<UInt64>(std::time(nullptr));
    const auto time_step = findOneTimePasswordTimeStep(code, secret, current_time);
    if (!time_step.has_value())
        return false;

    const auto period = static_cast<UInt64>(secret.params.period);
    const UInt64 time_step_start = static_cast<UInt64>(*time_step) * period;
    /// After this moment no code at or before this time step passes the time tolerance check.
    const UInt64 expires_at = time_step_start + 2 * period;

    {
        std::lock_guard lock(mutex);
        std::erase_if(last_accepted, [current_time](const auto & entry) { return entry.second.expires_at <= current_time; });

        auto it = last_accepted.find(user_name);
        if (it != last_accepted.end() && time_step_start <= it->second.time_step_start)
            return false;

        if (!get_zookeeper)
        {
            last_accepted.insert_or_assign(user_name, Entry{time_step_start, expires_at});
            return true;
        }
    }

    /// The local entries are only a cache of the accepts done by this server: they are sound as a pre-filter
    /// because the value in Keeper never decreases, while Keeper stays the authority.
    if (!tryConsumeInKeeper(user_name, time_step_start))
        return false;

    std::lock_guard lock(mutex);
    auto & entry = last_accepted[user_name];
    if (entry.time_step_start < time_step_start)
        entry = Entry{time_step_start, expires_at};
    return true;
}

bool OneTimePasswordUsageTracker::tryConsumeInKeeper(const String & user_name, UInt64 time_step_start)
{
    auto component_guard = Coordination::setCurrentComponent("OneTimePasswordUsageTracker::tryConsumeInKeeper");

    /// The node is named by a hash of the user name, so that Keeper (and its snapshots and logs) hold no user names.
    const auto user_name_digest = AuthenticationData::Util::encodeSHA256(user_name);
    String node_name;
    node_name.resize(user_name_digest.size() * 2);
    boost::algorithm::hex(user_name_digest.begin(), user_name_digest.end(), node_name.data());
    const String node_path = keeper_path + "/" + node_name;
    const String new_data = toString(time_step_start);

    /// Bounded, so that an authentication attempt fails instead of hanging while Keeper is unavailable.
    constexpr UInt64 max_retries = 3;
    constexpr UInt64 initial_backoff_ms = 100;
    constexpr UInt64 max_backoff_ms = 1000;
    ZooKeeperRetriesControl retries{
        "OneTimePasswordUsageTracker", log, ZooKeeperRetriesInfo{max_retries, initial_backoff_ms, max_backoff_ms, /* query_status */ nullptr}};

    bool accepted = false;
    retries.retryLoop([&]
    {
        auto zookeeper = get_zookeeper();

        /// Compare-and-set on the stored time step: every iteration either advances it or observes
        /// that another server advanced it concurrently, so the loop terminates.
        while (true)
        {
            String stored_data;
            Coordination::Stat stat;
            if (!zookeeper->tryGet(node_path, stored_data, &stat))
            {
                auto code = zookeeper->tryCreate(node_path, new_data, zkutil::CreateMode::Persistent);
                if (code == Coordination::Error::ZOK)
                {
                    accepted = true;
                    return;
                }
                if (code == Coordination::Error::ZNONODE)
                {
                    zookeeper->createAncestors(node_path);
                    continue;
                }
                if (code == Coordination::Error::ZNODEEXISTS)
                    continue;
                throw zkutil::KeeperException::fromPath(code, node_path);
            }

            if (time_step_start <= parse<UInt64>(stored_data))
            {
                accepted = false;
                return;
            }

            auto code = zookeeper->trySet(node_path, new_data, stat.version);
            if (code == Coordination::Error::ZOK)
            {
                accepted = true;
                return;
            }
            if (code == Coordination::Error::ZBADVERSION || code == Coordination::Error::ZNONODE)
                continue;
            throw zkutil::KeeperException::fromPath(code, node_path);
        }
    });
    return accepted;
}

}
