#pragma once

#include <filesystem>
#include <base/logger_useful.h>
#include <base/sort.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>

namespace fs = std::filesystem;

namespace zkutil
{

/** Initially was used to implement leader election algorithm described here:
  * http://zookeeper.apache.org/doc/r3.4.5/recipes.html#sc_leaderElection
  *
  * But then we decided to get rid of leader election, so every replica can become leader.
  * For now, every replica can become leader if there is no leader among replicas with old version.
  */

void checkNoOldLeaders(Poco::Logger * log, ZooKeeper & zookeeper, const String path)
{
    /// Previous versions (before 21.12) used to create ephemeral sequential node path/leader_election-
    /// Replica with the lexicographically smallest node name becomes leader (before 20.6) or enables multi-leader mode (since 20.6)
    constexpr auto persistent_multiple_leaders = "leader_election-0";   /// Less than any sequential node
    constexpr auto suffix = " (multiple leaders Ok)";
    constexpr auto persistent_identifier = "all (multiple leaders Ok)";

    size_t num_tries = 1000;
    while (num_tries--)
    {
        Strings potential_leaders;
        Coordination::Error code = zookeeper.tryGetChildren(path, potential_leaders);
        /// NOTE zookeeper_path/leader_election node must exist now, but maybe we will remove it in future versions.
        if (code == Coordination::Error::ZNONODE)
            return;
        else if (code != Coordination::Error::ZOK)
            throw KeeperException(code, path);

        Coordination::Requests ops;

        if (potential_leaders.empty())
        {
            /// Ensure that no leaders appeared and enable persistent multi-leader mode
            /// May fail with ZNOTEMPTY
            ops.emplace_back(makeRemoveRequest(path, 0));
            ops.emplace_back(makeCreateRequest(path, "", zkutil::CreateMode::Persistent));
            /// May fail with ZNODEEXISTS
            ops.emplace_back(makeCreateRequest(fs::path(path) / persistent_multiple_leaders, persistent_identifier, zkutil::CreateMode::Persistent));
        }
        else
        {
            ::sort(potential_leaders.begin(), potential_leaders.end());
            if (potential_leaders.front() == persistent_multiple_leaders)
                return;

            /// Ensure that current leader supports multi-leader mode and make it persistent
            auto current_leader = fs::path(path) / potential_leaders.front();
            Coordination::Stat leader_stat;
            String identifier;
            if (!zookeeper.tryGet(current_leader, identifier, &leader_stat))
            {
                LOG_INFO(log, "LeaderElection: leader suddenly changed, will retry");
                continue;
            }

            if (!identifier.ends_with(suffix))
                throw Poco::Exception(fmt::format("Found leader replica ({}) with too old version (< 20.6). Stop it before upgrading", identifier));

            /// Version does not matter, just check that it still exists.
            /// May fail with ZNONODE
            ops.emplace_back(makeCheckRequest(current_leader, leader_stat.version));
            /// May fail with ZNODEEXISTS
            ops.emplace_back(makeCreateRequest(fs::path(path) / persistent_multiple_leaders, persistent_identifier, zkutil::CreateMode::Persistent));
        }

        Coordination::Responses res;
        code = zookeeper.tryMulti(ops, res);
        if (code == Coordination::Error::ZOK)
            return;
        else if (code == Coordination::Error::ZNOTEMPTY || code == Coordination::Error::ZNODEEXISTS || code == Coordination::Error::ZNONODE)
            LOG_INFO(log, "LeaderElection: leader suddenly changed or new node appeared, will retry");
        else
            KeeperMultiException::check(code, ops, res);
    }

    throw Poco::Exception("Cannot check that no old leaders exist");
}

}
