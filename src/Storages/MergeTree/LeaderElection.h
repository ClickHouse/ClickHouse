#pragma once

#include <functional>
#include <memory>
#include <common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>


namespace zkutil
{

/** Initially was used to implement leader election algorithm described here:
  * http://zookeeper.apache.org/doc/r3.4.5/recipes.html#sc_leaderElection
  *
  * But then we decided to get rid of leader election, so every replica can become leader.
  * For now, every replica can become leader if there is no leader among replicas with old version.
  *
  * It's tempting to remove this class at all, but we have to maintain it,
  *  to maintain compatibility when replicas with different versions work on the same cluster
  *  (this is allowed for short time period during cluster update).
  *
  * Replicas with new versions creates ephemeral sequential nodes with values like "replica_name (multiple leaders Ok)".
  * If the first node belongs to a replica with new version, then all replicas with new versions become leaders.
  */
class LeaderElection
{
public:
    using LeadershipHandler = std::function<void()>;

    /** handler is called when this instance become leader.
      *
      * identifier - if not empty, must uniquely (within same path) identify participant of leader election.
      * It means that different participants of leader election have different identifiers
      *  and existence of more than one ephemeral node with same identifier indicates an error.
      */
    LeaderElection(
        DB::BackgroundSchedulePool & pool_,
        const std::string & path_,
        ZooKeeper & zookeeper_,
        LeadershipHandler handler_,
        const std::string & identifier_)
        : pool(pool_), path(path_), zookeeper(zookeeper_), handler(handler_), identifier(identifier_ + suffix)
        , log_name("LeaderElection (" + path + ")")
        , log(&Poco::Logger::get(log_name))
    {
        task = pool.createTask(log_name, [this] { threadFunction(); });
        createNode();
    }

    void shutdown()
    {
        if (shutdown_called)
            return;

        shutdown_called = true;
        task->deactivate();
    }

    ~LeaderElection()
    {
        releaseNode();
    }

private:
    static inline constexpr auto suffix = " (multiple leaders Ok)";
    DB::BackgroundSchedulePool & pool;
    DB::BackgroundSchedulePool::TaskHolder task;
    std::string path;
    ZooKeeper & zookeeper;
    LeadershipHandler handler;
    std::string identifier;
    std::string log_name;
    Poco::Logger * log;

    EphemeralNodeHolderPtr node;
    std::string node_name;

    std::atomic<bool> shutdown_called {false};

    void createNode()
    {
        shutdown_called = false;
        node = EphemeralNodeHolder::createSequential(fs::path(path) / "leader_election-", zookeeper, identifier);

        std::string node_path = node->getPath();
        node_name = node_path.substr(node_path.find_last_of('/') + 1);

        task->activateAndSchedule();
    }

    void releaseNode()
    {
        shutdown();
        node = nullptr;
    }

    void threadFunction()
    {
        bool success = false;

        try
        {
            Strings children = zookeeper.getChildren(path);
            std::sort(children.begin(), children.end());

            auto my_node_it = std::lower_bound(children.begin(), children.end(), node_name);
            if (my_node_it == children.end() || *my_node_it != node_name)
                throw Poco::Exception("Assertion failed in LeaderElection");

            String value = zookeeper.get(path + "/" + children.front());

            if (value.ends_with(suffix))
            {
                handler();
                return;
            }

            if (my_node_it == children.begin())
                throw Poco::Exception("Assertion failed in LeaderElection");

            /// Watch for the node in front of us.
            --my_node_it;
            std::string get_path_value;
            if (!zookeeper.tryGetWatch(path + "/" + *my_node_it, get_path_value, nullptr, task->getWatchCallback()))
                task->schedule();

            success = true;
        }
        catch (const KeeperException & e)
        {
            DB::tryLogCurrentException(log);

            if (e.code == Coordination::Error::ZSESSIONEXPIRED)
                return;
        }
        catch (...)
        {
            DB::tryLogCurrentException(log);
        }

        if (!success)
            task->scheduleAfter(10 * 1000);
    }
};

using LeaderElectionPtr = std::shared_ptr<LeaderElection>;

}
