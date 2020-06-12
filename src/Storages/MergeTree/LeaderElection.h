#pragma once

#include <functional>
#include <memory>
#include <common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>


namespace ProfileEvents
{
    extern const Event LeaderElectionAcquiredLeadership;
}

namespace CurrentMetrics
{
    extern const Metric LeaderElection;
}


namespace zkutil
{

/** Initially was used to implement leader election algorithm described here:
  * http://zookeeper.apache.org/doc/r3.4.5/recipes.html#sc_leaderElection
  *
  * But then we decided to get rid of leader election, so every replica can become leader.
  * For now, every replica can become leader if there is no leader among replicas with old version.
  *
  * Replicas with old versions participate in leader election with ephemeral sequential nodes.
  *  If the node is first, then replica is leader.
  * Replicas with new versions creates persistent sequential nodes.
  *  If the first node is persistent, then all replicas with new versions become leaders.
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
        : pool(pool_), path(path_), zookeeper(zookeeper_), handler(handler_), identifier(identifier_)
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
        shutdown();
    }

private:
    DB::BackgroundSchedulePool & pool;
    DB::BackgroundSchedulePool::TaskHolder task;
    const std::string path;
    ZooKeeper & zookeeper;
    LeadershipHandler handler;
    std::string identifier;
    std::string log_name;
    Poco::Logger * log;

    std::atomic<bool> shutdown_called {false};

    CurrentMetrics::Increment metric_increment{CurrentMetrics::LeaderElection};

    void createNode()
    {
        shutdown_called = false;
        zookeeper.create(path + "/leader_election-", identifier, CreateMode::PersistentSequential);
        task->activateAndSchedule();
    }

    void threadFunction()
    {
        try
        {
            Strings children = zookeeper.getChildren(path);
            if (children.empty())
                throw Poco::Exception("Assertion failed in LeaderElection");

            std::sort(children.begin(), children.end());

            Coordination::Stat stat;
            zookeeper.get(path + "/" + children.front(), &stat);

            if (!stat.ephemeralOwner)
            {
                /// It is sequential node - we can become leader.
                ProfileEvents::increment(ProfileEvents::LeaderElectionAcquiredLeadership);
                handler();
                return;
            }
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

        task->scheduleAfter(10 * 1000);
    }
};

using LeaderElectionPtr = std::shared_ptr<LeaderElection>;

}
