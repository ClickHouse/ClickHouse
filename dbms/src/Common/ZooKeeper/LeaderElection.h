#pragma once

#include "ZooKeeper.h"
#include "KeeperException.h"
#include <functional>
#include <memory>
#include <common/logger_useful.h>
#include <Common/CurrentMetrics.h>


namespace ProfileEvents
{
    extern const Event ObsoleteEphemeralNode;
    extern const Event LeaderElectionAcquiredLeadership;
}

namespace CurrentMetrics
{
    extern const Metric LeaderElection;
}


namespace zkutil
{

/** Implements leader election algorithm described here: http://zookeeper.apache.org/doc/r3.4.5/recipes.html#sc_leaderElection
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
    LeaderElection(const std::string & path_, ZooKeeper & zookeeper_, LeadershipHandler handler_, const std::string & identifier_ = "")
        : path(path_), zookeeper(zookeeper_), handler(handler_), identifier(identifier_)
    {
        createNode();
    }

    void shutdown()
    {
        if (shutdown_called)
            return;

        shutdown_called = true;
        event->set();
        if (thread.joinable())
            thread.join();
    }

    ~LeaderElection()
    {
        releaseNode();
    }

private:
    std::string path;
    ZooKeeper & zookeeper;
    LeadershipHandler handler;
    std::string identifier;

    EphemeralNodeHolderPtr node;
    std::string node_name;

    std::thread thread;
    std::atomic<bool> shutdown_called {false};
    EventPtr event = std::make_shared<Poco::Event>();

    CurrentMetrics::Increment metric_increment{CurrentMetrics::LeaderElection};

    void createNode()
    {
        shutdown_called = false;
        node = EphemeralNodeHolder::createSequential(path + "/leader_election-", zookeeper, identifier);

        std::string node_path = node->getPath();
        node_name = node_path.substr(node_path.find_last_of('/') + 1);

        thread = std::thread(&LeaderElection::threadFunction, this);
    }

    void releaseNode()
    {
        shutdown();
        node = nullptr;
    }

    void threadFunction()
    {
        while (!shutdown_called)
        {
            bool success = false;

            try
            {
                Strings children = zookeeper.getChildren(path);
                std::sort(children.begin(), children.end());
                auto it = std::lower_bound(children.begin(), children.end(), node_name);
                if (it == children.end() || *it != node_name)
                    throw Poco::Exception("Assertion failed in LeaderElection");

                if (it == children.begin())
                {
                    ProfileEvents::increment(ProfileEvents::LeaderElectionAcquiredLeadership);
                    handler();
                    return;
                }

                if (zookeeper.exists(path + "/" + *(it - 1), nullptr, event))
                    event->wait();

                success = true;
            }
            catch (const KeeperException & e)
            {
                DB::tryLogCurrentException("LeaderElection");

                if (e.code == ZooKeeperImpl::ZooKeeper::ZSESSIONEXPIRED)
                    break;
            }
            catch (...)
            {
                DB::tryLogCurrentException("LeaderElection");
            }

            if (!success)
                event->tryWait(10 * 1000);
        }
    }
};

using LeaderElectionPtr = std::shared_ptr<LeaderElection>;

}
