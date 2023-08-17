#include "ReplicatedMergeTreeGeoReplicationController.h"
#include <optional>
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
}

ReplicatedMergeTreeGeoReplicationController::ReplicatedMergeTreeGeoReplicationController(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
{
    region = storage.getSettings()->geo_replication_control_region;
}

void ReplicatedMergeTreeGeoReplicationController::onLeader()
{
    auto lease_path = fs::path(storage.getZooKeeperPath()) / "regions" / region / "leader_lease";
    current_zookeeper->createAncestors(lease_path);
    leader_lease_holder = zkutil::EphemeralNodeHolder::create(lease_path, *current_zookeeper, storage.getReplicaName());
    LOG_INFO(storage.log.load(), "Replica {} becomes leader for region {}", storage.getReplicaName(), region);
}

void ReplicatedMergeTreeGeoReplicationController::resetCurrentTerm()
{
    try
    {
        leader_lease_holder.reset();
    }
    catch (...)
    {
        /// Zookeeper session may have been expired
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    try
    {
        leader_election.reset();
    }
    catch (...)
    {
        /// Zookeeper session may have been expired
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    current_zookeeper.reset();
}

void ReplicatedMergeTreeGeoReplicationController::enterLeaderElection()
{
    try
    {
        current_zookeeper = storage.getZooKeeper();
        auto election_path = fs::path(storage.getZooKeeperPath()) / "regions" / region / "leader_election";

        current_zookeeper->createAncestors(fs::path(election_path) / "leader_election-");

        leader_election = std::make_shared<zkutil::LeaderElection>(
            storage.getContext()->getSchedulePool(),
            election_path,
            *current_zookeeper,
            [this]() { return onLeader(); },
            "",
            storage.getSettings()->geo_replication_control_leader_election_period_ms,
            false);
    }
    catch (...)
    {
        /// Zookeeper maybe not ready now
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ReplicatedMergeTreeGeoReplicationController::startLeaderElection()
{
    resetCurrentTerm();
    enterLeaderElection();
}

std::optional<String> ReplicatedMergeTreeGeoReplicationController::getCurrentLeader() const
{
    if (!isValid())
        return std::nullopt;

    if (!current_zookeeper)
        throw Exception(
            ErrorCodes::NO_ZOOKEEPER,
            "Zookeeper is not initialized, replica {} in region {} hasn't started leader election yet",
            storage.getReplicaName(),
            region);

    String leader_replica;
    current_zookeeper->tryGet(fs::path(storage.getZooKeeperPath()) / "regions" / region / "leader_lease", leader_replica);
    return leader_replica;
}

bool ReplicatedMergeTreeGeoReplicationController::isLeader() const
{
    if (!isValid())
        return true;
    return current_zookeeper && !current_zookeeper->expired() && leader_lease_holder;
}

}

namespace zkutil
{
/**
 * Implement leader election algorithm described here:
 * http://zookeeper.apache.org/doc/r3.4.5/recipes.html#sc_leaderElection
 *
 * Now only being used to elect regional leader, see Coordinator/ReplicatedMergeTreeGeoReplicationController.h
**/
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
        const std::string & identifier_,
        int time_wait_ms_,
        bool allow_multiple_leaders_)
        : pool(pool_)
        , path(path_)
        , zookeeper(zookeeper_)
        , handler(std::move(handler_))
        , identifier(allow_multiple_leaders_ ? (identifier_ + suffix) : identifier_)
        , time_wait_ms(time_wait_ms_ > 0 ? time_wait_ms_ : 10 * 1000)
        , allow_multiple_leaders(allow_multiple_leaders_)
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

    ~LeaderElection() { releaseNode(); }

private:
    static inline constexpr auto suffix = " (multiple leaders Ok)";
    DB::BackgroundSchedulePool & pool;
    DB::BackgroundSchedulePool::TaskHolder task;
    std::string path;
    ZooKeeper & zookeeper;
    LeadershipHandler handler;
    std::string identifier;
    int time_wait_ms;
    bool allow_multiple_leaders;
    std::string log_name;
    Poco::Logger * log;

    EphemeralNodeHolderPtr node;
    std::string node_name;

    std::atomic<bool> shutdown_called{false};

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
            LOG_INFO(log, "Running leader election");
            Strings children = zookeeper.getChildren(path);
            std::sort(children.begin(), children.end());

            auto my_node_it = std::lower_bound(children.begin(), children.end(), node_name);
            if (my_node_it == children.end() || *my_node_it != node_name)
                throw Poco::Exception("Assertion failed in LeaderElection");

            String value = zookeeper.get(path + "/" + children.front());

            if (allow_multiple_leaders)
            {
                if (value.ends_with(suffix))
                {
                    handler();
                    return;
                }

                if (my_node_it == children.begin())
                    throw Poco::Exception("Assertion failed in LeaderElection");
            }
            else
            {
                if (my_node_it == children.begin())
                {
                    handler();
                    LOG_INFO(log, "Become leader");
                    return;
                }
            }

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
            task->scheduleAfter(time_wait_ms);
    }
};

}
