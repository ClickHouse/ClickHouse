#include "ReplicatedMergeTreeGeoReplicationController.h"
#include <optional>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/ZooKeeper/ZooKeeper.h>

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
    if (!region.empty())
    {
        log_name = storage.getStorageID().getFullTableName() + " (StorageReplicatedMergeTree::GeoReplicationController)";
        task = storage.getContext()->getSchedulePool().createTask(log_name, [this]{ threadFunction(); });
        task->deactivate();
    }
}

void ReplicatedMergeTreeGeoReplicationController::onLeader()
{
    auto lease_path = fs::path(storage.getZooKeeperPath()) / "regions" / region / "leader_lease";
    current_zookeeper->createAncestors(lease_path);
    leader_lease_holder = zkutil::EphemeralNodeHolder::create(lease_path, *current_zookeeper, storage.getReplicaName());
}

void ReplicatedMergeTreeGeoReplicationController::resetPreviousTerm()
{
    region_holder.reset();
    leader_lease_holder.reset();
    leader_election.reset();
}

void ReplicatedMergeTreeGeoReplicationController::enterLeaderElection()
{
    auto election_path = fs::path(storage.getZooKeeperPath()) / "regions" / region / "leader_election";

    current_zookeeper->createAncestors(fs::path(election_path) / "leader_election-");

    leader_election = std::make_shared<zkutil::LeaderElection>(
        storage.getContext()->getSchedulePool(),
        election_path,
        *current_zookeeper,
        [this]()
        {
            if (shutdown)
                return;
            leader_lease_holder.reset();
            initialized = true;
        },
        [this]()
        {
            if (shutdown)
                return;
            onLeader();
            initialized = true;
        },
        "",
        storage.getSettings()->geo_replication_control_leader_election_period_ms);
}

void ReplicatedMergeTreeGeoReplicationController::stop()
{
    shutdown = true;
    if (task)
        task->deactivate();
    initialized = false;
}

void ReplicatedMergeTreeGeoReplicationController::start()
{
    if (task)
        task->activateAndSchedule();
    shutdown = false;
}

void ReplicatedMergeTreeGeoReplicationController::threadFunction()
{
    try
    {
        resetPreviousTerm();
        current_zookeeper = storage.getZooKeeper();

        if (!current_zookeeper)
            throw Exception(
                ErrorCodes::NO_ZOOKEEPER,
                "Zookeeper is not initialized, replica {} in region {} hasn't started leader election yet",
                storage.getReplicaName(),
                region);

        createEphemeralRegionNode();
        enterLeaderElection();
    }
    catch (...)
    {
        tryLogCurrentException(log_name.c_str());
        task->scheduleAfter(DBMS_GEO_REPLICATION_CONTROL_INIT_PERIOD_MS);
    }
}

bool ReplicatedMergeTreeGeoReplicationController::isLeader() const
{
    if (!isValid())
        return true;
    return current_zookeeper && !current_zookeeper->expired() && leader_lease_holder;
}

void ReplicatedMergeTreeGeoReplicationController::createEphemeralRegionNode()
{
    auto region_path = fs::path(storage.getZooKeeperPath()) / "replicas" / storage.getReplicaName() / "region";
    if (current_zookeeper->exists(region_path)) /// Old zookeeper is expired and new zookeeper has some delay removing the ephemeral node
        current_zookeeper->remove(region_path);
    region_holder = zkutil::EphemeralNodeHolder::create(region_path, *current_zookeeper, region);
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
        std::function<void()> before_election_,
        std::function<void()> on_leader_,
        const std::string & identifier_,
        int time_wait_ms_)
        : pool(pool_)
        , path(path_)
        , zookeeper(zookeeper_)
        , before_election(std::move(before_election_))
        , on_leader(std::move(on_leader_))
        , identifier(identifier_)
        , time_wait_ms(time_wait_ms_ > 0 ? time_wait_ms_ : 10 * 1000)
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
    DB::BackgroundSchedulePool & pool;
    DB::BackgroundSchedulePool::TaskHolder task;
    std::string path;
    ZooKeeper & zookeeper;
    std::function<void()> before_election;
    std::function<void()> on_leader;
    std::string identifier;
    int time_wait_ms;
    std::string log_name;
    Poco::Logger * log;

    EphemeralNodeHolderPtr node;
    std::string node_name;

    std::atomic<bool> shutdown_called{false};

    void createNode()
    {
        shutdown_called = false;
        node = EphemeralNodeHolder::createSequential(fs::path(path) / "leader_election-", zookeeper);

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
            before_election(); /// Allow to reset current state before starting a new term
            Strings children = zookeeper.getChildren(path);
            std::sort(children.begin(), children.end());

            auto my_node_it = std::lower_bound(children.begin(), children.end(), node_name);
            if (my_node_it == children.end() || *my_node_it != node_name)
                throw Poco::Exception("Assertion failed in LeaderElection");

            String value = zookeeper.get(path + "/" + children.front());

            if (my_node_it == children.begin())
            {
                on_leader();
                LOG_INFO(log, "{} becomes leader", identifier);
                return;
            }

            LOG_INFO(log, "{} becomes follower", identifier);
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
