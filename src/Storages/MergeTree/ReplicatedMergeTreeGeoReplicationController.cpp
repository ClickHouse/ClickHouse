#include <Storages/MergeTree/ReplicatedMergeTreeGeoReplicationController.h>
#include <optional>
#include <Interpreters/Context.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

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
        DB::BackgroundSchedulePoolPtr pool_,
        const std::string & path_,
        ZooKeeper & zookeeper_,
        std::function<void()> before_election_,
        std::function<void()> on_leader_,
        const std::string & identifier_,
        int time_wait_ms_)
        : pool(std::move(pool_))
        , path(path_)
        , zookeeper(zookeeper_)
        , before_election(std::move(before_election_))
        , on_leader(std::move(on_leader_))
        , identifier(identifier_)
        , time_wait_ms(time_wait_ms_ > 0 ? time_wait_ms_ : 10 * 1000)
        , log_name("LeaderElection (" + path + ")")
        , log(&Poco::Logger::get(log_name))
    {
        task = pool->createTask(DB::StorageID::createEmpty(), log_name, [this] { threadFunction(); });
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
    DB::BackgroundSchedulePoolPtr pool;
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
        /// Runs on the background schedule pool and issues ZooKeeper requests (both directly and through the
        /// `before_election` / `on_leader` callbacks), so it must set a component for the mandatory tracking.
        auto component_guard = Coordination::setCurrentComponent("LeaderElection::threadFunction");
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

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsString geo_replication_control_region;
    extern const MergeTreeSettingsUInt64 geo_replication_control_leader_election_period_ms;
}

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
}

ReplicatedMergeTreeGeoReplicationController::ReplicatedMergeTreeGeoReplicationController(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
{
    region = (*storage.getSettings())[MergeTreeSetting::geo_replication_control_region].toString();
    if (!region.empty())
    {
        log_name = storage.getStorageID().getFullTableName() + " (StorageReplicatedMergeTree::GeoReplicationController)";
        task = storage.getContext()->getSchedulePool()->createTask(storage.getStorageID(), log_name, [this]{ threadFunction(); });
        task->deactivate();
    }
}

void ReplicatedMergeTreeGeoReplicationController::onLeader()
{
    /// Runs with `state_mutex` held.
    auto lease_path = fs::path(storage.getZooKeeperPath()) / "regions" / region / "leader_lease";
    current_zookeeper->createAncestors(lease_path);
    leader_lease_holder = zkutil::EphemeralNodeHolder::create(lease_path, *current_zookeeper, storage.getReplicaName());
    /// Publish the leader role to the worker threads only after the lease is actually held.
    is_leader = true;
}

void ReplicatedMergeTreeGeoReplicationController::resetPreviousTerm()
{
    /// Destroying the ephemeral node holders and the leader election object issues `remove` requests to ZooKeeper.
    /// This is called from the schedule pool thread (via `threadFunction`), from the shutdown path (via `stop`) and
    /// from the destructor - none of which establish a ZooKeeper component otherwise - so set one here to satisfy
    /// the mandatory component tracking (`enforce_component_tracking`).
    auto component_guard = Coordination::setCurrentComponent("ReplicatedMergeTreeGeoReplicationController::resetPreviousTerm");
    is_leader = false;

    /// Stop the leader election before touching the holders, and do it without holding `state_mutex`:
    /// `LeaderElection::shutdown` waits for the election task to finish, and that task can be blocked on
    /// `state_mutex` inside the `before_election` / `on_leader` callbacks, so taking the mutex here would
    /// deadlock. Once `shutdown` returns, no callback can run anymore and no one else touches the holders.
    zkutil::LeaderElectionPtr election_to_stop;
    {
        std::lock_guard lock(state_mutex);
        election_to_stop = leader_election;
    }
    if (election_to_stop)
        election_to_stop->shutdown();

    std::lock_guard lock(state_mutex);
    region_holder.reset();
    leader_lease_holder.reset();
    leader_election.reset();
    /// A callback that had already passed the `shutdown` check could have republished the lease while we were
    /// stopping the election, so clear the role flag again after the holders are gone.
    is_leader = false;
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
            /// The callbacks run on the election task and mutate the controller state, so they take the same
            /// mutex as the controller task and the shutdown path.
            std::lock_guard lock(state_mutex);
            if (shutdown)
                return;
            /// A new election round starts: drop any previous leader role before we know the new outcome, so
            /// worker threads do not keep seeing this replica as a leader across a handoff.
            is_leader = false;
            leader_lease_holder.reset();
        },
        [this]()
        {
            std::lock_guard lock(state_mutex);
            if (shutdown)
                return;
            onLeader();
        },
        "",
        (*storage.getSettings())[MergeTreeSetting::geo_replication_control_leader_election_period_ms]);
}

void ReplicatedMergeTreeGeoReplicationController::stop()
{
    shutdown = true;
    if (task)
        task->deactivate();
    /// The controller task is deactivated above and `resetPreviousTerm` stops the leader election, so after this
    /// no thread touches the holders. Release them explicitly, otherwise this replica keeps publishing its region
    /// membership and holding the leader lease while its replication queues are stopped, preventing another
    /// replica in the region from becoming leader.
    resetPreviousTerm();
}

bool ReplicatedMergeTreeGeoReplicationController::start()
{
    if (!task)
        return true;

    /// Clear `shutdown` before running: otherwise the controller pass can observe a stale `shutdown == true`,
    /// returning early without ever creating the region node or entering leader election.
    shutdown = false;
    task->activate();

    /// Run the first controller pass synchronously, before the caller activates the replication queue. Peers
    /// classify same-region fetch sources from `/replicas/<name>/region`; if that node were published only
    /// asynchronously after the queue starts, a replica recovering from a backlog could be misclassified as
    /// out-of-region, exhaust `geo_replication_control_leader_wait_timeout`, and fall back to a cross-region
    /// fetch purely because region publication lagged behind queue startup. Running the pass here publishes the
    /// region node before the queue workers begin. Subsequent passes (leader re-election, retry after a transient
    /// ZooKeeper error) run on the schedule pool via `task`.
    ///
    /// The result is returned to the caller: if this first pass failed (a transient ZooKeeper error, for example),
    /// the region node is not published yet, and starting the queue now would let a recovering replica fetch
    /// cross-region until the retry succeeds. The caller keeps the table readonly and retries the whole startup.
    return threadFunction();
}

bool ReplicatedMergeTreeGeoReplicationController::threadFunction()
{
    /// This runs on the background schedule pool and touches ZooKeeper (region node, leader election), so it must
    /// set a component for the mandatory ZooKeeper component tracking.
    auto component_guard = Coordination::setCurrentComponent("ReplicatedMergeTreeGeoReplicationController::threadFunction");
    try
    {
        resetPreviousTerm();

        std::lock_guard lock(state_mutex);
        if (shutdown)
            return false;

        current_zookeeper = storage.getZooKeeper();

        if (!current_zookeeper)
            throw Exception(
                ErrorCodes::NO_ZOOKEEPER,
                "Zookeeper is not initialized, replica {} in region {} hasn't started leader election yet",
                storage.getReplicaName(),
                region);

        createEphemeralRegionNode();
        enterLeaderElection();
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(log_name.c_str());
        task->scheduleAfter(DBMS_GEO_REPLICATION_CONTROL_INIT_PERIOD_MS);
        return false;
    }
}

bool ReplicatedMergeTreeGeoReplicationController::isLeader() const
{
    /// Feature disabled for this table (`region` is set once in the constructor and never mutated afterwards, so
    /// reading it here is race-free): every replica may fetch from any region.
    if (region.empty())
        return true;

    /// Read only the atomic role flag here. This is called from the queue worker threads, while the
    /// `current_zookeeper` / `leader_lease_holder` shared_ptr members are mutated on the controller and
    /// leader-election threads; reading those pointers here would be a data race. `is_leader` stays false until
    /// this replica has actually won an election, so a replica whose controller has not entered a term yet is
    /// treated as a follower rather than assuming leadership.
    return is_leader;
}

void ReplicatedMergeTreeGeoReplicationController::createEphemeralRegionNode()
{
    auto region_path = fs::path(storage.getZooKeeperPath()) / "replicas" / storage.getReplicaName() / "region";
    if (current_zookeeper->exists(region_path)) /// Old zookeeper is expired and new zookeeper has some delay removing the ephemeral node
        current_zookeeper->remove(region_path);
    region_holder = zkutil::EphemeralNodeHolder::create(region_path, *current_zookeeper, region);
}

}
