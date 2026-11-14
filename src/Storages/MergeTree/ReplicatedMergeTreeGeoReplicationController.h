#pragma once

#include <mutex>
#include <optional>
#include <base/defines.h>
#include <Interpreters/Context_fwd.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>

namespace zkutil
{
    class LeaderElection;
    using LeaderElectionPtr = std::shared_ptr<LeaderElection>;
}

namespace DB
{

class StorageReplicatedMergeTree;

/**
 * Problem: Consider a clickhouse system with 4 replica across 2 regions, 2 in APAC and 2 in US. If any insertion
 * happens in a APAC replica, both US replicas will pull the log from log queue, then try to fetch data from
 * the APAC replica, which double the cross-ocean network bandwidth.
 *
 * ReplicatedMergeTreeGeoReplicationController enable the following replication architecture:
 *
 * US1 (regional leader) <-----------------------> APAC1 (regional leader)
 *  |                                               |
 * US2 (follower)                                  APAC2 (follower)
 *
 * In each region, there is one regional leader (leader) and multiple followers.
 * During replication, all replicas still pull log entries from zk log queue to its queue, no matter
 * which replica publish the entry. When replaying the log entry, if fetching needed, following constraints
 * apply:
 * 1. A leader can fetch part from any replica regardless the region
 * 2. A follower can only fetch part from replicas within the region
 *
 * The location information is configurable. The leader is per-table. After being elected,
 * the leader will maintain a `lease` in zk, so other node know that there is a leader. All replicas within
 * a region will run leader election if leader is absented.
 *
 * Additional zk nodes per replicated table:
 * - /{table_zk_path}/regions/{$REGION}
 * - /{table_zk_path}/regions/{$REGION}/leader_election: leader election node for the region REGION
 * - /{table_zk_path}/regions/{$REGION}/leader_lease: ephemeral node, current leader of the region
 * - /{table_zk_path}/replicas/{replica_name}/region: ephemeral node with the region of the replica, created together with
 *   `is_active` (see `addPublishRegionRequests`)
**/
class ReplicatedMergeTreeGeoReplicationController
{
public:
    explicit ReplicatedMergeTreeGeoReplicationController(StorageReplicatedMergeTree & storage_);
    ~ReplicatedMergeTreeGeoReplicationController() { resetPreviousTerm(); }


    /// Whether geo replication control is configured for this table. Region locality is enforced whenever the
    /// feature is configured - it must not depend on whether this replica's controller has finished its first
    /// election, otherwise a recovering replica would fetch cross-region before entering a term (see `isLeader`,
    /// which reports leadership separately and stays false until a term is actually won).
    bool isConfigured() const { return !region.empty(); }

    const String & getRegion() const { return region; }

    /// Adds to `ops` the requests that publish this replica's region membership: replace the `region` node with an
    /// ephemeral node of `zookeeper`'s session. Does nothing if the feature is not configured. The requests are part
    /// of the `multi` that creates `/replicas/<name>/is_active` in `activateReplica`, so the `region` node is
    /// published atomically with the claim of the replica name: a startup that does not own the replica name fails
    /// on creating `is_active` and never touches the region metadata of a live replica, and the `region` node of an
    /// active replica always belongs to the session that holds its `is_active`. Publishing it together with
    /// `is_active` also guarantees that peers never see this replica as active without its region.
    /// Returns the path of the `region` node, or an empty string if nothing was added.
    String addPublishRegionRequests(Coordination::Requests & ops, zkutil::ZooKeeper & zookeeper) const;

    /// Enters leader election. Must be called after the replica is activated (see `addPublishRegionRequests`).
    /// A failed attempt is retried on the schedule pool; until this replica wins an election, it acts as a follower.
    void start();

    void stop();

    bool isLeader() const;

private:
    static const int DBMS_GEO_REPLICATION_CONTROL_INIT_PERIOD_MS = 300;
    StorageReplicatedMergeTree & storage;
    String log_name;
    BackgroundSchedulePool::TaskHolder task;
    String region;

    /// Guards all the controller state below. It is mutated from the controller task (`threadFunction`), from the
    /// leader-election task (the `before_election` / `on_leader` callbacks) and from the shutdown path (`stop`,
    /// destructor), so every access has to be serialized.
    std::mutex state_mutex;
    zkutil::ZooKeeperPtr current_zookeeper TSA_GUARDED_BY(state_mutex);
    zkutil::LeaderElectionPtr leader_election TSA_GUARDED_BY(state_mutex);
    zkutil::EphemeralNodeHolderPtr leader_lease_holder TSA_GUARDED_BY(state_mutex);
    std::atomic_bool shutdown = false;
    /// Published from the controller / leader-election thread and read from the queue worker threads (`isLeader`).
    /// Exposing an atomic role flag - instead of letting worker threads read the `current_zookeeper` and
    /// `leader_lease_holder` shared_ptr members that those threads mutate - avoids a data race on the pointer
    /// objects during a leader handoff or restart.
    std::atomic_bool is_leader = false;

    void threadFunction();

    void resetPreviousTerm();
    /// These run with `state_mutex` held.
    void enterLeaderElection() TSA_REQUIRES(state_mutex);
    void onLeader() TSA_REQUIRES(state_mutex);
};

}
