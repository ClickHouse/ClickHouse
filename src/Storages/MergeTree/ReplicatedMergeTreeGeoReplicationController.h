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
 * - /{table_zk_path}/replicas/{replica_name}/{$REGION}: ephemeral node, zk information about the region of current replica
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

    /// Publishes this replica's region membership and enters leader election. Returns whether the region node is
    /// published: the caller must not start the replication queue until it is, otherwise peers classify this
    /// replica as out-of-region and a recovering replica falls back to a cross-region fetch.
    /// `active_node_identifier_` is the content this server writes to `/replicas/<name>/is_active`: an existing
    /// `region` node is replaced only when `is_active` is absent or owned by this server, so a startup that does
    /// not own the replica name never touches the region metadata of a live replica.
    bool start(const String & active_node_identifier_);

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
    /// The session that created this replica's `region` node, or null if this controller has not published it.
    /// Not an `EphemeralNodeHolder`: its destructor removes the path unconditionally, which would delete a node
    /// that another server holding this replica name has put in place of ours (see `removeRegionNodeIfOwned`).
    zkutil::ZooKeeperPtr region_node_zookeeper TSA_GUARDED_BY(state_mutex);
    String active_node_identifier TSA_GUARDED_BY(state_mutex);
    std::atomic_bool shutdown = false;
    /// Published from the controller / leader-election thread and read from the queue worker threads (`isLeader`).
    /// Exposing an atomic role flag - instead of letting worker threads read the `current_zookeeper` and
    /// `leader_lease_holder` shared_ptr members that those threads mutate - avoids a data race on the pointer
    /// objects during a leader handoff or restart.
    std::atomic_bool is_leader = false;

    /// Returns whether the region node has been published and leader election has been entered.
    bool threadFunction();

    void resetPreviousTerm();
    /// These three run with `state_mutex` held.
    void createEphemeralRegionNode() TSA_REQUIRES(state_mutex);
    void removeRegionNodeIfOwned() TSA_REQUIRES(state_mutex);
    void enterLeaderElection() TSA_REQUIRES(state_mutex);
    void onLeader() TSA_REQUIRES(state_mutex);
};

}
