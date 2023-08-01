#pragma once

#include <optional>
#include <Interpreters/Context_fwd.h>
#include <Common/ZooKeeper/ZooKeeper.h>

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
 * 1. A leader can fetch part from any replica (in and out of its region)
 * 2. A follower can only fetch part from the leader replica
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
    /// When follower fetch a part from replica and leader doesn't have the part yet, how long in seconds should it wait for
    /// before trying to fetch again.
    static constexpr int DBMS_DEFAULT_WAIT_FOR_REGION_LEADER = 20;
    static constexpr int DBMS_DEFAULT_WAIT_FOR_REGION_LEADER_TIMEOUT = 900;
    explicit ReplicatedMergeTreeGeoReplicationController(StorageReplicatedMergeTree & storage_);

    bool isValid() const { return !region.empty(); }

    const String & getRegion() const { return region; }

    void startLeaderElection();

    String getCurrentLeader() const;

private:
    StorageReplicatedMergeTree & storage;
    String region;
    zkutil::ZooKeeperPtr current_zookeeper;
    zkutil::LeaderElectionPtr leader_election;
    zkutil::EphemeralNodeHolderPtr leader_lease_holder;

    void onLeader();
    void exitLeaderElection();
    void enterLeaderElection();
};

}
