#pragma once

#include <Poco/Event.h>
#include <Core/BackgroundSchedulePool.h>
#include <base/types.h>
#include <thread>
#include <atomic>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>


namespace DB
{

class StorageReplicatedMergeTree;


/** Initializes ZK session.
  * Exposes ephemeral nodes. It sets the node values that are required for replica detection.
  * Starts participation in the leader selection. Starts all background threads.
  * Then monitors whether the session has expired. And if it expired, it will reinitialize it.
  */
class ReplicatedMergeTreeRestartingThread
{
public:
    explicit ReplicatedMergeTreeRestartingThread(StorageReplicatedMergeTree & storage_);

    void start(bool schedule);

    void wakeup();

    void shutdown(bool part_of_full_shutdown);

    void run();

private:
    StorageReplicatedMergeTree & storage;
    String log_name;
    LoggerPtr log;
    std::atomic<bool> need_stop {false};

    /// The random data we wrote into `/replicas/me/is_active`.
    String active_node_identifier;

    BackgroundSchedulePool::TaskHolder task;
    Int64 check_period_ms;                  /// The frequency of checking expiration of session in ZK.
    UInt32 consecutive_check_failures = 0;  /// How many consecutive checks have failed
    bool first_time = true;                 /// Activate replica for the first time.

    /// Restarts table if needed, returns false if it failed to restart replica.
    bool runImpl();

    /// Start or stop background threads. Used for partial reinitialization when re-creating a session in ZooKeeper.
    bool tryStartup(); /// Returns false if ZooKeeper is not available.

    /// Note in ZooKeeper that this replica is currently active.
    void activateReplica();

    /// Delete the parts for which the quorum has failed (for the time when the replica was inactive).
    void removeFailedQuorumParts();

    /// If there is an unreachable quorum, and we have a part, then add this replica to the quorum.
    void updateQuorumIfWeHavePart();

    void partialShutdown(bool part_of_full_shutdown = false);

    /// Set readonly mode for table
    void setReadonly(bool on_shutdown = false);

    /// Disable readonly mode for table
    void setNotReadonly();

    /// Fix replica metadata_version if needed
    Int32 fixReplicaMetadataVersionIfNeeded(zkutil::ZooKeeperPtr zookeeper);
};


}
