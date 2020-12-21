#pragma once

#include <Poco/Event.h>
#include <common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <common/types.h>
#include <thread>
#include <atomic>


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
    ReplicatedMergeTreeRestartingThread(StorageReplicatedMergeTree & storage_);

    void start() { task->activateAndSchedule(); }

    void wakeup() { task->schedule(); }

    void shutdown();

private:
    StorageReplicatedMergeTree & storage;
    String log_name;
    Poco::Logger * log;
    std::atomic<bool> need_stop {false};

    // We need it besides `storage.is_readonly`, because `shutdown()` may be called many times, that way `storage.is_readonly` will not change.
    bool incr_readonly = false;

    /// The random data we wrote into `/replicas/me/is_active`.
    String active_node_identifier;

    BackgroundSchedulePool::TaskHolder task;
    Int64 check_period_ms;                  /// The frequency of checking expiration of session in ZK.
    bool first_time = true;                 /// Activate replica for the first time.
    bool startup_completed = false;

    void run();

    /// Start or stop background threads. Used for partial reinitialization when re-creating a session in ZooKeeper.
    bool tryStartup(); /// Returns false if ZooKeeper is not available.

    /// Note in ZooKeeper that this replica is currently active.
    void activateReplica();

    /// Delete the parts for which the quorum has failed (for the time when the replica was inactive).
    void removeFailedQuorumParts();

    /// If there is an unreachable quorum, and we have a part, then add this replica to the quorum.
    void updateQuorumIfWeHavePart();

    void partialShutdown();
};


}
