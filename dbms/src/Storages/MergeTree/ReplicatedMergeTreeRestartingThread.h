#pragma once

#include <Poco/Event.h>
#include <common/logger_useful.h>
#include <Common/BackgroundSchedulePool.h>
#include <Core/Types.h>
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
    ~ReplicatedMergeTreeRestartingThread();

    void wakeup()
    {
        wakeup_event.set();
        task_handle->schedule();
    }

    Poco::Event & getWakeupEvent()
    {
        return wakeup_event;
    }

    void stop()
    {
        need_stop = true;
        wakeup_event.set();
    }

private:
    StorageReplicatedMergeTree & storage;
    Logger * log;
    Poco::Event wakeup_event;
    std::atomic<bool> need_stop {false};

    /// The random data we wrote into `/replicas/me/is_active`.
    String active_node_identifier;

    BackgroundSchedulePool::TaskHandle task_handle;
    Int64 check_period_ms;                  /// The frequency of checking expiration of session in ZK.
    bool first_time = true;                 /// Activate replica for the first time.
    time_t prev_time_of_check_delay = 0;
    bool startup_completed = false;

    void run();
    void completeShutdown();

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
