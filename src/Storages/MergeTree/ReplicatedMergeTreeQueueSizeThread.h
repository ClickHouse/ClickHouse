#pragma once

#include <base/types.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <thread>


namespace DB
{

class StorageReplicatedMergeTree;


/** Update the max queue size of all replicas
  */
class ReplicatedMergeTreeQueueSizeThread
{
public:
    explicit ReplicatedMergeTreeQueueSizeThread(StorageReplicatedMergeTree & storage_);

    void start() { task->activateAndSchedule(); }

    void wakeup() { task->schedule(); }

    void shutdown() { task->deactivate(); }

private:
    StorageReplicatedMergeTree & storage;
    String log_name;
    Poco::Logger * log;
    BackgroundSchedulePool::TaskHolder task;

    void run();
    void iterate();
};


}
