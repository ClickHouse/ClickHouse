#pragma once

#include <thread>
#include <Core/BackgroundSchedulePool.h>
#include <Common/ZooKeeper/Types.h>
#include <Core/Types.h>
#include <common/logger_useful.h>


namespace DB
{

class StorageReplicatedMergeTree;


/** Keeps track of changing the table structure in ZooKeeper and performs the necessary conversions.
  *
  * NOTE This has nothing to do with manipulating partitions,
  *  which are processed through the replication queue.
  */
class ReplicatedMergeTreeAlterThread
{
public:
    ReplicatedMergeTreeAlterThread(StorageReplicatedMergeTree & storage_);

    void start() { task->activateAndSchedule(); }

    void stop() { task->deactivate(); }

private:
    void run();

    StorageReplicatedMergeTree & storage;
    String log_name;
    Logger * log;
    BackgroundSchedulePool::TaskHolder task;
};

}
