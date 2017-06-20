#pragma once

#include <thread>
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

    ~ReplicatedMergeTreeAlterThread()
    {
        need_stop = true;
        wakeup_event->set();
        if (thread.joinable())
            thread.join();
    }

private:
    void run();

    StorageReplicatedMergeTree & storage;
    Logger * log;

    zkutil::EventPtr wakeup_event { std::make_shared<Poco::Event>() };
    std::atomic<bool> need_stop { false };

    std::thread thread;
};

}
