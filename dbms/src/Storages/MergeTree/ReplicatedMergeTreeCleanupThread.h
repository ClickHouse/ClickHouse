#pragma once

#include <Core/Types.h>
#include <common/logger_useful.h>
#include <thread>
#include <map>


namespace DB
{

class StorageReplicatedMergeTree;


/** Removes obsolete data from a table of type ReplicatedMergeTree.
  */
class ReplicatedMergeTreeCleanupThread
{
public:
    ReplicatedMergeTreeCleanupThread(StorageReplicatedMergeTree & storage_);

    ~ReplicatedMergeTreeCleanupThread()
    {
        if (thread.joinable())
            thread.join();
    }

private:
    StorageReplicatedMergeTree & storage;
    Logger * log;
    std::thread thread;

    void run();
    void iterate();

    /// Remove old records from ZooKeeper.
    void clearOldLogs();

    /// Remove old block hashes from ZooKeeper. This makes a leading replica.
    void clearOldBlocks();

    std::map<String, Int64> cached_block_ctime;

    /// TODO Removing old quorum/failed_parts
    /// TODO Removing old nonincrement_block_numbers
};


}
