#pragma once

#include <Storages/MergeTree/IMergeTreeCleanupThread.h>
#include <Common/Stopwatch.h>

namespace DB
{

class StorageMergeTree;

class MergeTreeCleanupThread : public IMergeTreeCleanupThread
{
public:
    explicit MergeTreeCleanupThread(StorageMergeTree & storage_);

private:
    StorageMergeTree & storage;

    AtomicStopwatch time_after_previous_cleanup_parts;
    AtomicStopwatch time_after_previous_cleanup_temporary_directories;

    /// Returns a number that is directly proportional to the number of cleaned up objects
    Float32 iterate() override;
};

}
