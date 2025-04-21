#pragma once

#include <Storages/MergeTree/IMergeTreeCleanupThread.h>

namespace DB
{

class StorageMergeTree;

class MergeTreeCleanupThread : public IMergeTreeCleanupThread
{
public:
    explicit MergeTreeCleanupThread(StorageMergeTree & storage_);

private:
    StorageMergeTree & storage;

    /// Returns a number this is directly proportional to the number of cleaned up blocks
    Float32 iterate() override;
};

}
