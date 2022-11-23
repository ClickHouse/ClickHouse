#pragma once

#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/UniqueMergeTree/PrimaryIndex.h>
#include <Common/CacheBase.h>

namespace DB
{
class StorageUniqueMergeTree;

class PrimaryIndexCache : public CacheBase<String, PrimaryIndex>
{
public:
    PrimaryIndexCache(StorageUniqueMergeTree & storage_, size_t max_cache_size) : CacheBase(max_cache_size), storage(storage_) { }
    PrimaryIndexPtr getOrCreate(const String & partition_id, const MergeTreePartition & partition);

private:
    StorageUniqueMergeTree & storage;
};
}
