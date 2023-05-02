#pragma once

#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/Unique/PrimaryIndex.h>
#include <Common/CacheBase.h>

namespace DB
{

class MergeTreeData;

class PrimaryIndexCache : public CacheBase<String, PrimaryIndex>
{
public:
    PrimaryIndexCache(MergeTreeData & storage_, size_t max_cache_size) : CacheBase(max_cache_size), storage(storage_) { }
    PrimaryIndexPtr getOrCreate(const String & partition_id, const MergeTreePartition & partition);

private:
    MergeTreeData & storage;
};
}
