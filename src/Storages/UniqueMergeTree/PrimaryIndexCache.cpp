#include <Storages/StorageUniqueMergeTree.h>
#include <Storages/UniqueMergeTree/PrimaryIndexCache.h>

namespace DB
{

PrimaryIndexPtr PrimaryIndexCache::getOrCreate(const String & partition_id, const MergeTreePartition & partition)
{
    if (auto it = get(partition_id); it && it->state == PrimaryIndex::State::VALID)
        return it;

    auto new_primary_index
        = std::make_shared<PrimaryIndex>(partition, storage.getSettings()->unique_merge_tree_max_primary_index_cache_size, storage);
    set(partition_id, new_primary_index);
    return new_primary_index;
}
}
