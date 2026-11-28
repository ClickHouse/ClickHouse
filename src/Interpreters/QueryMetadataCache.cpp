#include <Interpreters/QueryMetadataCache.h>

namespace DB
{

std::pair<QueryMetadataCache::StorageMetadataCache *, std::unique_lock<std::mutex>> QueryMetadataCache::getStorageMetadataCache() const
{
    return std::make_pair(&storage_metadata_cache, std::unique_lock(storage_metadata_cache_mutex));
}

std::pair<QueryMetadataCache::StorageSnapshotCache *, std::unique_lock<std::mutex>> QueryMetadataCache::getStorageSnapshotCache() const
{
    return std::make_pair(&storage_snapshot_cache, std::unique_lock(storage_snapshot_cache_mutex));
}

void QueryMetadataCache::clear() const
{
    {
        std::lock_guard lock(storage_metadata_cache_mutex);
        storage_metadata_cache.clear();
    }
    std::lock_guard lock(storage_snapshot_cache_mutex);
    storage_snapshot_cache.clear();
}

}
