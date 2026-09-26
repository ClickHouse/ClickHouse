#pragma once

#include <mutex>
#include <Storages/IStorage_fwd.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

class QueryMetadataCache
{
public:
    using StorageMetadataCache = std::unordered_map<const IStorage *, StorageMetadataPtr>;
    using StorageSnapshotCache = std::unordered_map<const IStorage *, StorageSnapshotPtr>;

    std::pair<StorageMetadataCache *, std::unique_lock<std::mutex>> getStorageMetadataCache() const;

    std::pair<StorageSnapshotCache *, std::unique_lock<std::mutex>> getStorageSnapshotCache() const;

private:
    mutable StorageMetadataCache storage_metadata_cache;
    mutable std::mutex storage_metadata_cache_mutex;

    mutable StorageSnapshotCache storage_snapshot_cache;
    mutable std::mutex storage_snapshot_cache_mutex;
};

using QueryMetadataCachePtr = std::shared_ptr<QueryMetadataCache>;
using QueryMetadataCacheWeakPtr = std::weak_ptr<QueryMetadataCache>;

}
