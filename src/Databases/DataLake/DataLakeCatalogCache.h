#pragma once
#include <chrono>
#include "config.h"

#if USE_AVRO

#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Databases/DataLake/ICatalog.h>
#include <Storages/IStorage_fwd.h>

namespace ProfileEvents
{
    extern const Event DataLakeCatalogCacheHits;
    extern const Event DataLakeCatalogCacheMisses;
    extern const Event DataLakeCatalogCacheStaleMisses;
    extern const Event DataLakeCatalogCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric DataLakeCatalogCacheBytes;
    extern const Metric DataLakeCatalogCacheFiles;
}

namespace DataLake
{

/// Cache entry for a single DataLakeCatalog table: the constructed,
/// long-lived StorageObjectStorage resolved from the catalog.
///
/// Caching the constructed storage (not just the TableMetadata) is what
/// makes ENGINE=DataLakeCatalog competitive with the per-table
/// ENGINE=Iceberg path: per-query table resolution otherwise rebuilds
/// the storage object, which re-parses metadata JSON + schema + manifest
/// list on every SELECT (~100ms at 150M rows, measured). Reusing one
/// storage per table matches the per-table engine's amortization:
/// iceberg_metadata_staleness_ms (profile) then refreshes the metadata
/// state on the cached storage as usual.
///
/// Stored per DatabaseDataLake instance (not global) and bounded by
/// catalog_cache_max_entries. TTL is enforced by the caller via
/// catalog_cache_staleness_ms, mirroring IcebergMetadataFilesCache's
/// getOrSetLatestMetadataVersion pattern.
struct DataLakeCatalogCacheEntry
{
    DB::StoragePtr storage;
    std::chrono::system_clock::time_point cached_at;
    size_t weight_bytes;

    DataLakeCatalogCacheEntry() = default;
    explicit DataLakeCatalogCacheEntry(DB::StoragePtr storage_)
        : storage(std::move(storage_))
        , cached_at(std::chrono::system_clock::now())
        , weight_bytes(estimateWeight(storage))
    {
    }

private:
    static size_t estimateWeight(const DB::StoragePtr & s)
    {
        // Rough estimate: parsed metadata state dominated by the schema
        // processor + table state snapshot; weight conservatively.
        size_t w = 4096; // overhead
        if (s)
            w += s->getStorageID().getTableName().size() + s->getStorageID().getDatabaseName().size();
        return w;
    }
};

struct DataLakeCatalogCacheWeightFunction
{
    size_t operator()(const DataLakeCatalogCacheEntry & e) const { return e.weight_bytes; }
};

class DataLakeCatalogCache : public DB::CacheBase<String, DataLakeCatalogCacheEntry, std::hash<String>, DataLakeCatalogCacheWeightFunction>
{
public:
    using Base = DB::CacheBase<String, DataLakeCatalogCacheEntry, std::hash<String>, DataLakeCatalogCacheWeightFunction>;

    DataLakeCatalogCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, CurrentMetrics::DataLakeCatalogCacheBytes, CurrentMetrics::DataLakeCatalogCacheFiles, max_size_in_bytes, max_count, size_ratio)
    {
    }

private:
    void onEntryRemoval(const size_t weight_loss, const MappedPtr & /*mapped_ptr*/) override
    {
        ProfileEvents::increment(ProfileEvents::DataLakeCatalogCacheWeightLost, weight_loss);
    }
};

using DataLakeCatalogCachePtr = std::shared_ptr<DataLakeCatalogCache>;

}

#endif
