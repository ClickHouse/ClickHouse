#pragma once
#include "config.h"

#if USE_AVRO

#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

namespace ProfileEvents
{
    extern const Event IcebergManifestPruneCacheHits;
    extern const Event IcebergManifestPruneCacheMisses;
    extern const Event IcebergManifestPruneCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric IcebergManifestPruneCacheBytes;
    extern const Metric IcebergManifestPruneCacheFiles;
}

namespace DB::Iceberg
{

/// Cache of complete partition-pruning results for one manifest. The value
/// contains only row indexes kept by the partition predicate; a warm query
/// jumps directly to those rows instead of probing a cache entry for every
/// manifest row. Point literals are deliberately excluded from the key, so
/// min-max pruning is still evaluated for every query on the few candidates.
/// The key includes the table snapshot id: Iceberg snapshots may reuse an
/// immutable manifest, but its inherited context and visible file set belong
/// to the snapshot being queried.
struct ManifestPruneCacheValue
{
    std::shared_ptr<const std::vector<size_t>> candidate_row_indexes;
};

struct ManifestPruneCacheWeightFunction
{
    size_t operator()(const ManifestPruneCacheValue & v) const
    {
        return 64 + (v.candidate_row_indexes ? v.candidate_row_indexes->size() * sizeof(size_t) : 0);
    }
};

class ManifestPruneCache : public CacheBase<String, ManifestPruneCacheValue, std::hash<String>, ManifestPruneCacheWeightFunction>
{
public:
    using Base = CacheBase<String, ManifestPruneCacheValue, std::hash<String>, ManifestPruneCacheWeightFunction>;

    ManifestPruneCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, CurrentMetrics::IcebergManifestPruneCacheBytes, CurrentMetrics::IcebergManifestPruneCacheFiles, max_size_in_bytes, max_count, size_ratio)
    {
    }

private:
    void onEntryRemoval(const size_t weight_loss, const MappedPtr & /*mapped_ptr*/) override
    {
        ProfileEvents::increment(ProfileEvents::IcebergManifestPruneCacheWeightLost, weight_loss);
    }
};

using ManifestPruneCachePtr = std::shared_ptr<ManifestPruneCache>;

// Global prune cache accessor for SYSTEM CLEAR
ManifestPruneCachePtr getGlobalPruneCache();
void clearGlobalPruneCache();

}

#endif
