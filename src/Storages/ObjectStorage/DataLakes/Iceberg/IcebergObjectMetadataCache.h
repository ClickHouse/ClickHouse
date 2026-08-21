#pragma once
#include "config.h"

#if USE_AVRO

#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

namespace ProfileEvents
{
    extern const Event IcebergObjectMetadataCacheHits;
    extern const Event IcebergObjectMetadataCacheMisses;
    extern const Event IcebergObjectMetadataCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric IcebergObjectMetadataCacheBytes;
    extern const Metric IcebergObjectMetadataCacheFiles;
}

namespace DB::Iceberg
{

/// Per-pod cache of object metadata (etag/size/mtime) for Iceberg data and
/// delete files, keyed by their resolved storage path.
///
/// Iceberg files are immutable: data/delete files are content-addressed by
/// UUID in their names and are never overwritten (updates write new files;
/// compaction rewrites under new names). Their HEAD response is therefore
/// stable for the lifetime of the file, and fetching it once per path removes
/// the per-query S3 HEAD that otherwise dominates warm point lookups
/// (measured: one S3HeadObject per query through the object-storage layer).
///
/// The real etag is preserved (s3s/minio/S3 all provide one), so etag-keyed
/// consumers (filesystem cache strong keys, s3_validate_etag_on_read, the
/// _etag virtual column) keep working unchanged.
struct IcebergObjectMetadataCacheWeightFunction
{
    size_t operator()(const ObjectMetadata & /*metadata*/) const
    {
        // Path + fixed metadata struct (~200B); weight conservatively.
        return 256;
    }
};

class IcebergObjectMetadataCache : public CacheBase<String, ObjectMetadata, std::hash<String>, IcebergObjectMetadataCacheWeightFunction>
{
public:
    using Base = CacheBase<String, ObjectMetadata, std::hash<String>, IcebergObjectMetadataCacheWeightFunction>;

    IcebergObjectMetadataCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, CurrentMetrics::IcebergObjectMetadataCacheBytes, CurrentMetrics::IcebergObjectMetadataCacheFiles, max_size_in_bytes, max_count, size_ratio)
    {
    }

    /// getOrSet with the hit/miss ProfileEvents wired (CacheBase only
    /// maintains internal counters; the IcebergMetadataFilesCache-style
    /// wrappers increment ProfileEvents explicitly).
    template <typename LoadFunc>
    ObjectMetadata getOrSetMetadata(const String & key, LoadFunc && load_func)
    {
        auto [value, inserted] = Base::getOrSet(
            key,
            [&] { return std::make_shared<ObjectMetadata>(load_func()); });
        if (inserted)
            ProfileEvents::increment(ProfileEvents::IcebergObjectMetadataCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::IcebergObjectMetadataCacheHits);
        return *value;
    }

private:
    void onEntryRemoval(const size_t weight_loss, const MappedPtr & /*mapped_ptr*/) override
    {
        ProfileEvents::increment(ProfileEvents::IcebergObjectMetadataCacheWeightLost, weight_loss);
    }
};

using IcebergObjectMetadataCachePtr = std::shared_ptr<IcebergObjectMetadataCache>;

/// Global cache accessor (per pod). Sized for ~100k files (36-partition
/// national tier has ~1400; region tier fewer) at ~256B per entry.
IcebergObjectMetadataCachePtr getObjectMetadataCache();

}

#endif
