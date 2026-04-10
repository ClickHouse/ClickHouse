#pragma once
#include <chrono>
#include <IO/CompressionMethod.h>
#include <base/defines.h>
#include <Common/Logger.h>
#include "config.h"

#if USE_AVRO

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/logger_useful.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace ProfileEvents
{
    extern const Event IcebergMetadataFilesCacheMisses;
    extern const Event IcebergMetadataFilesCacheStaleMisses;
    extern const Event IcebergMetadataFilesCacheHits;
    extern const Event IcebergMetadataFilesCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric IcebergMetadataFilesCacheBytes;
    extern const Metric IcebergMetadataFilesCacheFiles;
}

namespace DB
{

namespace Iceberg
{
struct MetadataFileWithInfo
{
    Int32 version;
    String path;
    CompressionMethod compression_method;
};
}

struct LatestMetadataVersion
{
    /// time when it's been received from the remote catalog and cached
    std::chrono::time_point<std::chrono::system_clock> cached_at;
    /// the actual metadata version reference
    Iceberg::MetadataFileWithInfo latest_metadata;
};
using LatestMetadataVersionPtr = std::shared_ptr<LatestMetadataVersion>;

/// The structure that can identify a manifest file. We store it in cache.
/// And we can get `ManifestFileContent` from cache by ManifestFileEntry.
struct ManifestFileCacheKey
{
    Iceberg::IcebergPathFromMetadata manifest_file_path;
    size_t manifest_file_byte_size;
    Int64 added_sequence_number;
    Int64 added_snapshot_id;
    Iceberg::ManifestFileContentType content_type;
};

using ManifestFileCacheKeys = std::vector<ManifestFileCacheKey>;

/// We have three kinds of metadata files in iceberg: metadata object, manifest list and manifest files.
/// For simplicity, we keep them in the same cache.
struct IcebergMetadataFilesCacheCell : private boost::noncopyable
{
    /// The cached entities can be:
    /// - reference to the latest metadata version (metadata.json path) [table_path --> LatestMetadataVersionPtr]
    /// - metadata.json content [file_path --> String]
    /// - manifest list consists of cache keys which will retrieve the manifest file from cache [file_path --> ManifestFileCacheKeys]
    /// - manifest file [file_path --> Iceberg::ManifestFileCacheableInfo]
    std::variant<String, LatestMetadataVersionPtr, ManifestFileCacheKeys, Iceberg::ManifestFileCacheableInfo> cached_element;
    Int64 memory_bytes;

    explicit IcebergMetadataFilesCacheCell(String && metadata_json_str)
        : cached_element(std::move(metadata_json_str))
        , memory_bytes(std::get<String>(cached_element).capacity() + SIZE_IN_MEMORY_OVERHEAD)
    {
    }
    explicit IcebergMetadataFilesCacheCell(LatestMetadataVersionPtr latest_metadata_version)
        : cached_element(latest_metadata_version)
        , memory_bytes(getMemorySizeOfMetadataVersion(latest_metadata_version) + SIZE_IN_MEMORY_OVERHEAD)
    {
    }
    explicit IcebergMetadataFilesCacheCell(ManifestFileCacheKeys && manifest_file_cache_keys_)
        : cached_element(std::move(manifest_file_cache_keys_))
        , memory_bytes(getMemorySizeOfManifestCacheKeys(std::get<ManifestFileCacheKeys>(cached_element)) + SIZE_IN_MEMORY_OVERHEAD)
    {
    }
    explicit IcebergMetadataFilesCacheCell(Iceberg::ManifestFileCacheableInfo manifest_file_)
        : cached_element(std::move(manifest_file_))
        , memory_bytes(3 * std::get<Iceberg::ManifestFileCacheableInfo>(cached_element).file_bytes_size + SIZE_IN_MEMORY_OVERHEAD)
    {
    }
private:
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 200; /// we always underestimate the size of an object;

    static size_t getMemorySizeOfMetadataVersion(const LatestMetadataVersionPtr & metadata_version)
    {
        chassert(metadata_version);
        return sizeof(LatestMetadataVersion) + metadata_version->latest_metadata.path.size();
    }

    static size_t getMemorySizeOfManifestCacheKeys(const ManifestFileCacheKeys & manifest_file_cache_keys)
    {
         size_t total_size = 0;
         for (const auto & entry: manifest_file_cache_keys)
         {
             total_size += sizeof(ManifestFileCacheKey) + entry.manifest_file_path.serialize().capacity();
         }
         return total_size;
    }

};

struct IcebergMetadataFilesCacheWeightFunction
{
    size_t operator()(const IcebergMetadataFilesCacheCell & cell) const
    {
        return cell.memory_bytes;
    }
};

class IcebergMetadataFilesCache : public CacheBase<String, IcebergMetadataFilesCacheCell, std::hash<String>, IcebergMetadataFilesCacheWeightFunction>
{
public:
    using Base = CacheBase<String, IcebergMetadataFilesCacheCell, std::hash<String>, IcebergMetadataFilesCacheWeightFunction>;

    IcebergMetadataFilesCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, CurrentMetrics::IcebergMetadataFilesCacheBytes, CurrentMetrics::IcebergMetadataFilesCacheFiles, max_size_in_bytes, max_count, size_ratio)
    {}

    static String getKey(const String& table_uuid, const String & data_path) { return table_uuid + data_path; }

    template <typename LoadFunc>
    String getOrSetTableMetadata(const String & data_path, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            auto metadata_json_str = load_fn();
            return std::make_shared<IcebergMetadataFilesCacheCell>(std::move(metadata_json_str));
        };
        auto result = Base::getOrSet(data_path, load_fn_wrapper);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheHits);
        return std::get<String>(result.first->cached_element);
    }

    template <typename LoadFunc>
    ManifestFileCacheKeys getOrSetManifestFileCacheKeys(const String & data_path, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            auto && manifest_file_cache_keys = load_fn();
            return std::make_shared<IcebergMetadataFilesCacheCell>(std::move(manifest_file_cache_keys));
        };
        auto result = Base::getOrSet(data_path, load_fn_wrapper);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheHits);
        return std::get<ManifestFileCacheKeys>(result.first->cached_element);
    }

    template <typename LoadFunc>
    Iceberg::ManifestFileCacheableInfo getOrSetManifestFile(const String & data_path, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            Iceberg::ManifestFileCacheableInfo manifest_file = load_fn();
            return std::make_shared<IcebergMetadataFilesCacheCell>(manifest_file);
        };
        auto result = Base::getOrSet(data_path, load_fn_wrapper);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheHits);
        return std::get<Iceberg::ManifestFileCacheableInfo>(result.first->cached_element);
    }

    template <typename LoadFunc>
    LatestMetadataVersionPtr getOrSetLatestMetadataVersion(const String & table_path, const std::optional<String> & table_uuid, LoadFunc && load_fn, time_t tolerated_staleness_ms)
    {
        /// This caching method for latest metadata version:
        /// 1.    Takes two keys to reference a table - path and [optional] uuid
        /// 2.    Probes the cache only if stale values are tolerated - sometimes we just have to force the latest value from the remote catalog and to cache it
        /// 2.1.  Lookup in cache is performed using table uuid if it's provided, or using table path
        /// 3.    If the value needs to be forced via the remote catalog, we first load it, then we place it in cache under both keys

        /// NOTE: There're couple of caveats and nuances regarding current implementation
        ///       (a) can't use getOrSet (as should've) at the moment - because load_fn accesses the same cache to extract uuid from the metadata.json file
        ///       (b) also can't use getOrSet - because there's no mechanism to disregard the existing value because of its staleness
        ///       (c) single value is referenced using two keys
        ///       For now the potential impact of these nuances is considered low for the potential gain - in the worst case, we'll call remote catalog more than once, which is still better than now
        /// TODO:
        ///       (a) and (c) will be solved by moving to AsyncIterator for Iceberg - the loading process should be decoupled, refactored and broken down into independent pieces;
        ///                   and later by refactoring caching to create a new logical layer of snapshot caching
        ///       (b) #97410 will address several design flaws of CacheBase + will introduce custom predicates for get/set operations

        /// tolerated_staleness_ms=0 would mean that a non-cached value is required
        if (tolerated_staleness_ms > 0)
        {
            const String & data_path = table_uuid ? *table_uuid : table_path;
            auto found = Base::get(data_path);

            if (found)
            {
                LatestMetadataVersionPtr cell = std::get<LatestMetadataVersionPtr>(found->cached_element);
                if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - cell->cached_at).count() <= tolerated_staleness_ms)
                {
                    /// the cached element is found and it's not stale accurding to our expectation
                    ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheHits);
                    return cell;
                }
                else
                {
                    ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheStaleMisses);
                }
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheMisses);
            }
        }

        LatestMetadataVersionPtr cell = std::make_shared<LatestMetadataVersion>();
        cell->latest_metadata = load_fn();
        cell->cached_at = std::chrono::system_clock::now();

        Base::set(table_path, std::make_shared<IcebergMetadataFilesCacheCell>(cell));
        if (table_uuid)
            Base::set(*table_uuid, std::make_shared<IcebergMetadataFilesCacheCell>(cell));

        return cell;
    }


private:
    /// Called for each individual entry being evicted from cache
    void onEntryRemoval(const size_t weight_loss, const MappedPtr & mapped_ptr) override
    {
        ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheWeightLost, weight_loss);
        UNUSED(mapped_ptr);
    }
};

using IcebergMetadataFilesCachePtr = std::shared_ptr<IcebergMetadataFilesCache>;

}
#endif
