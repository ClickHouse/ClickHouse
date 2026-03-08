#pragma once
#include <chrono>
#include <IO/CompressionMethod.h>
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

/// Entities to be cached:
/// a) The latest metadata.json file for an Iceberg table
///         [table_path]:String     -->  [metadata_json_path]:String
/// b) Parsed version of metadata.json
///         [metadata_json_path]    -->  [metadata_snapshot]:MetadataVersion
/// b*) Raw json version of metadata.json TODO remove it
///         [metadata_json_path]    -->  [metadata_json_file]:String
/// c) Manifest list file
///         [manifest_list_path]    -->  [manifest_files_list]:ManifestFileCacheKeys
/// d) Manifest file
///         [manifest_file_path]    -->  [manifest_file]:ManifestFilePtr

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
    /// when it's been received from the remote catalog and cached
    std::chrono::time_point<std::chrono::system_clock> cached_at;

    /// TODO: reuse and extend Iceberg::MetadataFileWithInfo - perhaps, move it here
    Iceberg::MetadataFileWithInfo latest_metadata;
};
using LatestMetadataVersionPtr = std::shared_ptr<LatestMetadataVersion>;

// struct MetadataVersion
// {
//     /// when it's been received from the remote catalog and cached
//     std::chrono::time_point<std::chrono::system_clock> cached_at;
//     /// actual update timestamp from metadata.json
//     Int64 updated_ms;
//     /// vX.metadata.json path (also it's a key) - TODO: perhaps not needed
//     String file_path;
//     /// version of the metadata.json file from its filename, e.g. X for vX.metadata.json - TODO: 32bit, sure?
//     Int32 version;
//     /// raw context of vX.metadata.json file
//     String json_content;
//     /// TODO: reference manifest list files from here (as filenames potentially pointing to inside this cache itself)
// };

/// The structure that can identify a manifest file. We store it in cache.
/// And we can get `ManifestFileContent` from cache by ManifestFileEntry.
struct ManifestFileCacheKey
{
    String manifest_file_path;
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
    /// The cached element could be
    /// - NEW latest metadata.json file path, containing version (LatestMetadataVersion)
    /// - NEW parsed metadata.json context (MetadataVersion) -- TODO: should replace metadata.json (String)
    /// - metadata.json content
    /// - manifest list consists of cache keys which will retrieve the manifest file from cache
    /// - manifest file
    std::variant<String, LatestMetadataVersionPtr, ManifestFileCacheKeys, Iceberg::ManifestFileCacheableInfo> cached_element;
    Int64 memory_bytes;

    explicit IcebergMetadataFilesCacheCell(String && metadata_json_str)
        : cached_element(std::move(metadata_json_str))
        , memory_bytes(std::get<String>(cached_element).capacity() + SIZE_IN_MEMORY_OVERHEAD)
    {
    }
    explicit IcebergMetadataFilesCacheCell(LatestMetadataVersionPtr latest_metadata_version)
        : cached_element(latest_metadata_version)
        , memory_bytes(SIZE_IN_MEMORY_OVERHEAD) /// TODO: calculate the size here
    {
    }
    // explicit IcebergMetadataFilesCacheCell(MetadataVersion && metadata_version)
    //     : cached_element(metadata_version)
    //     , memory_bytes(SIZE_IN_MEMORY_OVERHEAD) /// TODO: calculate the size here
    // {
    // }
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
    /// TODO: I'm not sure why overhead and why the comment says 'always underestimate' -- double check, perhaps we meant to align the size
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 200; /// we always underestimate the size of an object;

    static size_t getMemorySizeOfManifestCacheKeys(const ManifestFileCacheKeys & manifest_file_cache_keys)
    {
         size_t total_size = 0;
         for (const auto & entry: manifest_file_cache_keys)
         {
             total_size += sizeof(ManifestFileCacheKey) + entry.manifest_file_path.capacity();
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
    LatestMetadataVersionPtr getOrSetLatestMetadataVersion(const String & data_path, LoadFunc && load_fn, time_t tolerated_staleness_in_seconds)
    {
        /// tolerated_staleness_in_seconds=0 would mean that a non-cached value is required
        if (tolerated_staleness_in_seconds > 0)
        {
            auto found = Base::get(data_path);
            if (found)
            {
                LatestMetadataVersionPtr cell = std::get<LatestMetadataVersionPtr>(found->cached_element);
                if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - cell->cached_at).count() <= tolerated_staleness_in_seconds)
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
        }

        /// it's either nothing found in cache or it's too old (stale) - in both cases, we'll load it and cache it
        /// TODO: we force loading here and not passing the function to inside cache as it works normally, because
        ///       we'll be accessing the same cache during the loading (specific to this scenario) and that'd cause self-lock
        ///       First, #97410 CacheBase needs to be refactored to support generic entity-based quotation
        ///       Second, we should use two separate caches (can be encapsulated under the same umbrella), 1st keeps logical snapshots (latest version), 2nd files
        ///               and in this case, we could safely access the 2nd cache while loading the element for the 1st one.
        LatestMetadataVersionPtr cell = std::make_shared<LatestMetadataVersion>();
        cell->latest_metadata = load_fn();
        cell->cached_at = std::chrono::system_clock::now();

        /// we're not using getAndSet as the stale could be in cache, and we want to actually forcely set it
        /// TODO: Again, with #97410 by getting a generic caching for TTL & Staleness, we'll be able to force set on a stale key
        /// NOTE: if several threads will concurrently enter this method, and will concurrently load data - this will result in some extra work, which is fine-ish
        ///       but at least it won't lead to race - the last thread will overwrite the cached value, and this is okay
        Base::set(data_path, std::make_shared<IcebergMetadataFilesCacheCell>(cell));
        ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheMisses);
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
