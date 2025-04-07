#pragma once
#include "config.h"

#if USE_AVRO

#include <Core/Settings.h>
#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/logger_useful.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace ProfileEvents
{
    extern const Event IcebergMetadataFilesCacheMisses;
    extern const Event IcebergMetadataFilesCacheHits;
    extern const Event IcebergMetadataFilesCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric IcebergMetadataFilesCacheSize;
}

namespace DB
{

/// Manifest list contains only a list of manifest files, so we don't have to store the deserialized manifest_list content.
/// In ManifestListCacheCell we store a list of manifest files' data path and sequence number,
/// so we can get manifest files from cache or read and construct them.
/// struct ManifestListCacheCell
/// {
///     using CachedManifestList = std::list<ManifestFileEntry>;
///     CachedManifestList cached_manifest_files;
///     size_t getSizeInMemory() const
///     {
///         size_t total_size = sizeof(std::list<ManifestFileEntry>);
///         for (const auto & entry: cached_manifest_files)
///         {
///             total_size += sizeof(ManifestFileEntry) + entry.data_path.capacity();
///         }
///         return total_size;
///     }
///
///     /// getManifestList will return a list of ManifestFilePtr, from cache or construct from scratch.
///     template<typename LoadFunc>
///     Iceberg::ManifestListPtr getManifestList(LoadFunc && load_func) const
///     {
///         Iceberg::ManifestList manifest_list;
///         for (const auto & entry : cached_manifest_files)
///         {
///             /// in the load function, we will visit cache again.
///             /// If a manifest file does not exist in cache, we can read the content from remote file and construct them.
///             auto manifest_file_ptr = load_func(entry.data_path, entry.added_sequence_number);
///             manifest_list.push_back(manifest_file_ptr);
///         }
///         return std::make_shared<Iceberg::ManifestList>(std::move(manifest_list));
///     }
///
///     explicit ManifestListCacheCell(CachedManifestList && cached_manifest_list_)
///         : cached_manifest_files(std::move(cached_manifest_list_))
///     {
///     }
///
///     ManifestListCacheCell() = default;
/// };

/// The structure that can identify a manifest file. We store it in cache.
/// And we can get `ManifestFileContent` from cache by ManifestFileEntry.
struct ManifestFileCacheKey
{
    String manifest_file_path;
    Int64 added_sequence_number;
};

using ManifestFileCacheKeys = std::vector<ManifestFileCacheKey>;

/// We have three kind of metadata files in iceberg: metadata object, manifest list and manifest files.
/// For simplicity, we keep them in same cache.
struct IcebergMetadataFilesCacheCell
{
    Poco::JSON::Object::Ptr metadata_object;
    ManifestFileCacheKeys manifest_file_cache_keys;
    Iceberg::ManifestFilePtr manifest_file;

    Int64 memory_bytes;
    CurrentMetrics::Increment metric_increment;
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 200; /// we always underestimate the size of an object;

    static size_t getMemorySizeOfManifestCacheKeys(const ManifestFileCacheKeys & manifest_file_cache_keys)
    {
         size_t total_size = 0;
         for (const auto & entry: manifest_file_cache_keys)
         {
             total_size += sizeof(Iceberg::ManifestFileEntry) + entry.manifest_file_path.capacity();
         }
         return total_size;
    }

    explicit IcebergMetadataFilesCacheCell(Poco::JSON::Object::Ptr metadata_object_, size_t memory_bytes_)
        : metadata_object(metadata_object_)
        , memory_bytes(memory_bytes_ + SIZE_IN_MEMORY_OVERHEAD)
        , metric_increment{CurrentMetrics::IcebergMetadataFilesCacheSize, memory_bytes}
    {
    }
    explicit IcebergMetadataFilesCacheCell(ManifestFileCacheKeys && manifest_file_cache_keys_)
        : manifest_file_cache_keys(std::move(manifest_file_cache_keys_))
        , memory_bytes(getMemorySizeOfManifestCacheKeys(manifest_file_cache_keys) + SIZE_IN_MEMORY_OVERHEAD)
        , metric_increment{CurrentMetrics::IcebergMetadataFilesCacheSize, memory_bytes}
    {
    }
    explicit IcebergMetadataFilesCacheCell(Iceberg::ManifestFilePtr manifest_file_)
        : manifest_file(manifest_file_)
        , memory_bytes(manifest_file->getSizeInMemory() + SIZE_IN_MEMORY_OVERHEAD)
        , metric_increment{CurrentMetrics::IcebergMetadataFilesCacheSize, memory_bytes}
    {
    }
    IcebergMetadataFilesCacheCell(const IcebergMetadataFilesCacheCell &) = delete;
    IcebergMetadataFilesCacheCell & operator=(const IcebergMetadataFilesCacheCell &) = delete;
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
        : Base(cache_policy, max_size_in_bytes, max_count, size_ratio)
    {}

    static String getKey(StorageObjectStorage::ConfigurationPtr config, const String & data_path)
    {
        return std::filesystem::path(config->getDataSourceDescription()) / data_path;
    }

    template <typename LoadFunc>
    Poco::JSON::Object::Ptr getOrSetTableMetadata(const String & data_path, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            const auto & [json_ptr, json_size] = load_fn();
            return std::make_shared<IcebergMetadataFilesCacheCell>(json_ptr, json_size);
        };
        auto result = Base::getOrSet(data_path, load_fn_wrapper);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheHits);
        return result.first->metadata_object;
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
        return result.first->manifest_file_cache_keys;
    }

    template <typename LoadFunc>
    Iceberg::ManifestFilePtr getOrSetManifestFile(const String & data_path, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            Iceberg::ManifestFilePtr manifest_file = load_fn();
            return std::make_shared<IcebergMetadataFilesCacheCell>(manifest_file);
        };
        auto result = Base::getOrSet(data_path, load_fn_wrapper);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheHits);
        return result.first->manifest_file;
    }

private:
    void onRemoveOverflowWeightLoss(size_t weight_loss) override
    {
        ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheWeightLost, weight_loss);
    }
};

using IcebergMetadataFilesCachePtr = std::shared_ptr<IcebergMetadataFilesCache>;

}
#endif
