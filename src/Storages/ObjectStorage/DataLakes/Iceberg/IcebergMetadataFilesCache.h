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

/// The structure that can identify a manifest file. We store it in cache.
/// And we can get `ManifestFileContent` from cache by ManifestFileEntry.
struct ManifestFileCacheKey
{
    String manifest_file_path;
    Int64 added_sequence_number;
};

using ManifestFileCacheKeys = std::vector<ManifestFileCacheKey>;

/// We have three kinds of metadata files in iceberg: metadata object, manifest list and manifest files.
/// For simplicity, we keep them in the same cache.
struct IcebergMetadataFilesCacheCell : private boost::noncopyable
{
    /// The cached element could be
    /// - metadata.json deserialized as Poco::JSON::Object::Ptr
    /// - manifest list consists of cache keys which will retrieve the manifest file from cache
    /// - manifest file
    std::variant<Poco::JSON::Object::Ptr, ManifestFileCacheKeys, Iceberg::ManifestFilePtr> cached_element;
    Int64 memory_bytes;

    explicit IcebergMetadataFilesCacheCell(Poco::JSON::Object::Ptr metadata_object_, size_t memory_bytes_)
        : cached_element(metadata_object_)
        , memory_bytes(memory_bytes_ + SIZE_IN_MEMORY_OVERHEAD)
        , metric_increment{CurrentMetrics::IcebergMetadataFilesCacheSize, memory_bytes}
    {
    }
    explicit IcebergMetadataFilesCacheCell(ManifestFileCacheKeys && manifest_file_cache_keys_)
        : cached_element(std::move(manifest_file_cache_keys_))
        , memory_bytes(getMemorySizeOfManifestCacheKeys(std::get<ManifestFileCacheKeys>(cached_element)) + SIZE_IN_MEMORY_OVERHEAD)
        , metric_increment{CurrentMetrics::IcebergMetadataFilesCacheSize, memory_bytes}
    {
    }
    explicit IcebergMetadataFilesCacheCell(Iceberg::ManifestFilePtr manifest_file_)
        : cached_element(manifest_file_)
        , memory_bytes(std::get<Iceberg::ManifestFilePtr>(cached_element)->getSizeInMemory() + SIZE_IN_MEMORY_OVERHEAD)
        , metric_increment{CurrentMetrics::IcebergMetadataFilesCacheSize, memory_bytes}
    {
    }
private:
    CurrentMetrics::Increment metric_increment;
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
        return std::get<Poco::JSON::Object::Ptr>(result.first->cached_element);
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
        return std::get<Iceberg::ManifestFilePtr>(result.first->cached_element);
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
