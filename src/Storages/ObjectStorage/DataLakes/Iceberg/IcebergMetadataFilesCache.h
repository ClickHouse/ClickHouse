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
struct ManifestListCacheCell
{
    struct ManifestFileEntry
    {
        String data_path;
        Int64 added_sequence_number;
    };
    using CachedManifestList = std::list<ManifestFileEntry>;
    CachedManifestList cached_manifest_files;
    size_t getMemoryBytes() const
    {
        size_t total_size = sizeof(std::list<ManifestFileEntry>);
        for (const auto & entry: cached_manifest_files)
        {
            total_size += sizeof(ManifestFileEntry) + entry.data_path.capacity();
        }
        return total_size + 200;
    }

    /// getManifestList will return a list of ManifestFilePtr, from cache or construct from scratch.
    template<typename LoadFunc>
    Iceberg::ManifestListPtr getManifestList(LoadFunc && load_func) const
    {
        Iceberg::ManifestList manifest_list;
        for (const auto & entry : cached_manifest_files)
        {
            /// in the load function, we will visit cache again.
            /// If a manifest file does not exist in cache, we can read the content from remote file and construct them.
            auto manifest_file_ptr = load_func(entry.data_path, entry.added_sequence_number);
            manifest_list.push_back(manifest_file_ptr);
        }
        return std::make_shared<Iceberg::ManifestList>(std::move(manifest_list));
    }

    explicit ManifestListCacheCell(CachedManifestList && cached_manifest_list_)
        : cached_manifest_files(std::move(cached_manifest_list_))
    {
    }

    ManifestListCacheCell() = default;
};

/// We have three kind of metadata files in iceberg: metadata object, manifest list and manifest files.
/// For simplicity, we keep them in same cache.
struct IcebergMetadataFilesCacheCell
{
    Poco::JSON::Object::Ptr metadata_object;
    ManifestListCacheCell manifest_list_cell;
    Iceberg::ManifestFilePtr manifest_file;

    size_t memory_bytes;

    explicit IcebergMetadataFilesCacheCell(Poco::JSON::Object::Ptr metadata_object_, size_t memory_bytes_)
        : metadata_object(metadata_object_)
        , memory_bytes(memory_bytes_)
    {
        CurrentMetrics::add(CurrentMetrics::IcebergMetadataFilesCacheSize, memory_bytes);
    }
    explicit IcebergMetadataFilesCacheCell(ManifestListCacheCell && manifest_list_)
        : manifest_list_cell(std::move(manifest_list_))
        , memory_bytes(manifest_list_cell.getMemoryBytes())
    {
        CurrentMetrics::add(CurrentMetrics::IcebergMetadataFilesCacheSize, memory_bytes);
    }
    explicit IcebergMetadataFilesCacheCell(Iceberg::ManifestFilePtr manifest_file_)
        : manifest_file(manifest_file_)
        , memory_bytes(manifest_file->getMemoryBytes())
    {
        CurrentMetrics::add(CurrentMetrics::IcebergMetadataFilesCacheSize, memory_bytes);
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

    Poco::JSON::Object::Ptr getMetadataObject(const String & data_path)
    {
        auto result = Base::get(data_path);
        if (!result)
        {
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheMisses);
            return nullptr;
        }
        ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheHits);
        return result->metadata_object;
    }

    void setMetadataObject(const String & data_path, Poco::JSON::Object::Ptr metadata_object, size_t memory_bytes)
    {
        Base::set(data_path, std::make_shared<IcebergMetadataFilesCacheCell>(metadata_object, memory_bytes));
    }

    template <typename LoadFunc>
    Iceberg::ManifestListPtr getManifestList(const String & data_path, LoadFunc && load)
    {
        const auto result = Base::get(data_path);
        if (!result)
        {
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheMisses);
            return nullptr;
        }
        ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheHits);
        return result->manifest_list_cell.getManifestList(load);
    }

    void setManifestList(const String & data_path, ManifestListCacheCell::CachedManifestList && cached_manifest_list)
    {
        Base::set(data_path, std::make_shared<IcebergMetadataFilesCacheCell>(ManifestListCacheCell(std::move(cached_manifest_list))));
    }

    Iceberg::ManifestFilePtr getManifestFile(const String & data_path)
    {
        auto result = Base::get(data_path);
        if (!result)
        {
            ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheMisses);
            return nullptr;
        }
        ProfileEvents::increment(ProfileEvents::IcebergMetadataFilesCacheHits);
        return result->manifest_file;
    }

    void setManifestFile(const String & data_path, Iceberg::ManifestFilePtr manifest_file)
    {
        Base::set(data_path, std::make_shared<IcebergMetadataFilesCacheCell>(manifest_file));
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
