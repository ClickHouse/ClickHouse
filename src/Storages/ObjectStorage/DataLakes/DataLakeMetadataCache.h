#pragma once

#include <Core/Settings.h>
#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/logger_useful.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace ProfileEvents
{
    extern const Event DataLakeMetadataCacheMisses;
    extern const Event DataLakeMetadataCacheHits;
    extern const Event DataLakeMetadataCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric DataLakeMetadataCacheSize;
}

namespace DB
{

namespace Setting
{
extern const SettingsBool use_datalake_metadata_cache;
extern const SettingsBool enable_reads_from_datalake_metadata_cache;
extern const SettingsBool enable_writes_to_datalake_metadata_cache;
}

struct DataLakeMetadataCacheCell
{
    DataLakeMetadataPtr metadata;
    size_t memory_bytes;

    explicit DataLakeMetadataCacheCell(DataLakeMetadataPtr metadata_)
        : metadata(std::move(metadata_))
        , memory_bytes(metadata->getMemoryBytes())
    {
        CurrentMetrics::add(CurrentMetrics::DataLakeMetadataCacheSize, memory_bytes);
    }
    DataLakeMetadataCacheCell(const DataLakeMetadataCacheCell &) = delete;
    DataLakeMetadataCacheCell & operator=(const DataLakeMetadataCacheCell &) = delete;
};

struct DataLakeMetadataCacheWeightFunction
{
    size_t operator()(const DataLakeMetadataCacheCell & cell) const
    {
        return cell.memory_bytes;
    }
};

class DataLakeMetadataCache : public CacheBase<String, DataLakeMetadataCacheCell, std::hash<String>, DataLakeMetadataCacheWeightFunction>
{
public:
    using Base = CacheBase<String, DataLakeMetadataCacheCell, std::hash<String>, DataLakeMetadataCacheWeightFunction>;

    DataLakeMetadataCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, max_size_in_bytes, max_count, size_ratio)
    {}

    static String getKey(StorageObjectStorage::ConfigurationPtr config)
    {
        return std::filesystem::path(config->getDataSourceDescription()) / config->getPath();
    }

    template <bool get = true, bool set = true, typename LoadFunc>
    DataLakeMetadataPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto wrapped_load = [&]() -> std::shared_ptr<DataLakeMetadataCacheCell> {
            DataLakeMetadataPtr metadata = load();
            return std::make_shared<DataLakeMetadataCacheCell>(std::move(metadata));
        };

        if constexpr (get && set)
        {
            auto result = Base::getOrSet(key, wrapped_load);
            if (result.second)
                ProfileEvents::increment(ProfileEvents::DataLakeMetadataCacheMisses);
            else
                ProfileEvents::increment(ProfileEvents::DataLakeMetadataCacheHits);
            return result.first->metadata;
        }
        else if constexpr (get)
        {
            auto result = Base::get(key);
            if (result)
            {
                ProfileEvents::increment(ProfileEvents::DataLakeMetadataCacheMisses);
                return result->metadata;
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::DataLakeMetadataCacheHits);
                return nullptr;
            }
        }
        else if constexpr (set)
        {
            auto cell = wrapped_load();
            Base::set(key, cell);
            return cell->metadata;
        }
        return nullptr;
    }
private:
    void onRemoveOverflowWeightLoss(size_t weight_loss) override
    {
        ProfileEvents::increment(ProfileEvents::DataLakeMetadataCacheWeightLost, weight_loss);
    }
};

using DataLakeMetadataCachePtr = std::shared_ptr<DataLakeMetadataCache>;

/// This function decides whether get or set from cache by settings.
/// Right now we only support IcebergMetadata
template<typename LoadFunc>
static DataLakeMetadataPtr createDataLakeMetadataFromCache(ContextPtr local_context, StorageObjectStorage::ConfigurationPtr configuration_ptr, LoadFunc && create_metadata)
{
    DataLakeMetadataCachePtr metadata_cache = local_context->getDataLakeMetadataCache();
    if (local_context->getSettingsRef()[Setting::use_datalake_metadata_cache])
    {
        bool enable_read = local_context->getSettingsRef()[Setting::enable_reads_from_datalake_metadata_cache];
        bool enable_write = local_context->getSettingsRef()[Setting::enable_writes_to_datalake_metadata_cache];
        if (enable_read && enable_write)
        {
            auto metadata = metadata_cache->getOrSet(DataLakeMetadataCache::getKey(configuration_ptr), create_metadata);
            metadata->updateConfiguration(configuration_ptr);
            metadata->update(local_context);
            return metadata;
        }
        else if (enable_read)
        {
            auto metadata = metadata_cache->getOrSet<true, false>(DataLakeMetadataCache::getKey(configuration_ptr), create_metadata);
            if (metadata)
            {
                metadata->updateConfiguration(configuration_ptr);
                metadata->update(local_context);
                return metadata;
            }
        }
        else if (enable_write)
        {
            auto metadata = metadata_cache->getOrSet<false, true>(DataLakeMetadataCache::getKey(configuration_ptr), create_metadata);
            return metadata;
        }
        else
        {
            LOG_INFO(getLogger("DataLakeMetadataCache"), "setting `use_datalake_metadata_cache` is set to true, but neither `enable_reads_from_datalake_metadata_cache` nor `enable_writes_to_datalake_metadata_cache` is enabled");
        }
    }
    return create_metadata();
}

static void writeDataLakeMetadataToCache(ContextPtr local_context, StorageObjectStorage::ConfigurationPtr configuration_ptr, DataLakeMetadataPtr metadata)
{
    DataLakeMetadataCachePtr metadata_cache = local_context->getDataLakeMetadataCache();
    if (local_context->getSettingsRef()[Setting::use_datalake_metadata_cache])
    {
        bool enable_write = local_context->getSettingsRef()[Setting::enable_writes_to_datalake_metadata_cache];
        if (enable_write)
        {
            auto cell = std::make_shared<DataLakeMetadataCacheCell>(std::move(metadata));
            metadata_cache->set(DataLakeMetadataCache::getKey(configuration_ptr), cell);
        }
    }
}

}
