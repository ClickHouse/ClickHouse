#pragma once

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>

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

    template <typename LoadFunc>
    DataLakeMetadataPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto wrapped_load = [&]() -> std::shared_ptr<DataLakeMetadataCacheCell> {
            DataLakeMetadataPtr metadata = load();
            return std::make_shared<DataLakeMetadataCacheCell>(std::move(metadata));
        };

        auto result = Base::getOrSet(key, wrapped_load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::DataLakeMetadataCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::DataLakeMetadataCacheHits);
        return result.first->metadata;
    }
private:
    void onRemoveOverflowWeightLoss(size_t weight_loss) override
    {
        ProfileEvents::increment(ProfileEvents::DataLakeMetadataCacheWeightLost, weight_loss);
    }
};

using DataLakeMetadataCachePtr = std::shared_ptr<DataLakeMetadataCache>;

}
