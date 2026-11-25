#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeMetadataCache.h>

#if USE_DELTA_KERNEL_RS

#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#include <fmt/format.h>

namespace ProfileEvents
{
    extern const Event DeltaLakeMetadataCacheHits;
    extern const Event DeltaLakeMetadataCacheMisses;
    extern const Event DeltaLakeMetadataCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric DeltaLakeMetadataCacheBytes;
    extern const Metric DeltaLakeMetadataCacheEntries;
}

namespace DeltaLake
{

size_t SnapshotDataFiles::getFileWeight(const ScannedDataFile & file)
{
    return sizeof(ScannedDataFile) + sizeof(ScannedDataFilePtr) + file.path.capacity()
        + (file.statistics ? file.statistics->allocatedBytes() : 0);
}

SnapshotDataFilesCollector::SnapshotDataFilesCollector(size_t max_weight_)
    : max_weight(max_weight_)
{
}

void SnapshotDataFilesCollector::add(const ScannedDataFilePtr & file)
{
    if (!active)
        return;
    weight += SnapshotDataFiles::getFileWeight(*file);
    if (weight > max_weight)
    {
        active = false;
        data_files.files = {};
        return;
    }
    data_files.files.push_back(file);
}

std::shared_ptr<SnapshotDataFiles> SnapshotDataFilesCollector::release()
{
    return std::make_shared<SnapshotDataFiles>(std::move(data_files));
}

}

namespace DB
{

size_t DeltaLakeMetadataCacheWeightFunction::operator()(const DeltaLake::SnapshotDataFiles & data_files) const
{
    size_t weight = sizeof(DeltaLake::SnapshotDataFiles);
    for (const auto & file : data_files.files)
        weight += DeltaLake::SnapshotDataFiles::getFileWeight(*file);
    return weight;
}

DeltaLakeMetadataCache::DeltaLakeMetadataCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
    : Base(cache_policy, CurrentMetrics::DeltaLakeMetadataCacheBytes, CurrentMetrics::DeltaLakeMetadataCacheEntries, max_size_in_bytes, max_count, size_ratio)
{
}

String DeltaLakeMetadataCache::makeKey(const String & storage_description, const String & table_location, const String & table_id, UInt64 version)
{
    return fmt::format("{}\n{}\n{}\n{}", storage_description, table_location, table_id, version);
}

DeltaLakeMetadataCache::MappedPtr DeltaLakeMetadataCache::get(const Key & key)
{
    auto result = Base::get(key);
    ProfileEvents::increment(result ? ProfileEvents::DeltaLakeMetadataCacheHits : ProfileEvents::DeltaLakeMetadataCacheMisses);
    return result;
}

void DeltaLakeMetadataCache::onEntryRemoval(size_t weight_loss, const MappedPtr &)
{
    ProfileEvents::increment(ProfileEvents::DeltaLakeMetadataCacheWeightLost, weight_loss);
}

}

#endif
