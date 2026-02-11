#include <Processors/Formats/Impl/ParquetMetadataCache.h>

#if USE_PARQUET

namespace CurrentMetrics
{
extern const Metric ParquetMetadataCacheBytes;
extern const Metric ParquetMetadataCacheFiles;
}

namespace ProfileEvents
{
extern const Event ParquetMetadataCacheWeightLost;
}

namespace DB
{

bool ParquetMetadataCacheKey::operator==(const ParquetMetadataCacheKey & other) const
{
    return file_path == other.file_path && etag == other.etag;
}

size_t ParquetMetadataCacheKeyHash::operator()(const ParquetMetadataCacheKey & key) const
{
    return std::hash<String>{}(key.file_path + key.etag);
}

ParquetMetadataCacheCell::ParquetMetadataCacheCell(parquet::format::FileMetaData metadata_)
    : metadata(std::move(metadata_))
    , memory_bytes(calculateMemorySize() + SIZE_IN_MEMORY_OVERHEAD)
{
}
size_t ParquetMetadataCacheCell::calculateMemorySize() const
{
    return sizeof(metadata) + metadata.schema.size() * 100;
}

size_t ParquetMetadataCacheWeightFunction::operator()(const ParquetMetadataCacheCell & cell) const
{
    return cell.memory_bytes;
}

ParquetMetadataCache::ParquetMetadataCache(
    const String & cache_policy,
    size_t max_size_in_bytes,
    size_t max_count,
    double size_ratio)
    : Base(cache_policy,
        CurrentMetrics::ParquetMetadataCacheBytes,
        CurrentMetrics::ParquetMetadataCacheFiles,
        max_size_in_bytes,
        max_count,
        size_ratio)
    , log(getLogger("ParquetMetadataCache"))
{
}
ParquetMetadataCacheKey ParquetMetadataCache::createKey(const String & file_path, const String & file_attr)
{
    return ParquetMetadataCacheKey{file_path, file_attr};
}
void ParquetMetadataCache::onEntryRemoval(const size_t weight_loss, const MappedPtr &)
{
    LOG_TRACE(log, "cache eviction");
    ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheWeightLost, weight_loss);
}
}
#endif
