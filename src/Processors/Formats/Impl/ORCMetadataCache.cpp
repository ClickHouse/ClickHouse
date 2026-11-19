#include <Processors/Formats/Impl/ORCMetadataCache.h>

#if USE_ORC

namespace CurrentMetrics
{
extern const Metric ORCMetadataCacheBytes;
extern const Metric ORCMetadataCacheFiles;
}

namespace ProfileEvents
{
extern const Event ORCMetadataCacheWeightLost;
}

namespace DB
{

bool ORCMetadataCacheKey::operator==(const ORCMetadataCacheKey & other) const
{
    return file_path == other.file_path && etag == other.etag;
}

size_t ORCMetadataCacheKeyHash::operator()(const ORCMetadataCacheKey & key) const
{
    size_t hash = 0;
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.file_path.data(), key.file_path.size()));
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.etag.data(), key.etag.size()));
    return hash;
}

ORCMetadataCacheCell::ORCMetadataCacheCell(String serialized_tail_)
    : serialized_tail(std::move(serialized_tail_))
    , memory_bytes(serialized_tail.capacity() + SIZE_IN_MEMORY_OVERHEAD)
{
}

size_t ORCMetadataCacheWeightFunction::operator()(const ORCMetadataCacheCell & cell) const
{
    return cell.memory_bytes;
}

ORCMetadataCache::ORCMetadataCache(
    const String & cache_policy,
    size_t max_size_in_bytes,
    size_t max_count,
    double size_ratio)
    : Base(cache_policy,
        CurrentMetrics::ORCMetadataCacheBytes,
        CurrentMetrics::ORCMetadataCacheFiles,
        max_size_in_bytes,
        max_count,
        size_ratio)
    , log(getLogger("ORCMetadataCache"))
{
}

ORCMetadataCacheKey ORCMetadataCache::createKey(const String & file_path, const String & file_attr)
{
    return ORCMetadataCacheKey{file_path, file_attr};
}

void ORCMetadataCache::onEntryRemoval(const size_t weight_loss, const MappedPtr &)
{
    LOG_TRACE(log, "cache eviction");
    ProfileEvents::increment(ProfileEvents::ORCMetadataCacheWeightLost, weight_loss);
}
}
#endif
