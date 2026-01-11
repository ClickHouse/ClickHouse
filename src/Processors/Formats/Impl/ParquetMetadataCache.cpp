#include <Processors/Formats/Impl/ParquetMetadataCache.h>

#if USE_PARQUET

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

ParquetMetadataKeyMap::ParquetMetadataKeyMap(size_t max_size_)
    : max_size(max_size_ > 0 ? max_size_ : DEFAULT_MAX_SIZE)
{
    data.reserve(max_size);
}

size_t ParquetMetadataKeyMap::size() const
{
    std::lock_guard lock(mutex);
    return data.size();
}

bool ParquetMetadataKeyMap::empty() const
{
    std::lock_guard lock(mutex);
    return data.empty();
}

void ParquetMetadataKeyMap::clear()
{
    std::lock_guard lock(mutex);
    data.clear();
}

String ParquetMetadataKeyMap::getEtagForFile(const String & file_path)
{
    std::lock_guard lock(mutex);
    auto it = data.find(file_path);
    if (it == data.end())
        // TODO: can't return empty string, maybe throw an exception
        return "";
    return it->second;
}

void ParquetMetadataKeyMap::setEtagForFile(const String & file_path, const String & etag)
{
    std::lock_guard lock(mutex);
    data[file_path] = etag;
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
    , key_map(max_count)
    , log(getLogger("ParquetMetadataCache"))
{
}
ParquetMetadataCacheKey ParquetMetadataCache::createKey(const String & file_path, const String & file_attr)
{
    return ParquetMetadataCacheKey{file_path, file_attr};
}
void ParquetMetadataCache::onEntryRemoval(const size_t weight_loss, const MappedPtr &)
{
    LOG_DEBUG(log, "cache eviction");
    ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheWeightLost, weight_loss);
}
#endif
}
