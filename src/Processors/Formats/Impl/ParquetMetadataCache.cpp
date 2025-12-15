#include <Processors/Formats/Impl/ParquetMetadataCache.h>

#if USE_AWS_S3
#include <IO/ReadBufferFromS3.h>
#endif
#if USE_AZURE_BLOB_STORAGE
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#endif

#if USE_PARQUET

namespace DB
{

bool ParquetMetadataCacheKey::operator==(const ParquetMetadataCacheKey & other) const
{
    return file_path == other.file_path && file_attr == other.file_attr;
}

size_t ParquetMetadataCacheKeyHash::operator()(const ParquetMetadataCacheKey & key) const
{
    return std::hash<String>{}(key.file_path + key.file_attr);
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

ParquetMetadataCache::ParquetMetadataCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
    : Base(cache_policy, CurrentMetrics::ParquetMetadataCacheBytes, CurrentMetrics::ParquetMetadataCacheFiles, max_size_in_bytes, max_count, size_ratio)
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

static std::optional<ObjectMetadata> tryGetObjectMetadata(ReadBuffer & in)
{
    auto extract_metadata = [](auto * buffer) -> std::optional<ObjectMetadata> {
        if (!buffer)
            return std::nullopt;
        try {
            ObjectMetadata metadata = buffer->getObjectMetadataFromTheLastRequest();
            return metadata;
        }
        catch (...) {
            return std::nullopt;
        }
    };
    if (auto * s3_buffer = dynamic_cast<ReadBufferFromS3*>(&in))
        return extract_metadata(s3_buffer);
    if (auto * azure_buffer = dynamic_cast<ReadBufferFromAzureBlobStorage*>(&in))
        return extract_metadata(azure_buffer);
    return std::nullopt;
}
    
std::pair<String, String> extractObjectAttributes(ReadBuffer & in)
{
    auto log = getLogger("ParquetMetadataCache");
    /// Get the file name as the first part of the hash key
    String full_path = getFileNameFromReadBuffer(in);
    LOG_DEBUG(log, "got file path: {}", full_path);
    String etag;
    /// Get the object metadata
    if (auto maybe_metadata = tryGetObjectMetadata(in))
    {
        etag = maybe_metadata->etag;
        LOG_DEBUG(log, "got etag: {}", etag);
        return std::make_pair(full_path, etag);
    }
    LOG_DEBUG(log, "unable to get etag");
    return std::make_pair(full_path, "");
}
}
#endif
