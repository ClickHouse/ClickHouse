#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/CurrentMetrics.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

namespace CurrentMetrics
{
extern const Metric PuffinFilesCacheBytes;
extern const Metric PuffinFilesCacheFiles;
extern const Metric PuffinFooterCacheBytes;
extern const Metric PuffinFooterCacheFiles;
}

namespace ProfileEvents
{
extern const Event PuffinFilesCacheWeightLost;
extern const Event PuffinFooterCacheWeightLost;
}

namespace DB
{

namespace
{

constexpr size_t FOOTER_BLOB_OVERHEAD = 64;
constexpr size_t FOOTER_CELL_OVERHEAD = 200;

}

DataLakeObjectMetadata::ExcludedRowsPtr PuffinFilesCache::cloneExcludedRows(const PuffinFilesCacheCell & cell)
{
    if (cell.is_empty_deletion_vector)
        return nullptr;

    auto cloned = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    cloned->merge(*cell.excluded_rows);
    return cloned;
}

bool PuffinFilesCacheKey::operator==(const PuffinFilesCacheKey & other) const
{
    return storage_identity == other.storage_identity
        && file_path == other.file_path
        && etag == other.etag
        && content_offset == other.content_offset
        && content_size_in_bytes == other.content_size_in_bytes
        && referenced_data_file == other.referenced_data_file
        && expected_cardinality == other.expected_cardinality
        && data_file_record_count == other.data_file_record_count;
}

size_t PuffinFilesCacheKeyHash::operator()(const PuffinFilesCacheKey & key) const
{
    size_t hash = 0;
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.storage_identity.data(), key.storage_identity.size()));
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.file_path.data(), key.file_path.size()));
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.etag.data(), key.etag.size()));
    boost::hash_combine(hash, key.content_offset);
    boost::hash_combine(hash, key.content_size_in_bytes);
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.referenced_data_file.data(), key.referenced_data_file.size()));
    boost::hash_combine(hash, key.expected_cardinality);
    boost::hash_combine(hash, key.data_file_record_count);
    return hash;
}

UInt64 PuffinFilesCacheCell::calculateMemorySize(bool is_empty_deletion_vector_, const DataLakeObjectMetadata::ExcludedRowsPtr & excluded_rows_)
{
    if (is_empty_deletion_vector_)
        return EMPTY_DELETION_VECTOR_WEIGHT;

    return (excluded_rows_ ? excluded_rows_->getAllocatedBytes() : 0) + SIZE_IN_MEMORY_OVERHEAD;
}

PuffinFilesCacheCell::PuffinFilesCacheCell(DataLakeObjectMetadata::ExcludedRowsPtr excluded_rows_)
    : excluded_rows(std::move(excluded_rows_))
    , is_empty_deletion_vector(!excluded_rows)
    , memory_bytes(calculateMemorySize(is_empty_deletion_vector, excluded_rows))
{
}

size_t PuffinFilesCacheWeightFunction::operator()(const PuffinFilesCacheCell & cell) const
{
    return cell.memory_bytes;
}

bool PuffinFooterCacheKey::operator==(const PuffinFooterCacheKey & other) const
{
    return storage_identity == other.storage_identity && file_path == other.file_path && etag == other.etag;
}

size_t PuffinFooterCacheKeyHash::operator()(const PuffinFooterCacheKey & key) const
{
    size_t hash = 0;
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.storage_identity.data(), key.storage_identity.size()));
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.file_path.data(), key.file_path.size()));
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.etag.data(), key.etag.size()));
    return hash;
}

UInt64 PuffinFooterCacheCell::calculateMemorySize(const BlobsPtr & blobs_)
{
    UInt64 bytes = FOOTER_CELL_OVERHEAD;
    if (!blobs_)
        return bytes;

    bytes += blobs_->capacity() * sizeof(PuffinBlob);
    for (const auto & blob : *blobs_)
    {
        bytes += blob.type.size();
        bytes += blob.compression_codec.size();
        bytes += blob.fields.capacity() * sizeof(Int32);
        for (const auto & [key, value] : blob.properties)
            bytes += key.size() + value.size();
        bytes += FOOTER_BLOB_OVERHEAD;
    }
    return bytes;
}

PuffinFooterCacheCell::PuffinFooterCacheCell(BlobsPtr blobs_)
    : blobs(std::move(blobs_))
    , memory_bytes(calculateMemorySize(blobs))
{
}

size_t PuffinFooterCacheWeightFunction::operator()(const PuffinFooterCacheCell & cell) const
{
    return cell.memory_bytes;
}

PuffinFooterCache::PuffinFooterCache(
    const String & cache_policy,
    size_t max_size_in_bytes,
    size_t max_count,
    double size_ratio)
    : Base(
        cache_policy,
        CurrentMetrics::PuffinFooterCacheBytes,
        CurrentMetrics::PuffinFooterCacheFiles,
        max_size_in_bytes,
        max_count,
        size_ratio)
{
}

void PuffinFooterCache::onEntryRemoval(const size_t weight_loss, const MappedPtr &)
{
    ProfileEvents::increment(ProfileEvents::PuffinFooterCacheWeightLost, weight_loss);
}

PuffinFilesCache::PuffinFilesCache(
    const String & cache_policy,
    size_t max_size_in_bytes,
    size_t max_count,
    double size_ratio)
    : Base(
        cache_policy,
        CurrentMetrics::PuffinFilesCacheBytes,
        CurrentMetrics::PuffinFilesCacheFiles,
        max_size_in_bytes,
        max_count,
        size_ratio)
    , log(getLogger("PuffinFilesCache"))
    /// Footers are much smaller than roaring bitmaps; reuse the same limits so configuration stays one knob.
    , footer_cache(cache_policy, max_size_in_bytes, max_count, size_ratio)
{
}

void PuffinFilesCache::clear()
{
    Base::clear();
    footer_cache.clear();
}

void PuffinFilesCache::setMaxSizeInBytes(size_t max_size_in_bytes)
{
    Base::setMaxSizeInBytes(max_size_in_bytes);
    footer_cache.setMaxSizeInBytes(max_size_in_bytes);
}

void PuffinFilesCache::setMaxCount(size_t max_count)
{
    Base::setMaxCount(max_count);
    footer_cache.setMaxCount(max_count);
}

String PuffinFilesCache::makeStorageIdentity(const IObjectStorage & object_storage)
{
    /// Include getDescription() (S3 endpoint, Azure account URL, Local path, ...) so two
    /// backends with the same bucket/prefix on different hosts do not share cache entries.
    return object_storage.getName() + "://" + object_storage.getDescription() + "/"
        + object_storage.getObjectsNamespace() + "/" + object_storage.getCommonKeyPrefix();
}

std::optional<PuffinFilesCacheKey> PuffinFilesCache::tryCreateKey(
    const String & storage_identity,
    const String & file_path,
    const String & etag,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    const String & referenced_data_file,
    UInt64 expected_cardinality,
    UInt64 data_file_record_count)
{
    if (etag.empty())
        return std::nullopt;

    return PuffinFilesCacheKey{
        storage_identity,
        file_path,
        etag,
        content_offset,
        content_size_in_bytes,
        referenced_data_file,
        expected_cardinality,
        data_file_record_count};
}

std::optional<PuffinFooterCacheKey> PuffinFilesCache::tryCreateFooterKey(
    const String & storage_identity,
    const String & file_path,
    const String & etag)
{
    if (etag.empty())
        return std::nullopt;

    return PuffinFooterCacheKey{storage_identity, file_path, etag};
}

void PuffinFilesCache::onEntryRemoval(const size_t weight_loss, const MappedPtr &)
{
    LOG_TRACE(log, "Puffin files cache eviction");
    ProfileEvents::increment(ProfileEvents::PuffinFilesCacheWeightLost, weight_loss);
}

}
