#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/CurrentMetrics.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <base/arithmeticOverflow.h>

#include <initializer_list>
#include <limits>

namespace CurrentMetrics
{
extern const Metric PuffinFilesCacheBytes;
extern const Metric PuffinFilesCacheFiles;
}

namespace ProfileEvents
{
extern const Event PuffinFilesCacheWeightLost;
}

namespace DB
{

namespace
{

UInt64 saturatingAdd(UInt64 left, UInt64 right)
{
    UInt64 result = 0;
    if (common::addOverflow(left, right, result))
        return std::numeric_limits<UInt64>::max();
    return result;
}

UInt64 saturatingAdd(std::initializer_list<UInt64> values)
{
    UInt64 result = 0;
    for (UInt64 value : values)
        result = saturatingAdd(result, value);
    return result;
}

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

UInt64 PuffinFilesCacheKey::approximateMemoryBytes() const
{
    /// Charge string payloads plus the key object / hash-map slot baseline.
    return saturatingAdd(
        {static_cast<UInt64>(sizeof(PuffinFilesCacheKey)),
         static_cast<UInt64>(storage_identity.size()),
         static_cast<UInt64>(file_path.size()),
         static_cast<UInt64>(etag.size()),
         static_cast<UInt64>(referenced_data_file.size())});
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

UInt64 PuffinFilesCacheCell::calculateMemorySize(
    bool is_empty_deletion_vector_,
    const DataLakeObjectMetadata::ExcludedRowsPtr & excluded_rows_,
    UInt64 key_memory_bytes_)
{
    const UInt64 payload_bytes = is_empty_deletion_vector_
        ? 0
        : (excluded_rows_ ? excluded_rows_->getAllocatedBytes() : 0);

    return saturatingAdd(
        {key_memory_bytes_,
         payload_bytes,
         static_cast<UInt64>(sizeof(PuffinFilesCacheCell)),
         static_cast<UInt64>(SIZE_IN_MEMORY_OVERHEAD)});
}

PuffinFilesCacheCell::PuffinFilesCacheCell(DataLakeObjectMetadata::ExcludedRowsPtr excluded_rows_, UInt64 key_memory_bytes_)
    : excluded_rows(std::move(excluded_rows_))
    , is_empty_deletion_vector(!excluded_rows)
    , memory_bytes(calculateMemorySize(is_empty_deletion_vector, excluded_rows, key_memory_bytes_))
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
    , footer_memo_max_count(max_count)
{
}

void PuffinFilesCache::clear()
{
    Base::clear();
    std::lock_guard lock(footer_mutex);
    footer_memo.clear();
}

void PuffinFilesCache::setMaxSizeInBytes(size_t max_size_in_bytes)
{
    Base::setMaxSizeInBytes(max_size_in_bytes);
}

void PuffinFilesCache::setMaxCount(size_t max_count)
{
    Base::setMaxCount(max_count);
    std::lock_guard lock(footer_mutex);
    footer_memo_max_count = max_count;
    if (footer_memo_max_count > 0 && footer_memo.size() > footer_memo_max_count)
        footer_memo.clear();
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
