#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/CurrentMetrics.h>

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
    return file_path == other.file_path
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

    if (!excluded_rows_)
        return 0;

    return excluded_rows_->getAllocatedBytes();
}

PuffinFilesCacheCell::PuffinFilesCacheCell(DataLakeObjectMetadata::ExcludedRowsPtr excluded_rows_)
    : excluded_rows(std::move(excluded_rows_))
    , is_empty_deletion_vector(!excluded_rows)
    , memory_bytes(
          calculateMemorySize(is_empty_deletion_vector, excluded_rows)
          + (is_empty_deletion_vector ? 0 : SIZE_IN_MEMORY_OVERHEAD))
{
}

size_t PuffinFilesCacheWeightFunction::operator()(const PuffinFilesCacheCell & cell) const
{
    return cell.memory_bytes;
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
{
}

std::optional<PuffinFilesCacheKey> PuffinFilesCache::tryCreateKey(
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
        file_path,
        etag,
        content_offset,
        content_size_in_bytes,
        referenced_data_file,
        expected_cardinality,
        data_file_record_count};
}

void PuffinFilesCache::onEntryRemoval(const size_t weight_loss, const MappedPtr &)
{
    LOG_TRACE(log, "Puffin files cache eviction");
    ProfileEvents::increment(ProfileEvents::PuffinFilesCacheWeightLost, weight_loss);
}

}
