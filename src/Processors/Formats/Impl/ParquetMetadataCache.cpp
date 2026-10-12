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
    size_t hash = 0;
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.file_path.data(), key.file_path.size()));
    boost::hash_combine(hash, CityHash_v1_0_2::CityHash64(key.etag.data(), key.etag.size()));
    return hash;
}

ParquetMetadataCacheCell::ParquetMetadataCacheCell(parquet::format::FileMetaData metadata_)
    : metadata(std::move(metadata_))
    , memory_bytes(calculateMemorySize() + SIZE_IN_MEMORY_OVERHEAD)
    , kept_statistics(std::make_shared<ParquetKeptStatistics>(std::max<size_t>(memory_bytes / 4, MIN_KEPT_STATISTICS_BYTES)))
{
}
size_t ParquetMetadataCacheCell::calculateMemorySize() const
{
    size_t total_size = sizeof(metadata);

    // SchemaElements parquet::format::SchemaElement
    for (const parquet::format::SchemaElement & element : metadata.schema)
    {
        total_size += sizeof(element);
        total_size += element.name.capacity();
    }

    // RowGroups parquet::format::RowGroup
    for (const parquet::format::RowGroup & row_group : metadata.row_groups)
    {
        total_size += sizeof(row_group);
        total_size += row_group.sorting_columns.capacity() * sizeof(parquet::format::SortingColumn);
        for (const parquet::format::ColumnChunk & column_chunk : row_group.columns)
        {
            total_size += sizeof(column_chunk);

            total_size += column_chunk.file_path.capacity();
            total_size += column_chunk.encrypted_column_metadata.capacity();

            // ColumnMetaData parquet::format::ColumnMetaData
            for (const auto & path : column_chunk.meta_data.path_in_schema)
                total_size += path.capacity();

            for (const auto & kv : column_chunk.meta_data.key_value_metadata)
                total_size += kv.key.capacity() + kv.value.capacity();

            total_size += column_chunk.meta_data.encodings.capacity() * sizeof(parquet::format::Encoding::type);
            total_size += column_chunk.meta_data.encoding_stats.capacity() * sizeof(parquet::format::PageEncodingStats);
            total_size += column_chunk.meta_data.geospatial_statistics.geospatial_types.capacity() * sizeof(int32_t);

            total_size += column_chunk.meta_data.statistics.min_value.capacity();
            total_size += column_chunk.meta_data.statistics.max_value.capacity();
            total_size += column_chunk.meta_data.statistics.min.capacity();
            total_size += column_chunk.meta_data.statistics.max.capacity();

            total_size += column_chunk.meta_data.size_statistics.repetition_level_histogram.capacity() * sizeof(int64_t);
            total_size += column_chunk.meta_data.size_statistics.definition_level_histogram.capacity() * sizeof(int64_t);
        }
    }

    // KeyValueMetadata parquet::format::KeyValueMetadata
    for (const auto & kv : metadata.key_value_metadata)
        total_size += kv.key.capacity() + kv.value.capacity();

    // ColumnOrder parquet::format::ColumnOrders
    for (const auto & order : metadata.column_orders)
        total_size += sizeof(order);

    // Top-level fields
    total_size += metadata.created_by.capacity();
    total_size += metadata.footer_signing_key_metadata.capacity();

    /// String fields for metadata.encryption_algorithm.* generally are either very small
    /// within SSO limits or are empty in the case where we don't use encryption at all
    /// so I'm skipping accounting for them as they'll fall within our SIZE_IN_MEMORY_OVERHEAD
    return total_size;
}

namespace
{

size_t heapBytesOfField(const Field & field)
{
    return field.getType() == Field::Types::String ? field.safeGet<String>().size() : 0;
}

}

void ParquetKeptStatistics::add(const String & column_key, const Ranges & ranges)
{
    std::lock_guard lock(mutex);
    if (ranges_by_column.contains(column_key))
        return;

    size_t bytes = sizeof(Ranges) + column_key.size() + ranges.size() * sizeof(Range);
    for (const auto & range : ranges)
        bytes += heapBytesOfField(range.left) + heapBytesOfField(range.right);
    if (total_bytes + bytes > max_bytes)
        return;
    total_bytes += bytes;
    ranges_by_column.emplace(column_key, std::make_shared<const Ranges>(ranges));
    added = true;
}

ParquetKeptStatistics::RangesPtr ParquetKeptStatistics::get(const String & column_key) const
{
    std::lock_guard lock(mutex);
    auto it = ranges_by_column.find(column_key);
    return it == ranges_by_column.end() ? nullptr : it->second;
}

size_t ParquetKeptStatistics::bytes() const
{
    std::lock_guard lock(mutex);
    return total_bytes;
}

bool ParquetKeptStatistics::takeAdded()
{
    return added.load(std::memory_order_relaxed) && added.exchange(false);
}

size_t ParquetMetadataCacheWeightFunction::operator()(const ParquetMetadataCacheCell & cell) const
{
    return cell.memory_bytes + cell.kept_statistics->bytes();
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
ParquetMetadataCache::MappedPtr ParquetMetadataCache::getCell(const ParquetMetadataCacheKey & key)
{
    auto cell = Base::get(key);
    if (cell)
        countKeptStatistics(key, cell);
    return cell;
}

void ParquetMetadataCache::countKeptStatistics(const ParquetMetadataCacheKey & key, const MappedPtr & cell)
{
    /// `set` recomputes the weight.
    if (cell->kept_statistics->takeAdded())
        Base::replaceIfMatches(key, cell, cell);
}

void ParquetMetadataCache::onEntryRemoval(const size_t weight_loss, const MappedPtr &)
{
    LOG_TRACE(log, "cache eviction");
    ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheWeightLost, weight_loss);
}
}
#endif
