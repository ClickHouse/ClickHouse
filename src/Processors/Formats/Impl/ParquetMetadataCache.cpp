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
