#include <Processors/Formats/Impl/ParquetMetadataCache.h>

#if USE_PARQUET

#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>

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

ParquetMetadataCacheCell::ParquetMetadataCacheCell(
    std::shared_ptr<const Parquet::FileFooter> footer_, std::vector<Parquet::FileColumnChunksPtr> column_chunks_)
    : footer(std::move(footer_))
    , column_chunks(std::move(column_chunks_))
    , memory_bytes(calculateMemorySize() + SIZE_IN_MEMORY_OVERHEAD)
{
}
size_t ParquetMetadataCacheCell::calculateMemorySize() const
{
    const parquet::format::FileMetaData & metadata = footer->metadata;
    size_t total_size = sizeof(*footer);
    total_size += footer->serialized.capacity();
    total_size += footer->column_chunk_offsets.capacity() * sizeof(std::vector<UInt32>);
    for (const auto & offsets : footer->column_chunk_offsets)
        total_size += offsets.capacity() * sizeof(offsets[0]);

    total_size += column_chunks.capacity() * sizeof(column_chunks[0]);
    for (const auto & column : column_chunks)
        if (column)
            total_size += sizeof(*column) + column->memory_bytes;

    // SchemaElements parquet::format::SchemaElement
    total_size += metadata.schema.capacity() * sizeof(parquet::format::SchemaElement);
    for (const parquet::format::SchemaElement & element : metadata.schema)
    {
        total_size += element.name.capacity();
        total_size += element.logicalType.GEOMETRY.crs.capacity() + element.logicalType.GEOGRAPHY.crs.capacity();
    }

    // RowGroups parquet::format::RowGroup, without column chunks
    total_size += metadata.row_groups.capacity() * sizeof(parquet::format::RowGroup);
    for (const parquet::format::RowGroup & row_group : metadata.row_groups)
    {
        total_size += row_group.sorting_columns.capacity() * sizeof(parquet::format::SortingColumn);
    }

    // KeyValueMetadata parquet::format::KeyValueMetadata
    total_size += metadata.key_value_metadata.capacity() * sizeof(parquet::format::KeyValue);
    for (const auto & kv : metadata.key_value_metadata)
        total_size += kv.key.capacity() + kv.value.capacity();

    // ColumnOrder parquet::format::ColumnOrders
    total_size += metadata.column_orders.capacity() * sizeof(parquet::format::ColumnOrder);

    // Top-level fields
    total_size += metadata.created_by.capacity();
    total_size += metadata.footer_signing_key_metadata.capacity();

    const auto & encryption = metadata.encryption_algorithm;
    total_size += encryption.AES_GCM_V1.aad_prefix.capacity() + encryption.AES_GCM_V1.aad_file_unique.capacity();
    total_size += encryption.AES_GCM_CTR_V1.aad_prefix.capacity() + encryption.AES_GCM_CTR_V1.aad_file_unique.capacity();
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
void ParquetMetadataCache::addColumnChunks(
    const ParquetMetadataCacheKey & key, const MappedPtr & cell, const std::vector<Parquet::FileColumnChunksPtr> & column_chunks)
{
    std::vector<Parquet::FileColumnChunksPtr> merged = cell->column_chunks;
    merged.resize(std::max(merged.size(), column_chunks.size()));
    bool added = false;
    for (size_t i = 0; i < column_chunks.size(); ++i)
    {
        if (column_chunks[i] && !merged[i])
        {
            merged[i] = column_chunks[i];
            added = true;
        }
    }
    if (!added)
        return;
    Base::replaceIfMatches(key, cell, std::make_shared<ParquetMetadataCacheCell>(cell->footer, std::move(merged)));
}

void ParquetMetadataCache::onEntryRemoval(const size_t weight_loss, const MappedPtr &)
{
    LOG_TRACE(log, "cache eviction");
    ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheWeightLost, weight_loss);
}
}
#endif
