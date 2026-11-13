#include <Processors/Formats/Impl/Parquet/FileStatistics.h>

#if USE_PARQUET

#include <Core/Settings.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/Parquet/Prefetcher.h>
#include <Processors/Formats/Impl/Parquet/Reader.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>
#include <Processors/Formats/Impl/ParquetMetadataCache.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

#include <Poco/String.h>

#include <cmath>
#include <cstring>
#include <map>
#include <vector>

namespace DB
{

namespace Setting
{
    extern const SettingsBool use_parquet_metadata_cache;
}

namespace
{

namespace parq = parquet::format;

/// The value of a Thrift enum read from a file, which may be out of the range of the enum.
template <typename E>
Int32 thriftEnumValue(const E & e)
{
    static_assert(sizeof(E) == sizeof(Int32));
    Int32 value;
    std::memcpy(&value, &e, sizeof(value));
    return value;
}

/// All data pages of the column chunk are dictionary-encoded, as `Reader::columnChunkCanUseDictionaryFilter` checks.
bool isFullyDictionaryEncoded(const parq::ColumnMetaData & meta)
{
    if (!meta.__isset.dictionary_page_offset || !meta.__isset.encoding_stats || meta.data_page_offset <= meta.dictionary_page_offset)
        return false;

    bool has_dictionary_data_page = false;
    for (const auto & stats : meta.encoding_stats)
    {
        if (stats.count < 0)
            return false;
        if (stats.count == 0)
            continue;
        const Int32 page_type = thriftEnumValue(stats.page_type);
        if (page_type != parq::PageType::DATA_PAGE && page_type != parq::PageType::DATA_PAGE_V2)
            continue;
        const Int32 encoding = thriftEnumValue(stats.encoding);
        if (encoding != parq::Encoding::PLAIN_DICTIONARY && encoding != parq::Encoding::RLE_DICTIONARY)
            return false;
        has_dictionary_data_page = true;
    }
    return has_dictionary_data_page;
}

/// The number of values of the dictionary page of a column chunk, from its header; nullopt if it is not a dictionary page.
std::optional<UInt64> readDictionaryPageValues(Parquet::Prefetcher & prefetcher, const parq::ColumnMetaData & meta, size_t file_size)
{
    /// A page header without statistics takes a few dozen bytes.
    constexpr size_t max_header_size = 4096;
    const size_t offset = static_cast<size_t>(meta.dictionary_page_offset);
    const size_t size = std::min({static_cast<size_t>(meta.data_page_offset) - offset, max_header_size, file_size - std::min(offset, file_size)});
    if (size == 0)
        return std::nullopt;

    std::string bytes(size, '\0');
    prefetcher.readSync(bytes.data(), size, offset);

    parq::PageHeader header;
    Parquet::deserializeThriftStruct(header, bytes.data(), size);
    if (thriftEnumValue(header.type) != parq::PageType::DICTIONARY_PAGE || !header.__isset.dictionary_page_header
        || header.dictionary_page_header.num_values < 0)
        return std::nullopt;
    return static_cast<UInt64>(header.dictionary_page_header.num_values);
}

/// The indices of the column chunks of the top-level primitive columns by their names. The schema is a tree flattened
/// in pre-order, and column chunks are its leaves in this order.
struct LeafColumn
{
    size_t index = 0;
    /// A `REQUIRED` column has no NULLs, and writers may omit its NULL count.
    bool required = false;
};

std::unordered_map<String, LeafColumn> getTopLevelLeafColumns(const parq::FileMetaData & metadata, bool case_insensitive)
{
    std::unordered_map<String, LeafColumn> result;
    if (metadata.schema.empty())
        return result;

    /// The numbers of children left to visit of the elements on the path from the root.
    std::vector<Int64> remaining_children{metadata.schema[0].num_children};
    size_t leaf_index = 0;
    for (size_t i = 1; i < metadata.schema.size() && !remaining_children.empty(); ++i)
    {
        const auto & element = metadata.schema[i];
        const bool is_top_level = remaining_children.size() == 1;
        const bool is_leaf = !element.__isset.num_children || element.num_children == 0;
        --remaining_children.back();

        if (is_leaf)
        {
            const Int32 repetition = element.__isset.repetition_type ? thriftEnumValue(element.repetition_type) : -1;
            if (is_top_level && element.__isset.type && repetition != parq::FieldRepetitionType::REPEATED)
                result.emplace(
                    case_insensitive ? Poco::toLower(element.name) : element.name,
                    LeafColumn{.index = leaf_index, .required = repetition == parq::FieldRepetitionType::REQUIRED});
            ++leaf_index;
        }
        else
        {
            remaining_children.push_back(element.num_children);
        }

        while (!remaining_children.empty() && remaining_children.back() <= 0)
            remaining_children.pop_back();
    }
    return result;
}

}

UInt64 estimateDistinctValuesFromSample(UInt64 sample_rows, UInt64 sample_distinct_values, UInt64 max_distinct_values)
{
    max_distinct_values = std::max(max_distinct_values, sample_distinct_values);
    if (sample_distinct_values == 0 || sample_rows == 0)
        return std::max<UInt64>(sample_distinct_values, 1);

    const double n = static_cast<double>(sample_rows);
    const double d = static_cast<double>(sample_distinct_values);
    auto expected_distinct_values = [n](double values) { return values * -std::expm1(-n / values); };

    /// The expected number of distinct values of the sample grows with `D` towards `n`.
    double high = static_cast<double>(max_distinct_values);
    if (expected_distinct_values(high) <= d)
        return max_distinct_values;
    double low = d;
    for (size_t i = 0; i < 100 && high - low > 0.5; ++i)
    {
        const double middle = (low + high) / 2;
        if (expected_distinct_values(middle) < d)
            low = middle;
        else
            high = middle;
    }
    return std::clamp<UInt64>(static_cast<UInt64>(std::llround(high)), sample_distinct_values, max_distinct_values);
}

ParquetFileSummary summarizeParquetFile(
    ReadBufferFromFileBase & buf,
    const RelativePathWithMetadata & object,
    const FormatSettings & format_settings,
    const ContextPtr & context,
    const Names & column_names,
    size_t max_dictionary_pages_to_read)
{
    Parquet::ReadOptions read_options;
    read_options.format = format_settings;
    read_options.format.seekable_read = true;

    Parquet::Prefetcher prefetcher;
    prefetcher.init(&buf, read_options, /*parser_shared_resources_=*/ nullptr);
    auto read_footer = [&] { return Parquet::Reader::readFileFooter(prefetcher, read_options.format.parquet.footer_read_size); };

    /// The same key as the reader's, so that the read reuses the footer.
    ParquetMetadataCachePtr metadata_cache;
    if (context->getSettingsRef()[Setting::use_parquet_metadata_cache])
        metadata_cache = context->tryGetParquetMetadataCache();
    std::shared_ptr<const Parquet::FileFooter> footer;
    if (metadata_cache && object.metadata && object.metadata->getContentCacheToken())
        footer = metadata_cache
                     ->getOrSetMetadata(
                         ParquetMetadataCache::createKey(object.getPath(), object.metadata->getContentCacheToken().value_or(object.metadata->etag)),
                         read_footer)
                     .first->footer;
    else
        footer = read_footer();

    const auto & metadata = footer->metadata;
    ParquetFileSummary result;
    for (const auto & row_group : metadata.row_groups)
        result.rows += static_cast<UInt64>(std::max<Int64>(row_group.num_rows, 0));

    const bool case_insensitive = format_settings.parquet.case_insensitive_column_matching;
    const auto leaves = getTopLevelLeafColumns(metadata, case_insensitive);

    /// The requested columns by their column chunk index, sorted, as `deserializeColumnChunks` needs.
    std::map<size_t, std::pair<String, bool>> columns_by_index;
    for (const auto & column_name : column_names)
        if (auto it = leaves.find(case_insensitive ? Poco::toLower(column_name) : column_name); it != leaves.end())
            columns_by_index.emplace(it->second.index, std::pair{column_name, it->second.required});
    if (columns_by_index.empty())
        return result;

    Parquet::ColumnIndices column_indices;
    for (const auto & [index, _] : columns_by_index)
        column_indices.push_back(index);
    VectorWithMemoryTracking<Parquet::FileColumnChunks> parsed;
    Parquet::deserializeColumnChunks(footer->serialized.data(), footer->serialized.size(), footer->column_chunk_offsets, column_indices, parsed);

    const bool written_by_old_duckdb = metadata.created_by == "DuckDB";
    const size_t file_size = buf.getFileSize();
    size_t k = 0;
    for (const auto & [index, column] : columns_by_index)
    {
        const auto & [column_name, required] = column;
        auto & summary = result.columns[column_name];
        size_t dictionary_pages_read = 0;
        auto & chunks = parsed[k++].chunks;
        for (size_t row_group = 0; row_group < chunks.size() && row_group < metadata.row_groups.size(); ++row_group)
        {
            auto & chunk = chunks[row_group];
            Parquet::Reader::fixUpColumnChunk(chunk, written_by_old_duckdb);
            if (!chunk.__isset.meta_data)
                continue;
            const auto & meta = chunk.meta_data;

            summary.rows += static_cast<UInt64>(std::max<Int64>(metadata.row_groups[row_group].num_rows, 0));
            summary.compressed_bytes += static_cast<UInt64>(std::max<Int64>(meta.total_compressed_size, 0));
            if (!required)
            {
                if (summary.nulls && meta.__isset.statistics && meta.statistics.__isset.null_count && meta.statistics.null_count >= 0)
                    *summary.nulls += static_cast<UInt64>(meta.statistics.null_count);
                else
                    summary.nulls.reset();
            }

            if (!summary.fully_dictionary_encoded)
                continue;
            if (!isFullyDictionaryEncoded(meta))
            {
                summary.fully_dictionary_encoded = false;
                continue;
            }
            if (dictionary_pages_read < max_dictionary_pages_to_read)
            {
                ++dictionary_pages_read;
                /// The row group is taken as a random sample of the rows of the file.
                if (auto values = readDictionaryPageValues(prefetcher, meta, file_size))
                    summary.max_dictionary_values = std::max(
                        summary.max_dictionary_values.value_or(0),
                        estimateDistinctValuesFromSample(
                            static_cast<UInt64>(std::max<Int64>(metadata.row_groups[row_group].num_rows, 0)), *values, result.rows));
            }
        }
    }
    return result;
}

}

#endif
