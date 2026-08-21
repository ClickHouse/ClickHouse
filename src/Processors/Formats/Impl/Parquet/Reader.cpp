#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Formats/Impl/ArrowGeoTypes.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Columns/FilterDescription.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/checkStackSize.h>
#include <Common/HashTable/HashSet.h>
#include <Formats/FormatFilterInfo.h>
#include <Interpreters/castColumn.h>
#include <IO/CompressionMethod.h>
#include <IO/Libdeflate.h>
#include <Processors/Formats/Impl/Parquet/Decoding.h>
#include <Processors/Formats/Impl/Parquet/GeoFilter.h>
#include <Processors/Formats/Impl/Parquet/parquetBloomFilterHash.h>
#include <Processors/Formats/Impl/Parquet/Reader.h>
#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>
#include <Storages/SelectQueryInfo.h>
#include <base/scope_guard.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeSplitPrewhereIntoReadSteps.h>

#include <mutex>
#include <list>
#include <lz4.h>
#include <arrow/util/crc32.h>

#if USE_SNAPPY
#include <snappy.h>
#endif

namespace DB::ErrorCodes
{
    extern const int CANNOT_DECOMPRESS;
    extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace ProfileEvents
{
    extern const Event ParquetRowsFilterExpression;
    extern const Event ParquetColumnsFilterExpression;
    extern const Event ParquetReadPages;
    extern const Event ParquetPrunedPages;
    extern const Event ParquetRowGroupMinMaxPredicateChecks;
    extern const Event ParquetOrderedRowGroupIndexCacheHits;
    extern const Event ParquetOrderedRowGroupIndexCacheMisses;
}

namespace DB::Parquet
{

namespace
{
enum class OrderedRowGroupDirection : UInt8
{
    Unknown,
    Ascending,
    Descending,
};

struct OrderedRowGroupBound
{
    size_t row_group_idx;
    Range range;
};

struct OrderedRowGroupIndex
{
    bool proven = false;
    OrderedRowGroupDirection direction = OrderedRowGroupDirection::Unknown;
    std::vector<OrderedRowGroupBound> bounds;
};

class OrderedRowGroupIndexCache
{
public:
    std::shared_ptr<const OrderedRowGroupIndex> get(const String & key)
    {
        std::lock_guard lock(mutex);
        auto it = entries.find(key);
        if (it == entries.end())
            return {};
        lru.splice(lru.begin(), lru, it->second.lru_position);
        return it->second.index;
    }

    std::shared_ptr<const OrderedRowGroupIndex> set(
        const String & key, std::shared_ptr<const OrderedRowGroupIndex> index)
    {
        std::lock_guard lock(mutex);
        if (auto it = entries.find(key); it != entries.end())
            return it->second.index;

        const size_t weight = 128 + index->bounds.size() * 128;
        if (weight > max_size_bytes)
            return index;
        while (!lru.empty() && size_bytes + weight > max_size_bytes)
        {
            const String & victim = lru.back();
            auto victim_it = entries.find(victim);
            size_bytes -= victim_it->second.weight;
            entries.erase(victim_it);
            lru.pop_back();
        }
        lru.push_front(key);
        entries.emplace(key, Entry{index, weight, lru.begin()});
        size_bytes += weight;
        return index;
    }

private:
    struct Entry
    {
        std::shared_ptr<const OrderedRowGroupIndex> index;
        size_t weight;
        std::list<String>::iterator lru_position;
    };

    static constexpr size_t max_size_bytes = 64 * 1024 * 1024;
    std::mutex mutex;
    std::list<String> lru;
    std::unordered_map<String, Entry> entries;
    size_t size_bytes = 0;
};

OrderedRowGroupIndexCache & getOrderedRowGroupIndexCache()
{
    static OrderedRowGroupIndexCache cache;
    return cache;
}
}

/// Thrift deserialization can store an out-of-range value into an unscoped enum field when the
/// input file is malformed. Loading such an enum directly is undefined behavior (caught by
/// -fsanitize=enum); reading the raw underlying integer via memcpy is well-defined. We use it to
/// validate page-header enums up front, so the rest of the reader only ever loads valid values.
template <typename E>
static int thriftEnumToInt(const E & e)
{
    std::underlying_type_t<E> v;
    memcpy(&v, &e, sizeof(v));
    return static_cast<int>(v);
}

template <typename E>
static bool isValidThriftEnum(const E & e, const std::map<int, const char *> & valid_values)
{
    return valid_values.contains(thriftEnumToInt(e));
}

template <typename E>
static void checkThriftEnum(const E & e, const std::map<int, const char *> & valid_values, const char * what)
{
    if (!isValidThriftEnum(e, valid_values))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid {} in Parquet metadata", what);
}

static void decompressLZ4Raw(const char * data, size_t compressed_size, size_t uncompressed_size, char * out)
{
    if (compressed_size > INT32_MAX || uncompressed_size > INT32_MAX)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Compressed page is too long");
    int n = LZ4_decompress_safe(data, out, int(compressed_size), int(uncompressed_size));
    if (n < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed compressed page");
    if (size_t(n) != uncompressed_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected uncompressed page size");
}

static bool tryDecompressLZ4Hadoop(const char * data, size_t compressed_size, size_t uncompressed_size, char * out)
{
    if (compressed_size > INT32_MAX || uncompressed_size > INT32_MAX)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Compressed page is too long");

    /// From TryDecompressHadoop in arrow/cpp/src/arrow/util/compression_lz4.cc:
    ///  > Parquet files written with the Hadoop Lz4Codec use their own framing.
    ///  > The input buffer can contain an arbitrary number of "frames", each
    ///  > with the following structure:
    ///  > - bytes 0..3: big-endian uint32_t representing the frame decompressed size
    ///  > - bytes 4..7: big-endian uint32_t representing the frame compressed size
    ///  > - bytes 8...: frame compressed data
    while (compressed_size > 0)
    {
        if (compressed_size < 8)
            return false;
        size_t frame_uncompressed_size = unalignedLoadEndian<std::endian::big, UInt32>(data);
        size_t frame_compressed_size = unalignedLoadEndian<std::endian::big, UInt32>(data + 4);
        data += 8;
        compressed_size -= 8;
        if (frame_compressed_size > compressed_size || frame_uncompressed_size > uncompressed_size)
            return false;

        int n = LZ4_decompress_safe(data, out, int(frame_compressed_size), int(frame_uncompressed_size));
        if (n < 0 || size_t(n) != frame_uncompressed_size)
            return false;

        data += frame_compressed_size;
        compressed_size -= frame_compressed_size;
        out += frame_uncompressed_size;
        uncompressed_size -= frame_uncompressed_size;
    }
    return uncompressed_size == 0;
}

static void decompress(const char * data, size_t compressed_size, size_t uncompressed_size, parq::CompressionCodec::type codec, char * out)
{
    CompressionMethod method = CompressionMethod::None;
    switch (codec)
    {
        case parq::CompressionCodec::UNCOMPRESSED:
            chassert(false);
            break;
        case parq::CompressionCodec::SNAPPY:
#if USE_SNAPPY
        {
            /// Can't use CompressionMethod::Snappy because it dispatches to HadoopSnappyReadBuffer,
            /// which expects some additional header before the compressed block.
            size_t actual_uncompressed_size = 0;
            if (!snappy::GetUncompressedLength(data, compressed_size, &actual_uncompressed_size))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Malformed snappy compressed page (couldn't get uncompressed length)");
            if (actual_uncompressed_size != uncompressed_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected uncompressed page size");
            if (!snappy::RawUncompress(data, compressed_size, out))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Malformed snappy compressed page");
            return;
        }
#else
            throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "Cannot decompress Snappy: ClickHouse was compiled without Snappy support");
#endif
        case parq::CompressionCodec::GZIP:
#if USE_LIBDEFLATE
            /// One-shot libdeflate: the whole page is in memory and the uncompressed size is known,
            /// which is faster than the streaming zlib path.
            Libdeflate::decompress(CompressionMethod::Gzip, data, compressed_size, out, uncompressed_size);
            return;
#else
            method = CompressionMethod::Gzip;
            break;
#endif
        case parq::CompressionCodec::LZO:
            /// Arrow also doesn't support it.
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "LZO decompression is not supported");
        case parq::CompressionCodec::BROTLI:
            method = CompressionMethod::Brotli;
            break;
        case parq::CompressionCodec::ZSTD:
            method = CompressionMethod::Zstd;
            break;
        case parq::CompressionCodec::LZ4_RAW:
        {
            /// LZ4 block.
            decompressLZ4Raw(data, compressed_size, uncompressed_size, out);
            return;
        }
        case parq::CompressionCodec::LZ4:
            /// LZ4 with or without hadoop framing - we have to guess.
            /// In parquet this is deprecated in favor of LZ4_RAW.
            if (!tryDecompressLZ4Hadoop(data, compressed_size, uncompressed_size, out))
                decompressLZ4Raw(data, compressed_size, uncompressed_size, out);
            return;
    }
    if (method == CompressionMethod::None)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected compression codec in parquet: {}", thriftToString(codec));

    auto mem_buf = std::make_unique<ReadBuffer>(const_cast<char *>(data), compressed_size, 0);
    std::unique_ptr<ReadBuffer> decompressor = wrapReadBufferWithCompressionMethod(
        std::move(mem_buf),
        method,
        /*zstd_window_log_max*/ 0,
        /// Parquet's `SNAPPY` codec is raw block compression and is special-cased above —
        /// this dispatch never sees it, so the snappy mode here is irrelevant.
        SnappyMode::Basic,
        uncompressed_size,
        out);
    size_t pos = 0;
    while (pos < uncompressed_size)
    {
        decompressor->set(out + pos, uncompressed_size - pos);
        decompressor->next();
        chassert(decompressor->position() == out + pos);
        size_t n = decompressor->available();
        chassert(n <= uncompressed_size - pos);
        pos += n;
    }
}

void Reader::init(
    const ReadOptions & options_, const Block & sample_block_, FormatFilterInfoPtr format_filter_info_,
    std::optional<String> row_group_index_cache_key_)
{
    options = options_;
    sample_block = &sample_block_;
    format_filter_info = format_filter_info_;
    row_group_index_cache_key = std::move(row_group_index_cache_key_);
}

parq::FileMetaData Reader::readFileMetaData(Prefetcher & prefetcher)
{
    /// Parquet file ends with:
    ///  * serialized FileMetaData struct,
    ///  * [4 bytes] size of serialized FileMetaData struct,
    ///  * "PAR1" magic bytes.

    size_t file_size = prefetcher.getFileSize();
    if (file_size <= 8)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet file too short: {} bytes", file_size);

    /// Read the last 64 KiB in hopes that FileMetaData is smaller than that.
    /// This is usually enough for files smaller than a few hundred MB.
    size_t initial_read_size = std::min(file_size, 64ul << 10);
    PODArray<char> buf(initial_read_size);
    prefetcher.readSync(buf.data(), initial_read_size, file_size - initial_read_size);

    if (memcmp(buf.data() + initial_read_size - 4, "PAR1", 4) != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not a Parquet file (wrong magic bytes at the end of file)");

    int32_t metadata_size_i32 = 0;
    memcpy(&metadata_size_i32, buf.data() + initial_read_size - 8, 4);
    if (metadata_size_i32 <= 0 || size_t(metadata_size_i32) + 8 > file_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Bad metadata size in parquet file: {} bytes", metadata_size_i32);

    size_t metadata_size = size_t(metadata_size_i32);
    size_t buf_offset = 0;
    if (metadata_size + 8 > initial_read_size)
    {
        size_t remaining_bytes_to_read = metadata_size + 8 - initial_read_size;
        buf.resize(metadata_size);
        memmove(buf.data() + remaining_bytes_to_read, buf.data(), initial_read_size - 8);
        prefetcher.readSync(buf.data(), remaining_bytes_to_read, file_size - metadata_size - 8);
    }
    else
    {
        buf_offset = initial_read_size - 8 - metadata_size;
    }

    parq::FileMetaData file_metadata = {};
    deserializeThriftStruct(file_metadata, buf.data() + buf_offset, metadata_size);

    /// Some writers incorrectly set dictionary_page_offset offset to 0 when there's no dictionary
    /// page at offset 0 in the file. Work around it.
    for (auto & rg : file_metadata.row_groups)
    {
        for (auto & col : rg.columns)
        {
            if (col.meta_data.__isset.dictionary_page_offset && col.meta_data.dictionary_page_offset == 0)
                col.meta_data.__isset.dictionary_page_offset = false;
        }
    }

    /// Consider two quirks:
    ///  (1) Some versions of spark didn't write dictionary_page_offset even when dictionary page is
    ///      present. Instead, data_page_offset points to the dictionary page.
    ///  (2) Old DuckDB versions (<= 0.10.2) wrote incorrect data_page_offset when dictionary is
    ///      present.
    /// We work around (1) in initializeDataPage by allowing dictionary page in place of data page.
    /// We work around (2) here by converting it to case (1):
    ///   data_page_offset = dictionary_page_offset
    ///   dictionary_page_offset.reset()
    /// Note: newer versions of DuckDB include version number in the `created_by` string, so this
    /// `if` only applies to relatively old versions. Newer versions don't have this bug.
    if (file_metadata.created_by == "DuckDB")
    {
        for (auto & rg : file_metadata.row_groups)
        {
            for (auto & col : rg.columns)
            {
                if (!col.__isset.offset_index_offset && col.meta_data.__isset.dictionary_page_offset)
                {
                    col.meta_data.data_page_offset = col.meta_data.dictionary_page_offset;
                    col.meta_data.__isset.dictionary_page_offset = false;
                    col.meta_data.dictionary_page_offset = 0;
                }
            }
        }
    }

    return file_metadata;
}

void Reader::getHyperrectangleForRowGroup(const parq::RowGroup * meta, Hyperrectangle & hyperrectangle, bool only_spatial_bbox) const
{
    for (const PrimitiveColumnInfo & column_info : primitive_columns)
    {
        if (!column_info.used_by_key_condition)
            continue;
        if (only_spatial_bbox && !column_info.is_spatial_bbox_column)
            continue;
        if (!column_info.decoder.allow_stats)
            continue;
        try
        {
            const auto & column_meta = meta->columns.at(column_info.column_idx).meta_data;
            if (!column_meta.__isset.statistics)
                continue;

            /// The Range must be in terms of the type that checkInHyperrectangle compares it
            /// against, which may differ from decoded_type - see cast_stats_to_output_type.
            const IDataType & output_block_type = *extended_sample_block_data_types.at(column_info.idx_in_output_block);

            Range & range = hyperrectangle[column_info.idx_in_output_block];

            bool nullable = column_info.levels.back().def > 0;
            bool always_null = column_meta.statistics.__isset.null_count &&
                            column_meta.statistics.null_count == column_meta.num_values;
            bool can_be_null = !column_meta.statistics.__isset.null_count ||
                            column_meta.statistics.null_count != 0;
            bool null_as_default = options.format.null_as_default && !column_info.output_nullable;

            if (nullable && always_null)
            {
                /// Single-point range containing either the default value or one of the infinities.
                if (null_as_default)
                    range.right = range.left = output_block_type.getDefault();
                else
                    range.right = range.left;
                continue;
            }

            if (column_meta.statistics.__isset.min_value)
                column_info.decoder.decodeField(column_meta.statistics.min_value, /*is_max=*/ false, *column_info.decoded_type, output_block_type, range.left);
            if (column_meta.statistics.__isset.max_value)
                column_info.decoder.decodeField(column_meta.statistics.max_value, /*is_max=*/ true, *column_info.decoded_type, output_block_type, range.right);

            adjustRangeFromIndexIfNeeded(range, column_info, can_be_null);
        }
        catch (Exception & e)
        {
            /// covering.bbox columns exist only for this optimization (they're never part of the
            /// query's own output or WHERE columns) - malformed helper stats must fail closed
            /// (skip pruning for this row group) rather than aborting the whole read, and
            /// "input_format_parquet_filter_push_down=0" would not even disable this path.
            if (column_info.is_spatial_bbox_column)
                continue;
            e.addMessage("in column chunk statistics for column '{}'; use input_format_parquet_filter_push_down=0 to ignore", column_info.name);
            throw;
        }
    }
}

std::vector<Reader::PointProbe> Reader::findPointProbes() const
{
    std::vector<PointProbe> probes;
    if (!format_filter_info->key_condition)
        return probes;

    std::vector<std::pair<size_t, std::shared_ptr<KeyCondition>>> point_conditions;
    format_filter_info->key_condition->extractSingleColumnConditions(point_conditions, nullptr);
    for (const auto & [key_idx, condition] : point_conditions)
    {
        Ranges ranges;
        if (!condition->extractPlainRanges(ranges))
            ranges = condition->extractBounds();
        if (ranges.size() != 1)
            continue;

        const Range & range = ranges.front();
        if (!range.left_included || !range.right_included
            || !range.fullBounded() || range.left.isNull() || range.right.isNull()
            || !Range::equals(range.left, range.right)
            || key_idx >= sample_block_to_output_columns_idx.size())
            continue;

        const auto & output_idx = sample_block_to_output_columns_idx[key_idx];
        if (!output_idx.has_value())
            continue;
        const OutputColumnInfo & output_info = output_columns[*output_idx];
        if (output_info.is_missing_column || !output_info.is_primitive
            || output_info.primitive_end != output_info.primitive_start + 1)
            continue;

        const size_t primitive_idx = output_info.primitive_start;
        const PrimitiveColumnInfo & primitive = primitive_columns[primitive_idx];
        if (primitive.idx_in_output_block != key_idx || !primitive.decoder.allow_stats)
            continue;

        probes.push_back(PointProbe{
            .key_idx = key_idx,
            .primitive_idx = primitive_idx,
            .point = static_cast<const Field &>(range.left)});
    }
    return probes;
}

bool Reader::getFiniteRowGroupRange(
    const parq::RowGroup & meta, const PrimitiveColumnInfo & column_info, Range & range) const
{
    const auto & column_meta = meta.columns.at(column_info.column_idx).meta_data;
    if (!column_meta.__isset.statistics)
        return false;

    const IDataType & output_block_type = *extended_sample_block_data_types.at(column_info.idx_in_output_block);
    bool nullable = column_info.levels.back().def > 0;
    bool always_null = column_meta.statistics.__isset.null_count
        && column_meta.statistics.null_count == column_meta.num_values;
    bool can_be_null = !column_meta.statistics.__isset.null_count
        || column_meta.statistics.null_count != 0;
    bool null_as_default = options.format.null_as_default && !column_info.output_nullable;

    if (nullable && always_null)
    {
        if (null_as_default)
            range.right = range.left = output_block_type.getDefault();
        else
            range.right = range.left;
    }
    else
    {
        if (!column_meta.statistics.__isset.min_value || !column_meta.statistics.__isset.max_value)
            return false;
        column_info.decoder.decodeField(
            column_meta.statistics.min_value, /*is_max=*/ false,
            *column_info.decoded_type, output_block_type, range.left);
        column_info.decoder.decodeField(
            column_meta.statistics.max_value, /*is_max=*/ true,
            *column_info.decoded_type, output_block_type, range.right);
        adjustRangeFromIndexIfNeeded(range, column_info, can_be_null);
    }

    return range.fullBounded() && !range.left.isNull() && !range.right.isNull();
}

Reader::OrderedRowGroupLookup Reader::findOrderedRowGroupForPoint(const PointProbe & probe) const
{
    const PrimitiveColumnInfo & column_info = primitive_columns[probe.primitive_idx];
    String cache_key;
    std::shared_ptr<const OrderedRowGroupIndex> index;
    if (row_group_index_cache_key)
    {
        cache_key = fmt::format(
            "{}#column={}:{}#decoded={}#output={}#null_default={}#output_nullable={}",
            *row_group_index_cache_key,
            column_info.column_idx,
            column_info.schema_idx,
            column_info.decoded_type->getName(),
            extended_sample_block_data_types.at(column_info.idx_in_output_block)->getName(),
            options.format.null_as_default,
            column_info.output_nullable);
        index = getOrderedRowGroupIndexCache().get(cache_key);
        if (index)
            ProfileEvents::increment(ProfileEvents::ParquetOrderedRowGroupIndexCacheHits);
        else
            ProfileEvents::increment(ProfileEvents::ParquetOrderedRowGroupIndexCacheMisses);
    }

    if (!index)
    {
        auto built = std::make_shared<OrderedRowGroupIndex>();
        built->bounds.reserve(file_metadata.row_groups.size());
        for (size_t row_group_idx = 0; row_group_idx < file_metadata.row_groups.size(); ++row_group_idx)
        {
            const auto & meta = file_metadata.row_groups[row_group_idx];
            if (meta.num_rows < 0 || meta.columns.size() != total_primitive_columns_in_file)
                break;
            if (meta.num_rows == 0)
                continue;

            Range range = Range::createWholeUniverse();
            try
            {
                if (!getFiniteRowGroupRange(meta, column_info, range))
                    break;
            }
            catch (Exception & e)
            {
                e.addMessage(
                    "in column chunk statistics for column '{}'; use input_format_parquet_filter_push_down=0 to ignore",
                    column_info.name);
                throw;
            }

            if (!built->bounds.empty())
            {
                const Range & previous = built->bounds.back().range;
                const bool ascending = Range::less(previous.right, range.left);
                const bool descending = Range::less(range.right, previous.left);
                if (ascending == descending)
                    break;
                const auto pair_direction = ascending
                    ? OrderedRowGroupDirection::Ascending
                    : OrderedRowGroupDirection::Descending;
                if (built->direction == OrderedRowGroupDirection::Unknown)
                    built->direction = pair_direction;
                else if (built->direction != pair_direction)
                    break;
            }
            built->bounds.push_back(OrderedRowGroupBound{row_group_idx, std::move(range)});
        }
        built->proven = built->bounds.size()
            == static_cast<size_t>(std::count_if(file_metadata.row_groups.begin(), file_metadata.row_groups.end(),
                [](const auto & row_group) { return row_group.num_rows != 0; }));
        index = cache_key.empty()
            ? std::shared_ptr<const OrderedRowGroupIndex>(std::move(built))
            : getOrderedRowGroupIndexCache().set(cache_key, std::move(built));
    }

    if (!index->proven)
        return {};

    OrderedRowGroupLookup result{.proven = true, .candidate_row_group = std::nullopt};
    size_t left = 0;
    size_t right = index->bounds.size();
    while (left < right)
    {
        const size_t middle = left + (right - left) / 2;
        const Range & range = index->bounds[middle].range;
        if (index->direction != OrderedRowGroupDirection::Descending)
        {
            if (Range::less(range.right, probe.point))
                left = middle + 1;
            else if (Range::less(probe.point, range.left))
                right = middle;
            else
            {
                result.candidate_row_group = index->bounds[middle].row_group_idx;
                break;
            }
        }
        else
        {
            if (Range::less(range.right, probe.point))
                right = middle;
            else if (Range::less(probe.point, range.left))
                left = middle + 1;
            else
            {
                result.candidate_row_group = index->bounds[middle].row_group_idx;
                break;
            }
        }
    }
    return result;
}

bool Reader::spatialBboxStatsHaveNoNulls(const parq::RowGroup & meta, size_t spatial_key_condition_idx) const
{
    for (size_t bbox_pc_idx : spatial_key_condition_bbox_col_indices.at(spatial_key_condition_idx))
    {
        if (bbox_pc_idx == SIZE_MAX)
            return false;
        const auto & stats = meta.columns.at(primitive_columns[bbox_pc_idx].column_idx).meta_data.statistics;
        if (!stats.__isset.null_count || stats.null_count != 0)
            return false;
    }
    return true;
}

void Reader::prefilterAndInitRowGroups(const std::optional<std::unordered_set<UInt64>> & row_groups_to_read)
{
    extended_sample_block = *sample_block;
    for (const auto & col : format_filter_info->additional_columns)
        extended_sample_block.insert(col);

    /// Parse GeoParquet metadata once. Used by both Phase A (covering.bbox column injection)
    /// and SchemaConverter (geo type resolution). Parsing here avoids a redundant second parse
    /// in the SchemaConverter constructor when both allow_geoparquet_parser and
    /// spatial_filter_push_down are on.
    ///
    /// std::optional distinguishes three states for SchemaConverter:
    ///   nullopt      — not parsed here; SchemaConverter parses if its own setting allows
    ///   Some(empty)  — parsed (or failed); SchemaConverter must not re-parse (avoids rethrow
    ///                  on malformed metadata when the try/catch above already issued a warning)
    ///   Some(map)    — parsed successfully with geo columns; use directly
    std::optional<std::unordered_map<String, DB::GeoColumnMetadata>> geo_meta;
    if (options.format.parquet.allow_geoparquet_parser
        || options.format.parquet.spatial_filter_push_down)
    {
        geo_meta.emplace(); // Mark as "parsed" upfront; filled in on success, left empty on failure.
        for (const auto & kv : file_metadata.key_value_metadata)
        {
            if (kv.key != "geo")
                continue;
            try
            {
                *geo_meta = DB::parseGeoMetadataEncoding(&kv.value);
            }
            catch (...)
            {
                if (options.format.parquet.allow_geoparquet_parser)
                    throw;
                LOG_WARNING(getLogger("ParquetReader"), "Failed to parse GeoParquet metadata, spatial pruning and geo type resolution disabled: {}", getCurrentExceptionMessage(false));
            }
            break;
        }
    }

    /// `geo_meta` is keyed by raw parquet column names (the "geo" metadata is part of the file
    /// itself), but `SpatialFilter::geometry_column_name` lives in a different naming domain when
    /// `format_filter_info->column_mapper` has been swapped for a per-file mapper (data lake schema
    /// evolution, e.g. after an Iceberg `RENAME COLUMN`): it is the ClickHouse name as of the
    /// CURRENT/query-side schema, not the raw name the file's OWN schema used.
    ///
    /// Join `current_schema_column_mapper` (query-side ClickHouse name -> field_id) with the
    /// per-file `column_mapper` (field_id -> the name this file's OWN schema used) via
    /// `ColumnMapper::makeMapping`, which handles arbitrary (including nested) column paths and,
    /// crucially, never touches the Parquet footer's own `field_id` metadata - it works purely from
    /// Iceberg schema metadata, which our writer always has even though it currently omits
    /// per-column `field_id` from the footer.
    ///
    /// Note this is a one-way translation (query-side -> raw). `covering.bbox` sub-column paths
    /// from `geo_meta` are already raw parquet-side names and need no translation: `SchemaConverter`
    /// resolves `primitive_columns[i].name` via the same per-file `column_mapper`
    /// (`useColumnMapperIfNeeded`), which returns each field's name as of the file's OWN schema -
    /// i.e. the very same raw name `geo_meta` already carries. Translating them to the query-side
    /// name (as an earlier version of this code did) breaks the match against
    /// `primitive_columns[i].name` for any bbox sub-column that was itself renamed.
    std::unordered_map<String, String> clickhouse_to_parquet_name;
    const auto * query_side_column_mapper = format_filter_info->current_schema_column_mapper
        ? format_filter_info->current_schema_column_mapper.get()
        : format_filter_info->column_mapper.get();
    if (query_side_column_mapper && format_filter_info->column_mapper)
        clickhouse_to_parquet_name =
            query_side_column_mapper->makeMapping(format_filter_info->column_mapper->getFieldIdToClickHouseName()).first;
    auto resolve_geo_meta = [&](const String & ch_name) -> std::unordered_map<String, DB::GeoColumnMetadata>::const_iterator
    {
        if (auto it = clickhouse_to_parquet_name.find(ch_name); it != clickhouse_to_parquet_name.end())
            return geo_meta->find(it->second);
        return geo_meta->find(ch_name);
    };
    /// `rowGroupFailsSpatialFilters` (the `geospatial_statistics.bbox` fallback) matches
    /// `filter.geometry_column_name` directly against `primitive_columns[i].name`, which - like
    /// the `covering.bbox` sub-columns above - is a raw/file-side name in the per-file
    /// `column_mapper` case. Translate it the same way `resolve_geo_meta` does, or a renamed
    /// geometry column with only `geospatial_statistics.bbox` (no `covering.bbox`) silently loses
    /// pruning.
    auto to_raw_geometry_name = [&](const String & ch_name) -> String
    {
        if (auto it = clickhouse_to_parquet_name.find(ch_name); it != clickhouse_to_parquet_name.end())
            return it->second;
        return ch_name;
    };

    /// Phase A: inject covering.bbox sub-columns into extended_sample_block BEFORE
    /// SchemaConverter runs, so the bbox primitives get proper idx_in_output_block and stats support.
    std::vector<SpatialFilter> all_spatial_filters;
    std::vector<SpatialFilter> geostats_spatial_filters;
    /// Tracks bbox column names that Phase A actually injected (not already present in
    /// extended_sample_block). Used in Phase B to suppress data decoding for those columns:
    /// they exist only for row-group statistics, not for query output or filter evaluation.
    std::unordered_set<String> injected_bbox_columns;
    if (options.format.parquet.spatial_filter_push_down && format_filter_info->filter_actions_dag)
    {
        all_spatial_filters = extractSpatialFilters(*format_filter_info->filter_actions_dag, extended_sample_block);

        /// Collect all leaf column paths from the Parquet schema.
        /// Used below to guard covering.bbox injection: a bbox path from GeoParquet metadata
        /// might not exist in the actual file schema (stale/malformed metadata). Without this
        /// check, SchemaConverter throws THERE_IS_NO_COLUMN for the injected column when
        /// input_format_parquet_allow_missing_columns = 0, turning a readable file into an exception.
        std::unordered_set<String> schema_leaf_paths;
        {
            const auto & schema = file_metadata.schema;
            if (schema.size() >= 2 && schema.at(0).num_children > 0)
            {
                size_t schema_idx = 1;
                std::function<void(const String &)> dfs = [&](const String & parent)
                {
                    if (schema_idx >= schema.size())
                        return;
                    const auto & elem = schema.at(schema_idx++);
                    String path = parent.empty() ? String(elem.name) : parent + "." + elem.name;
                    bool is_primitive = !elem.__isset.num_children || (elem.num_children == 0 && elem.__isset.type);
                    if (is_primitive)
                        schema_leaf_paths.insert(path);
                    else
                        for (int i = 0; i < elem.num_children; ++i)
                            dfs(path);
                };
                for (int i = 0; i < schema.at(0).num_children; ++i)
                    dfs({});
            }
        }

        auto float64 = std::make_shared<DataTypeFloat64>();
        for (const auto & sf : all_spatial_filters)
        {
            auto geo_it = resolve_geo_meta(sf.geometry_column_name);
            if (geo_it == geo_meta->end() || !geo_it->second.covering_bbox.has_value())
            {
                SpatialFilter raw_sf = sf;
                raw_sf.geometry_column_name = to_raw_geometry_name(sf.geometry_column_name);
                geostats_spatial_filters.push_back(std::move(raw_sf));
                continue;
            }

            const auto & bbox_cov = *geo_it->second.covering_bbox;

            const std::array<const String *, 4> raw_bbox_col_ptrs = {
                &bbox_cov.xmin_column, &bbox_cov.ymin_column,
                &bbox_cov.xmax_column, &bbox_cov.ymax_column};

            /// Skip injection if any bbox column is absent from the actual file schema. Checked
            /// against raw parquet-side paths, matching `schema_leaf_paths` (built from the file's
            /// own schema). Falls back to geostats pruning; avoids THERE_IS_NO_COLUMN when
            /// input_format_parquet_allow_missing_columns = 0 with stale metadata.
            bool all_bbox_in_schema = true;
            for (const String * col : raw_bbox_col_ptrs)
                if (!schema_leaf_paths.contains(*col))
                { all_bbox_in_schema = false; break; }
            if (!all_bbox_in_schema)
            {
                SpatialFilter raw_sf = sf;
                raw_sf.geometry_column_name = to_raw_geometry_name(sf.geometry_column_name);
                geostats_spatial_filters.push_back(std::move(raw_sf));
                continue;
            }

            /// Raw parquet-side names, matching what SchemaConverter will produce for these
            /// primitives (see comment above on the per-file column_mapper).
            const std::array<String, 4> bbox_cols = {
                bbox_cov.xmin_column, bbox_cov.ymin_column,
                bbox_cov.xmax_column, bbox_cov.ymax_column};

            /// Skip injection if parent struct column (e.g. "location_bbox") is already in block.
            bool conflict = false;
            for (const String & col : bbox_cols)
            {
                auto dot = col.find('.');
                if (dot != String::npos && extended_sample_block.has(col.substr(0, dot)))
                { conflict = true; break; }
            }
            if (conflict)
            {
                SpatialFilter raw_sf = sf;
                raw_sf.geometry_column_name = to_raw_geometry_name(sf.geometry_column_name);
                geostats_spatial_filters.push_back(std::move(raw_sf));
                continue;
            }

            for (const String & col : bbox_cols)
                if (!extended_sample_block.has(col))
                {
                    extended_sample_block.insert({float64->createColumn(), float64, col});
                    injected_bbox_columns.insert(col);
                }
        }
    }

    extended_sample_block_data_types = extended_sample_block.getDataTypes();
    const auto & row_level_filter = format_filter_info->row_level_filter;
    const auto & prewhere_info = format_filter_info->prewhere_info;

    /// Pass pre-parsed geo_meta to SchemaConverter only when allow_geoparquet_parser is set,
    /// so that spatial_filter_push_down alone does not change column types to Geometry.
    /// geo_meta is not moved so Phase B can still look up covering_bbox entries directly.
    /// Pass std::nullopt when allow_geoparquet_parser is disabled so SchemaConverter skips
    /// geo type resolution entirely (it will not re-parse).
    SchemaConverter schemer(
        file_metadata,
        options,
        &extended_sample_block,
        options.format.parquet.allow_geoparquet_parser ? geo_meta : std::nullopt);
    auto add_prewhere_outputs = [&](const ActionsDAG & actions)
    {
        for (const auto * node : actions.getOutputs())
            if (node->type != ActionsDAG::ActionType::INPUT && sample_block->has(node->result_name))
                schemer.external_columns.push_back(node->result_name);
    };
    if (row_level_filter)
        add_prewhere_outputs(row_level_filter->actions);
    if (prewhere_info)
        add_prewhere_outputs(prewhere_info->prewhere_actions);
    schemer.column_mapper = format_filter_info->column_mapper.get();
    schemer.prepareForReading();
    primitive_columns = std::move(schemer.primitive_columns);
    total_primitive_columns_in_file = schemer.primitive_column_idx;
    output_columns = std::move(schemer.output_columns);

    /// Precalculate some column index mappings.

    sample_block_to_output_columns_idx.resize(extended_sample_block.columns());
    for (size_t i = 0; i < output_columns.size(); ++i)
    {
        const auto & idx = output_columns[i].idx_in_output_block;
        if (idx.has_value())
        {
            chassert(!sample_block_to_output_columns_idx.at(*idx).has_value());
            sample_block_to_output_columns_idx.at(*idx) = i;
        }
    }

    if (format_filter_info->key_condition)
    {
        for (size_t idx_in_output_block : format_filter_info->key_condition->getUsedColumns())
        {
            const auto & output_idx = sample_block_to_output_columns_idx.at(idx_in_output_block);
            /// No file-readable column for this key-condition column: it has no column-chunk
            /// stats, so it cannot prune. Skip it (its range stays the whole universe).
            if (!output_idx.has_value())
                continue;
            const OutputColumnInfo & output_info = output_columns[output_idx.value()];

            if (output_info.is_primitive)
                primitive_columns[output_info.primitive_start].used_by_key_condition = true;
        }
    }

    const auto & rows_to_read = format_filter_info->rows_to_read;
    if (rows_to_read && !std::is_sorted(rows_to_read->begin(), rows_to_read->end()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Rows to read are not sorted");

    /// Phase B: build spatial KeyConditions now that SchemaConverter has set idx_in_output_block
    /// for the injected bbox columns. Also mark those primitives as used_by_key_condition so
    /// getHyperrectangleForRowGroup() reads their min/max stats.
    if (options.format.parquet.spatial_filter_push_down && !all_spatial_filters.empty())
    {
        if (auto ctx = format_filter_info->context.lock())
        {
            for (const auto & sf : all_spatial_filters)
            {
                auto geo_it = resolve_geo_meta(sf.geometry_column_name);
                if (geo_it == geo_meta->end() || !geo_it->second.covering_bbox.has_value())
                    continue; // already in geostats_spatial_filters

                const auto & bbox_cov = *geo_it->second.covering_bbox;
                /// Raw parquet-side bbox paths - matches Phase A, and what extended_sample_block /
                /// primitive_columns[i].name actually use (see comment above on the per-file
                /// column_mapper).
                const std::array<String, 4> bbox_cols = {
                    bbox_cov.xmin_column, bbox_cov.ymin_column,
                    bbox_cov.xmax_column, bbox_cov.ymax_column};
                auto sc = buildBboxKeyCondition(sf,
                    bbox_cols[0], bbox_cols[1],
                    bbox_cols[2], bbox_cols[3],
                    ctx, extended_sample_block);
                if (!sc)
                    continue;
                spatial_key_conditions.push_back(sc);

                /// Mark bbox primitives so getHyperrectangleForRowGroup reads their stats.
                /// Also record their primitive_columns indices for null_count checks at
                /// row-group pruning time (NULL bbox = unknown extent, must not prune).
                std::array<size_t, 4> bbox_pc_indices = {SIZE_MAX, SIZE_MAX, SIZE_MAX, SIZE_MAX};
                for (size_t bi = 0; bi < 4; ++bi)
                    for (size_t ci = 0; ci < primitive_columns.size(); ++ci)
                        if (primitive_columns[ci].name == bbox_cols[bi])
                        {
                            bbox_pc_indices[bi] = ci;
                            break;
                        }
                spatial_key_condition_bbox_col_indices.push_back(bbox_pc_indices);

                for (const String & col : bbox_cols)
                    for (auto & pc : primitive_columns)
                        if (pc.name == col)
                        {
                            pc.used_by_key_condition = true;
                            pc.is_spatial_bbox_column = true;
                            /// Columns that Phase A injected are statistics-only: they are not
                            /// user outputs and not needed for filter evaluation. Suppress data
                            /// decoding by using SIZE_MAX as a sentinel step index that
                            /// ReadManager never matches. Columns already present before Phase A
                            /// (user-selected or used in WHERE/PREWHERE) are not in
                            /// injected_bbox_columns and keep their normal scheduling.
                            if (injected_bbox_columns.contains(col))
                                pc.first_step_to_calculate = SIZE_MAX;
                        }
            }
        }
        else
        {
            /// Context expired: covering.bbox pruning is unavailable (buildBboxKeyCondition needs
            /// it), so everything falls back to the geospatial_statistics.bbox path and needs the
            /// same query-side -> raw-name translation as above.
            for (auto & sf : all_spatial_filters)
                sf.geometry_column_name = to_raw_geometry_name(sf.geometry_column_name);
            geostats_spatial_filters = std::move(all_spatial_filters);
        }
    }

    /// Populate row_groups. Skip row groups based on column chunk min/max statistics.
    OrderedRowGroupLookup ordered_lookup;
    if (options.format.parquet.filter_push_down && format_filter_info->key_condition)
    {
        for (const auto & probe : findPointProbes())
        {
            ordered_lookup = findOrderedRowGroupForPoint(probe);
            if (ordered_lookup.proven)
                break;
        }
    }

    size_t total_rows = 0;
    for (size_t row_group_idx = 0; row_group_idx < file_metadata.row_groups.size(); ++row_group_idx)
    {
        const auto * meta = &file_metadata.row_groups[row_group_idx];
        if (meta->num_rows < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Row group {} has negative row count: {}", row_group_idx, meta->num_rows);
        if (meta->num_rows == 0)
            continue; /// Empty row groups are valid in Parquet; skip them.
        if (meta->columns.size() != total_primitive_columns_in_file)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Row group {} has unexpected number of columns: {} != {}", row_group_idx, meta->columns.size(), total_primitive_columns_in_file);

        total_rows += size_t(meta->num_rows); // before potentially skipping the row group

        if (ordered_lookup.proven && ordered_lookup.candidate_row_group != row_group_idx)
            continue;

        /// Lazy materialization: skip row groups that contain none of the requested rows.
        std::pair<size_t, size_t> requested_rows_slice {0, 0};
        if (rows_to_read)
        {
            size_t group_start_row = total_rows - size_t(meta->num_rows);
            const auto * begin_it = std::lower_bound(rows_to_read->begin(), rows_to_read->end(), group_start_row);
            const auto * end_it = std::lower_bound(begin_it, rows_to_read->end(), total_rows);
            if (begin_it == end_it)
                continue;
            requested_rows_slice = {size_t(begin_it - rows_to_read->begin()), size_t(end_it - rows_to_read->begin())};
        }

        Hyperrectangle hyperrectangle(extended_sample_block.columns(), Range::createWholeUniverse());
        if ((options.format.parquet.filter_push_down && format_filter_info->key_condition)
            || !spatial_key_conditions.empty())
        {
            /// When filter_push_down is disabled, only read bbox column stats to preserve the
            /// escape hatch for malformed non-spatial stats: spatial pruning builds its own
            /// hyperrectangle from the four bbox primitives only.
            bool only_spatial_bbox = !options.format.parquet.filter_push_down || !format_filter_info->key_condition;
            getHyperrectangleForRowGroup(meta, hyperrectangle, only_spatial_bbox);
        }

        if (options.format.parquet.filter_push_down && format_filter_info->key_condition)
        {
            ProfileEvents::increment(ProfileEvents::ParquetRowGroupMinMaxPredicateChecks);
            if (!format_filter_info->key_condition->checkInHyperrectangle(
                    hyperrectangle, extended_sample_block_data_types).can_be_true)
                continue;
        }

        /// Check spatial KeyConditions (covering.bbox column stats via hyperrectangle).
        /// All spatial conditions here come from AND-conjunctive extraction, so if ANY
        /// single condition cannot be satisfied in this row group, the full conjunction
        /// cannot be true — prune the row group.
        /// A spatial condition is skipped when any of its four bbox columns has non-zero or
        /// unknown null_count: NULL bbox means unknown spatial extent and must not be pruned.
        if (!spatial_key_conditions.empty())
        {
            bool prune_by_spatial = false;
            for (size_t sci = 0; sci < spatial_key_conditions.size(); ++sci)
            {
                if (!spatialBboxStatsHaveNoNulls(*meta, sci))
                    continue;
                if (!spatial_key_conditions[sci]->checkInHyperrectangle(hyperrectangle, extended_sample_block_data_types).can_be_true)
                {
                    prune_by_spatial = true;
                    break;
                }
            }
            if (prune_by_spatial)
                continue;
        }

        /// Fallback: geospatial_statistics on the geometry column itself.
        if (!geostats_spatial_filters.empty()
            && rowGroupFailsSpatialFilters(*meta, primitive_columns, geostats_spatial_filters))
            continue;

        RowGroup & row_group = row_groups.emplace_back();
        row_group.meta = meta;
        row_group.need_to_process = !row_groups_to_read.has_value() || row_groups_to_read->contains(row_group_idx);
        row_group.requested_rows_slice = requested_rows_slice;
        row_group.row_group_idx = row_group_idx;
        row_group.start_global_row_idx = total_rows - size_t(meta->num_rows);
        row_group.columns.resize(primitive_columns.size());
        row_group.hyperrectangle = std::move(hyperrectangle);

        for (size_t column_idx = 0; column_idx < primitive_columns.size(); ++column_idx)
        {
            ColumnChunk & column = row_group.columns[column_idx];
            size_t parquet_column_idx = primitive_columns[column_idx].column_idx;
            column.meta = &meta->columns.at(parquet_column_idx);

            /// Whether the innermost array element type is nullable.
            /// E.g. Nullable(String) or Array(Nullable(String)).
            /// Does not apply to nullable arrays, e.g. Nullable(Array(String)), because clickhouse
            /// doesn't support them; we convert null arrays to empty arrays, no null map.
            bool is_nullable = !primitive_columns[column_idx].levels.back().is_array;
            /// If column is declared as nullable, but statistics say there are no nulls, don't
            /// waste time converting definition levels into null map.
            bool null_count_is_known_to_be_zero =
                column.meta->meta_data.statistics.__isset.null_count &&
                column.meta->meta_data.statistics.null_count == 0;
            column.need_null_map = is_nullable && !null_count_is_known_to_be_zero;
        }
    }

    if (rows_to_read && !rows_to_read->empty() && rows_to_read->back() >= total_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Requested to read row {} of a parquet file that has only {} rows", rows_to_read->back(), total_rows);

    if (row_groups.empty())
        return; // all row groups were skipped

    /// prepareBloomFilterCondition computes the query-constant hashes used by both the bloom filter
    /// and the dictionary filter, so run it if either is enabled.
    if ((options.format.parquet.bloom_filter_push_down || options.dictionary_filter_limit_bytes != 0)
        && format_filter_info->key_condition)
        prepareBloomFilterCondition();

    if (options.format.parquet.page_filter_push_down && format_filter_info->key_condition)
    {
        format_filter_info->key_condition->extractSingleColumnConditions(column_conditions, nullptr);
        for (const auto & [idx_in_output_block, key_condition] : column_conditions)
        {
            const auto & output_idx = sample_block_to_output_columns_idx.at(idx_in_output_block);
            /// No file-readable column for this key-condition column: it has no page-index
            /// stats, so it cannot prune. Skip it (page-level pruning is disabled for it).
            if (!output_idx.has_value())
                continue;
            const OutputColumnInfo & output_info = output_columns[output_idx.value()];

            if (!output_info.is_primitive || !primitive_columns[output_info.primitive_start].decoder.allow_stats)
                continue;
            primitive_columns[output_info.primitive_start].column_index_conditions.push_back({key_condition.get(), SIZE_MAX});
        }
    }

    /// Page-level pruning for spatial bbox columns: extract per-column conditions from each
    /// spatial KeyCondition (covering.bbox) and wire them to the bbox primitive columns.
    /// Bbox columns are hidden auxiliaries — they have no output_columns entry, so we match
    /// primitive_columns directly by idx_in_output_block.
    if (options.format.parquet.page_filter_push_down && !spatial_key_conditions.empty())
    {
        for (size_t sci = 0; sci < spatial_key_conditions.size(); ++sci)
        {
            const size_t prev_size = spatial_column_conditions.size();
            spatial_key_conditions[sci]->extractSingleColumnConditions(spatial_column_conditions, nullptr);
            for (size_t i = prev_size; i < spatial_column_conditions.size(); ++i)
            {
                const auto & [idx_in_output_block, key_condition] = spatial_column_conditions[i];
                for (auto & pc : primitive_columns)
                {
                    if (pc.idx_in_output_block != idx_in_output_block)
                        continue;
                    if (!pc.decoder.allow_stats)
                        break;
                    /// Remember which spatial predicate this single-column condition came from:
                    /// it may only prune a page when all four of that predicate's bbox columns
                    /// are known to be null-free (see `applyColumnIndex`).
                    pc.column_index_conditions.push_back({key_condition.get(), sci});
                    break;
                }
            }
        }
    }

    initializePrefetches();
}

/// Glue to convert thrift types to equivalent arrow types because arrow felt the need to
/// duplicate them for some reason. Our parquetTryHashColumn is called from both the
/// arrow-based reader v0 and this reader v3, so arrow types are the common denominator.
/// Warning: this requires that we use the same thrift-generated types as arrow; if we
/// ever switch to thrift-generating our own code from parquet.thrift (e.g. to use a
/// newer version), this will stop working.
static parquet::ColumnDescriptor makeColumnDescriptor(const parq::FileMetaData & file_metadata, const Reader::PrimitiveColumnInfo & column_info)
{
    const parquet::format::SchemaElement * schema_element = &file_metadata.schema.at(column_info.schema_idx);
    auto node = parquet::schema::PrimitiveNode::FromParquet(static_cast<const void *>(schema_element));
    return parquet::ColumnDescriptor(std::move(node), column_info.levels.back().def, column_info.levels.back().rep);
}

void Reader::prepareBloomFilterCondition()
{
    /// Index in output block -> arrow column info.
    std::vector<std::optional<std::pair</*primitive_idx*/ size_t, parquet::ColumnDescriptor>>>
        bf_eligible_columns(extended_sample_block.columns());
    /// Index in output block -> whether at least one surviving row group can use the dictionary page
    /// for this column. Used below to exempt the exact dictionary filter from the bloom filter's
    /// set-size cap (see hash_many).
    std::vector<bool> dict_filter_eligible_columns(extended_sample_block.columns(), false);
    bool any_column_eligible_for_bf = false;
    for (size_t primitive_idx = 0; primitive_idx < primitive_columns.size(); ++primitive_idx)
    {
        const PrimitiveColumnInfo & column_info = primitive_columns[primitive_idx];
        if (!column_info.used_by_key_condition)
            continue;

        /// We hash query constants for any column that has either a bloom filter or a usable
        /// dictionary page in at least one surviving row group, so that the same hashes can later be
        /// looked up in whichever of the two we end up using. The per-row-group decision is made
        /// again in initializePrefetches (via columnChunkCanUseDictionaryFilter and the bloom filter
        /// offset); here we only need to know whether hashing the constants can ever be useful for
        /// this column. Encoding is a per-row-group property - a high-cardinality row group can fall
        /// back to PLAIN (and, unless bloom filters are written, becomes ineligible) while a
        /// low-cardinality one stays dictionary-encoded - so we must scan all row groups rather than
        /// assume the first one is representative, otherwise later row groups silently lose pruning.
        bool any_row_group_eligible = false;
        bool any_row_group_dict_eligible = false;
        for (const RowGroup & row_group : row_groups)
        {
            const parq::ColumnChunk * column_chunk_meta = row_group.columns[primitive_idx].meta;
            bool has_bloom_filter = options.format.parquet.bloom_filter_push_down
                && column_chunk_meta->meta_data.__isset.bloom_filter_offset;
            bool dict_eligible = columnChunkCanUseDictionaryFilter(*column_chunk_meta);
            any_row_group_eligible |= has_bloom_filter || dict_eligible;
            any_row_group_dict_eligible |= dict_eligible;
            /// Dictionary eligibility already implies overall eligibility, so once we have seen it
            /// there is nothing left to learn from the remaining row groups.
            if (any_row_group_dict_eligible)
                break;
        }
        if (!any_row_group_eligible)
            continue;

        parquet::ColumnDescriptor desc = makeColumnDescriptor(file_metadata, column_info);
        bf_eligible_columns[column_info.idx_in_output_block].emplace(primitive_idx, std::move(desc));
        dict_filter_eligible_columns[column_info.idx_in_output_block] = any_row_group_dict_eligible;
        any_column_eligible_for_bf = true;
    }

    if (any_column_eligible_for_bf)
    {
        bool any_column_uses_bf = false;

        auto hash_one = [&](size_t column_idx, const Field & f) -> std::optional<uint64_t>
        {
            const auto & pair = bf_eligible_columns.at(column_idx);
            if (!pair.has_value())
                return std::nullopt;
            const auto & [primitive_idx, descriptor] = *pair;
            auto hash = parquetTryHashField(f, &descriptor);
            if (!hash.has_value())
                return std::nullopt;

            PrimitiveColumnInfo & column_info = primitive_columns[primitive_idx];
            column_info.use_bloom_filter = true;
            column_info.bloom_filter_hashes.push_back(*hash);
            any_column_uses_bf = true;
            return hash;
        };

        auto hash_many = [&](size_t column_idx, const ColumnPtr & column) -> std::optional<std::vector<uint64_t>>
        {
            const auto & pair = bf_eligible_columns.at(column_idx);
            if (!pair.has_value())
                return std::nullopt;
            /// The `bloom_filter_max_set_size` cutoff exists because a large queried set is unlikely to
            /// be ruled out by a probabilistic bloom filter and would make us read many filter blocks
            /// for nothing. The dictionary filter is exact and reads no extra data per value, so that
            /// rationale does not apply: capping here would silently disable dictionary pruning for
            /// `IN` lists with more than `bloom_filter_max_set_size` elements, which is exactly the
            /// workload this feature targets. So for a column that can use nothing but the bloom filter
            /// we keep the cap (skip entirely); for a dictionary-eligible column we still hash the large
            /// set so the dictionary filter can use it, but must not let the bloom filter probe it.
            bool exceeds_bloom_filter_cap = column->size() > options.bloom_filter_max_set_size;
            if (exceeds_bloom_filter_cap && !dict_filter_eligible_columns[column_idx])
                return std::nullopt;
            const auto & [primitive_idx, descriptor] = *pair;
            auto hashes = parquetTryHashColumn(column.get(), &descriptor);
            if (!hashes.has_value())
                return std::nullopt;

            PrimitiveColumnInfo & column_info = primitive_columns[primitive_idx];
            column_info.use_bloom_filter = true;
            if (!exceeds_bloom_filter_cap)
                /// Register the hashes for bloom-filter prefetching. For an over-cap set we skip this:
                /// the hashes are still returned (and reach the exact dictionary filter via the
                /// query-condition RPN, which reads no extra data per value), but the probabilistic
                /// bloom filter must not read a block per value on row groups that fall back to it.
                /// `initializePrefetches` keeps the bloom filter enabled for the column as long as some
                /// other atom registered hashes here, and `BloomFilterLookup::findAnyHash` treats the
                /// unregistered over-cap hashes as possibly present.
                column_info.bloom_filter_hashes.insert(column_info.bloom_filter_hashes.end(), hashes->begin(), hashes->end());
            any_column_uses_bf = true;
            return hashes;
        };

        bloom_filter_condition.emplace(*format_filter_info->key_condition);
        bloom_filter_condition->prepareBloomFilterData(hash_one, hash_many);

        if (!any_column_uses_bf)
            bloom_filter_condition.reset();
    }
}

void Reader::initializePrefetches()
{
    bool use_offset_index = options.format.parquet.use_offset_index || format_filter_info->prewhere_info || format_filter_info->row_level_filter
        || format_filter_info->rows_to_read
        || std::any_of(primitive_columns.begin(), primitive_columns.end(), [](const auto & c) { return !c.column_index_conditions.empty(); });
    bool need_to_find_bloom_filter_lengths_the_hard_way = false;

    for (RowGroup & row_group : row_groups)
    {
        /// Initialize prefetches.
        for (size_t column_idx = 0; column_idx < primitive_columns.size(); ++column_idx)
        {
            ColumnChunk & column = row_group.columns[column_idx];

            /// Dictionary page.
            size_t dict_page_length = 0;
            if (column.meta->meta_data.__isset.dictionary_page_offset)
            {
                /// We assume that the dictionary page is immediately followed by the first data page.
                size_t start = size_t(column.meta->meta_data.dictionary_page_offset);
                dict_page_length = size_t(column.meta->meta_data.data_page_offset) - start;
                column.dictionary_page_prefetch = prefetcher.registerRange(
                    start, dict_page_length, /*likely_to_be_used=*/ true);

                /// Dictionary filter. We only enable it if prepareBloomFilterCondition produced query
                /// hashes for this column (use_bloom_filter), i.e. the condition has an equality/IN on
                /// a hashable type; otherwise the dictionary lookup, which relies on those hashes,
                /// couldn't filter anything anyway. This also guarantees that `bloom_filter_condition`
                /// is non-null whenever any column has use_dictionary_filter set.
                if (primitive_columns[column_idx].use_bloom_filter)
                    column.use_dictionary_filter = columnChunkCanUseDictionaryFilter(*column.meta);
            }

            /// Bloom filter.
            /// `PrimitiveColumnInfo::use_bloom_filter` only means "we hashed the query constants for
            /// this column"; it is also set for dictionary filtering, even when bloom filter push-down
            /// is disabled. So we must re-check the setting here, otherwise a row group that is not
            /// dictionary-filter eligible but has a bloom filter would use it despite the user
            /// disabling `input_format_parquet_bloom_filter_push_down`.
            /// `bloom_filter_hashes` is empty when the only query constants for this column come from
            /// `IN` sets larger than `bloom_filter_max_set_size`, which were hashed only for the exact
            /// dictionary filter; a bloom filter over that many values would read a block per value for
            /// little benefit, so we keep it disabled on row groups (like this one) that fall back to
            /// it. If some smaller atom did register hashes, we still enable it for those - the
            /// unregistered over-cap hashes are handled conservatively in `BloomFilterLookup::findAnyHash`.
            /// We prepare the bloom filter even for a dictionary-filter-eligible column that also carries
            /// one, as a fallback: the exact dictionary path can still decline at runtime when its decoded
            /// page or value set does not fit the pruning memory budget (see `decodeDictionaryPage` and
            /// `hashDictionaryValues`), and without this the row group would then be read in full even
            /// though its bloom filter could have ruled it out - a regression from the pre-existing
            /// bloom-only behavior. `applyBloomAndDictionaryFilters` still prefers the exact dictionary
            /// filter and only falls back to the bloom filter for that case; the only extra cost here is
            /// the small bloom-filter header read, since the filter blocks are prefetched lazily
            /// (`likely_to_be_used=false`) and read only if the fallback is actually taken.
            /// The bloom filter is built only from the chunk's non-null values, so on a chunk that may
            /// contain nulls read into a non-nullable output it cannot be used at all: with
            /// `input_format_null_as_default` disabled, pruning would suppress the
            /// `CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN` exception the read must raise; with it enabled,
            /// nulls decode to the type's default value, which the filter does not contain, so a row
            /// group whose only matches come from nulls would be wrongly skipped. The exact dictionary
            /// filter models both cases (see `hashDictionaryValues`), the probabilistic bloom filter
            /// can't, so keep it disabled for such chunks - both standalone and as the dictionary
            /// filter's fallback.
            bool nullable = primitive_columns[column_idx].levels.back().def > 0;
            bool can_be_null = !column.meta->meta_data.statistics.__isset.null_count
                || column.meta->meta_data.statistics.null_count != 0;
            bool bloom_filter_null_safe = !nullable || !can_be_null || primitive_columns[column_idx].output_nullable;

            if (options.format.parquet.bloom_filter_push_down &&
                primitive_columns[column_idx].use_bloom_filter &&
                !primitive_columns[column_idx].bloom_filter_hashes.empty() &&
                bloom_filter_null_safe &&
                column.meta->meta_data.__isset.bloom_filter_offset)
            {
                /// Have to guess the header size upper bound.
                size_t max_header_length = 256;
                if (!column.meta->meta_data.__isset.bloom_filter_length)
                    need_to_find_bloom_filter_lengths_the_hard_way = true;
                else
                {
                    size_t len = size_t(column.meta->meta_data.bloom_filter_length);
                    max_header_length = std::min(max_header_length, len);
                    column.bloom_filter_data_bytes = len;
                    column.bloom_filter_data_prefetch = prefetcher.registerRange(
                        size_t(column.meta->meta_data.bloom_filter_offset),
                        len, /*likely_to_be_used=*/ false);
                }
                /// bloom_filter_header_prefetch and bloom_filter_data_prefetch overlap, that's ok.
                column.use_bloom_filter = true;
                column.bloom_filter_header_prefetch = prefetcher.registerRange(
                    size_t(column.meta->meta_data.bloom_filter_offset),
                    max_header_length, /*likely_to_be_used=*/ true);
            }

            /// Offset index.
            if (use_offset_index &&
                column.meta->__isset.offset_index_offset && column.meta->__isset.offset_index_length)
            {
                column.offset_index_prefetch = prefetcher.registerRange(
                    size_t(column.meta->offset_index_offset),
                    size_t(column.meta->offset_index_length), /*likely_to_be_used*/ true);
            }

            /// Column index.
            column.use_column_index = !primitive_columns[column_idx].column_index_conditions.empty()
                && column.offset_index_prefetch
                && column.meta->__isset.column_index_offset && column.meta->__isset.column_index_length;
            if (column.use_column_index)
                column.column_index_prefetch = prefetcher.registerRange(
                    size_t(column.meta->column_index_offset),
                    size_t(column.meta->column_index_length), /*likely_to_be_used=*/ true);

            /// Data pages.

            column.data_pages_bytes = size_t(column.meta->meta_data.total_compressed_size) - dict_page_length;

            /// Old versions of parquet-mr wrote incorrect total_compressed_size, see PARQUET-816.
            /// Work around it with the same hack as in apache impala: add 100 bytes to the length.
            /// But leave `data_pages_bytes` unchanged because it's used to check whether there are any
            /// more pages to read, and we don't want to start reading a page inside these 100 bytes.
            size_t data_pages_extra_bytes = 0;
            if (file_metadata.created_by == "parquet-mr" && !column.meta->meta_data.__isset.dictionary_page_offset && !column.meta->__isset.offset_index_offset)
                data_pages_extra_bytes = std::min(100ul, prefetcher.getFileSize() - size_t(column.meta->meta_data.data_page_offset) - column.data_pages_bytes);

            column.data_pages_prefetch = prefetcher.registerRange(
                size_t(column.meta->meta_data.data_page_offset),
                column.data_pages_bytes + data_pages_extra_bytes,
                /*likely_to_be_used=*/ true);
        }
    }

    if (need_to_find_bloom_filter_lengths_the_hard_way)
    {
        /// Parquet metadata is missing information about bloom filter sizes, but we want to know
        /// them (at least an upper bound) in advance, so that Prefetcher can coalesce it with other
        /// reads if it's small.
        /// Bloom filter ends when something else starts (or earlier). So we list all possible
        /// "something else" offsets and do binary search for each bloom filter to find where it ends.
        std::vector<size_t> all_offsets;
        all_offsets.reserve(file_metadata.row_groups.size() * file_metadata.schema.size() * 6);
        for (const auto & rg : file_metadata.row_groups)
        {
            for (const auto & col : rg.columns)
            {
                all_offsets.push_back(col.file_offset);
                if (col.__isset.offset_index_offset)
                    all_offsets.push_back(col.offset_index_offset);
                if (col.__isset.column_index_offset)
                    all_offsets.push_back(col.column_index_offset);
                if (col.meta_data.__isset.dictionary_page_offset)
                    all_offsets.push_back(col.meta_data.dictionary_page_offset);
                all_offsets.push_back(col.meta_data.data_page_offset);
                if (col.meta_data.__isset.bloom_filter_offset)
                    all_offsets.push_back(col.meta_data.bloom_filter_offset);
            }
        }
        std::sort(all_offsets.begin(), all_offsets.end());
        for (RowGroup & row_group : row_groups)
        {
            for (ColumnChunk & column : row_group.columns)
            {
                if (!column.use_bloom_filter)
                    continue;
                chassert(column.meta->meta_data.__isset.bloom_filter_offset);
                size_t offset = size_t(column.meta->meta_data.bloom_filter_offset);
                auto it = std::upper_bound(all_offsets.begin(), all_offsets.end(), offset);
                size_t end = it == all_offsets.end() ? prefetcher.getFileSize() : *it;

                column.bloom_filter_data_bytes = end - offset;
                column.bloom_filter_data_prefetch = prefetcher.registerRange(
                    offset, end - offset, /*likely_to_be_used=*/ false);
            }
        }
    }

    prefetcher.finalizeRanges();
}

void Reader::preparePrewhere()
{
    const auto & row_level_filter = format_filter_info->row_level_filter;
    const auto & prewhere_info = format_filter_info->prewhere_info;
    std::unordered_set<size_t> prewhere_output_column_idxs;

    /// TODO [parquet]: We currently run prewhere after reading all prewhere columns of the row
    ///     subgroup, in one thread per row group. Instead, we could extract single-column conditions
    ///     and run them after decoding the corresponding columns, in parallel.
    ///     (Still run multi-column conditions, like `col1 = 42 or col2 = 'yes'`, after reading all columns.)
    ///     Probably reuse tryBuildPrewhereSteps from MergeTree for splitting the expression.

    /// Convert ActionsDAG to ExpressionActions.
    std::optional<ExpressionActionsSettings> actions_settings;

    auto add_single_step = [&] (const ActionsDAG & dag, const String & filter_column_name, bool needs_filter, size_t step_idx)
    {
        if (!actions_settings.has_value())
            actions_settings.emplace();
        Step step { .actions = ExpressionActions(dag.clone(), actions_settings.value()) };
        if (needs_filter)
            step.filter_column_name = filter_column_name;

        /// Find inputs in extended sample block.
        for (const auto & col : step.actions.getRequiredColumnsWithTypes())
        {
            size_t idx_in_output_block = extended_sample_block.getPositionByName(col.name, /* case_insensitive= */ false);
            const auto & output_idx = sample_block_to_output_columns_idx.at(idx_in_output_block);
            if (output_idx.has_value())
            {
                OutputColumnInfo & output_info = output_columns[output_idx.value()];
                output_info.step_idx = step_idx + 1;
                bool only_for_prewhere = idx_in_output_block >= sample_block->columns();

                for (size_t primitive_idx = output_info.primitive_start; primitive_idx < output_info.primitive_end; ++primitive_idx)
                {
                    if (primitive_columns[primitive_idx].first_step_to_calculate == 0)
                        primitive_columns[primitive_idx].first_step_to_calculate = steps.size() + 1;
                    primitive_columns[primitive_idx].only_for_prewhere = only_for_prewhere;
                }
            }
            else
            {
                if (!prewhere_output_column_idxs.contains(idx_in_output_block))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "PREWHERE appears to use its own output as input: column '{}' (idx {})",
                        col.name, idx_in_output_block);
            }
            step.input_idxs.push_back(idx_in_output_block);
        }

        /// Find outputs in sample block.
        for (const auto * node : dag.getOutputs())
        {
            auto idx = extended_sample_block.findPositionByName(node->result_name);
            /// Note: prewhere output may also be an input, if it's just passed through.
            if (idx.has_value() && !sample_block_to_output_columns_idx.at(*idx).has_value() && !prewhere_output_column_idxs.contains(*idx))
            {
                step.idxs_in_output_block.emplace_back(node->result_name, *idx);
                prewhere_output_column_idxs.insert(*idx);
            }
        }

        steps.push_back(std::move(step));
    };

    auto add_step = [&](const ActionsDAG & dag, const String & filter_column_name, bool needs_filter)
    {
        if (!actions_settings.has_value())
            actions_settings.emplace();

        PrewhereExprInfo prewhere_expr_info;
        bool success = false;

        /// The per-condition split only registers kept prewhere outputs while filtering, so it is
        /// used only when needs_filter is true; otherwise fall through to the single step below.
        if (needs_filter)
        {
            auto prewhere_info_patched = std::make_shared<PrewhereInfo>(dag.clone(), filter_column_name);
            prewhere_info_patched->need_filter = needs_filter;

            success = tryBuildPrewhereSteps(
                prewhere_info_patched,
                *actions_settings,
                prewhere_expr_info,
                /*force_short_circuit_execution*/ false);

            /// A cross-step column is addressable only if it is an original prewhere input or an
            /// intermediate an earlier step wrote to a dedicated prewhere-output slot. Otherwise the
            /// split step is unaddressable (or would resolve to a physical column that only shares the
            /// generated name), so fall back to a single step.
            NameSet addressable_columns;
            for (const auto & col : dag.getRequiredColumns())
                addressable_columns.insert(col.name);

            for (const auto & step : prewhere_expr_info.steps)
            {
                if (!success)
                    break;
                for (const auto & col : step->actions->getActionsDAG().getRequiredColumns())
                {
                    if (!addressable_columns.contains(col.name))
                    {
                        success = false;
                        break;
                    }
                }
                if (!success)
                    break;

                /// An intermediate this step computes can be read by a later step only through a
                /// dedicated prewhere-output slot: one present in `extended_sample_block` whose slot is
                /// not an original output column. This mirrors how `add_single_step` registers prewhere
                /// outputs, and excludes generated names that only collide with physical columns.
                for (const auto * node : step->actions->getActionsDAG().getOutputs())
                {
                    auto idx = extended_sample_block.findPositionByName(node->result_name);
                    if (idx.has_value() && !sample_block_to_output_columns_idx.at(*idx).has_value())
                        addressable_columns.insert(node->result_name);
                }
            }
        }

        if (success)
        {
            /// Add all steps separately.
            for (size_t i = 0; i < prewhere_expr_info.steps.size(); ++i)
            {
                auto filter = prewhere_expr_info.steps[i];
                add_single_step(filter->actions->getActionsDAG(), filter->filter_column_name, true, i);
            }
        }
        else
        {
            /// Execute everything as one large step
            add_single_step(dag, filter_column_name, needs_filter, 0);
        }
    };

    if (row_level_filter)
        add_step(row_level_filter->actions, row_level_filter->column_name, true);
    if (prewhere_info)
        add_step(prewhere_info->prewhere_actions, prewhere_info->prewhere_column_name, prewhere_info->need_filter);

    /// Assert that we found all columns of the sample block, either in the file or in prewhere outputs.
    for (size_t i = 0; i < sample_block_to_output_columns_idx.size(); ++i)
    {
        /// Column must appear in exactly one of {output_columns, prewhere output}.
        if (sample_block_to_output_columns_idx[i].has_value() == prewhere_output_column_idxs.contains(i))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column in sample block: {}", extended_sample_block.getByPosition(i).name);
    }

    /// A primitive whose output slot is past `sample_block` is discarded by `applyPrewhere` after the
    /// last step, and no step above claimed this one, so nothing can read it. Never schedule it:
    /// the main step would decode into a slot that is already gone.
    for (auto & pc : primitive_columns)
        if (pc.first_step_to_calculate == 0
            && pc.idx_in_output_block < extended_sample_block.columns()
            && pc.idx_in_output_block >= sample_block->columns())
            pc.first_step_to_calculate = SIZE_MAX;
}

void Reader::processBloomFilterHeader(ColumnChunk & column, const PrimitiveColumnInfo & column_info)
{
    auto data = prefetcher.getRangeData(column.bloom_filter_header_prefetch);
    size_t header_size = deserializeThriftStruct(column.bloom_filter_header, data.data(), data.size());

    if (!column.bloom_filter_header.algorithm.__isset.BLOCK ||
        !column.bloom_filter_header.hash.__isset.XXHASH ||
        !column.bloom_filter_header.compression.__isset.UNCOMPRESSED)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported bloom filter format. Use setting input_format_parquet_bloom_filter_push_down=0 to ignore.");

    /// Parquet bloom filter is sharded into 32-byte blocks using the upper half of the hash bits.
    /// Here we take the set of hashes we're looking for and map it to the set of blocks to read.

    const size_t bytes_per_block = 32;
    if (column.bloom_filter_header.numBytes <= 0 || column.bloom_filter_header.numBytes % bytes_per_block != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid bloom filter size.");
    /// The bitset must fit in the bloom filter byte range the file declared, otherwise the block
    /// subranges below would point outside the data we fetched.
    if (header_size > column.bloom_filter_data_bytes ||
        size_t(column.bloom_filter_header.numBytes) > column.bloom_filter_data_bytes - header_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Bloom filter bitset of {} bytes doesn't fit in {} bytes of bloom filter "
            "data (including a {}-byte header) at offset {}. Use setting input_format_parquet_bloom_filter_push_down=0 to ignore.",
            column.bloom_filter_header.numBytes, column.bloom_filter_data_bytes, header_size, column.meta->meta_data.bloom_filter_offset);
    size_t num_blocks = size_t(column.bloom_filter_header.numBytes) / bytes_per_block;

    const auto & hashes = column_info.bloom_filter_hashes;
    std::vector<size_t> block_idxs;
    block_idxs.reserve(hashes.size());
    for (UInt64 h : column_info.bloom_filter_hashes)
    {
        /// Calculate block index as described in
        /// https://parquet.apache.org/docs/file-format/bloomfilter/
        size_t block_idx = ((h >> 32) * num_blocks) >> 32;
        block_idxs.push_back(block_idx);
    }

    std::sort(block_idxs.begin(), block_idxs.end());
    block_idxs.erase(std::unique(block_idxs.begin(), block_idxs.end()), block_idxs.end());

    std::vector<std::pair</*global_offset*/ size_t, /*length*/ size_t>> subranges;
    subranges.reserve(block_idxs.size());
    size_t base_offset = column.meta->meta_data.bloom_filter_offset + header_size;
    for (size_t block_idx : block_idxs)
        subranges.emplace_back(base_offset + block_idx * bytes_per_block, bytes_per_block);

    std::vector<PrefetchHandle> prefetches;
    if (!subranges.empty()) // can be empty e.g. if `WHERE x IN ()`
        prefetches = prefetcher.splitRange(std::move(column.bloom_filter_data_prefetch), subranges, /*likely_to_be_used*/ false);

    column.bloom_filter_blocks.reserve(block_idxs.size());
    for (size_t i = 0; i < block_idxs.size(); ++i)
    {
        BloomFilterBlock & block = column.bloom_filter_blocks.emplace_back();
        block.block_idx = block_idxs[i];
        block.prefetch = std::move(prefetches[i]);
    }
}

bool Reader::decodeDictionaryPage(
    ColumnChunk & column, const PrimitiveColumnInfo & column_info,
    const PruningMemoryReservation & reservation, size_t * held_reserved_bytes)
{
    if (held_reserved_bytes)
        *held_reserved_bytes = 0;

    auto data = prefetcher.getRangeData(column.dictionary_page_prefetch);
    const char * data_ptr = data.data();
    const char * data_end = data.data() + data.size();
    auto [header, page_data] = decodeAndCheckPageHeader(data_ptr, data_end);

    if (header.type != parq::PageType::DICTIONARY_PAGE)
    {
        if (column.meta->meta_data.__isset.dictionary_page_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected dictionary page type: {}", thriftToString(header.type));

        /// Parquet metadata didn't specifically say that this byte range is a dictionary page.
        return false;
    }

    /// Dictionary-filter pruning path: bound the decoded dictionary size *before* it is decoded and
    /// used, and reserve it live so several row groups pruning in parallel cannot collectively overshoot
    /// the watermark. `columnChunkCanUseDictionaryFilter` only limits the compressed on-disk dictionary
    /// page (`dictionary_filter_limit_bytes`, 1 MiB by default); a highly compressible dictionary can
    /// still decompress to many times that. On the pruning path (`BloomFilterBlocksOrDictionary` stage)
    /// `reservation` is a live handle on the shared stage budget - the reader's memory high watermark
    /// minus what the stage already holds (the decoded dictionaries and value sets other row groups are
    /// holding right now, plus this batch's not-yet-flushed pruning memory; see
    /// `ReadManager::pruningMemoryReservation` and `PruningMemoryReservation`). We reserve the decoded
    /// footprint against it before decoding, so a dictionary that would push the pruning stage past the
    /// watermark is rejected before `Dictionary::decode` allocates anything and the caller falls back to
    /// a full scan (a missed optimization, never a wrong result). The complementary cap on the decoded
    /// value set built from the dictionary lives in `hashDictionaryValues`. A default (unbounded)
    /// reservation means the data-read path, where the dictionary is decoded lazily and throttled by the
    /// normal column-data memory accounting.
    bool bounded = reservation.stage_memory != nullptr && reservation.watermark != 0;
    size_t reserved_bytes = 0;
    if (bounded)
    {
        /// Worst-case pre-decode gate. `Dictionary::decode` allocates per-entry state *on top of* the
        /// decompressed page bytes - a `StringPlain` offsets array, or a fully decoded `col` for types
        /// that need conversion - so the true footprint can be several times the page payload (e.g. a
        /// bit-packed dictionary decoded into a wider column). `Dictionary::decodedFootprintUpperBound`
        /// predicts that footprint from the page header before anything is allocated, so an oversized
        /// dictionary is rejected *before* `decode` transiently materializes it, and the pruning path
        /// never overshoots the budget even momentarily. If it does not fit, skip it and let the caller
        /// fall back to a full scan; the dictionary is re-decoded later, unbounded and throttled, on the
        /// data-read path if the column is actually read.
        if (header.compressed_page_size < 0 || header.uncompressed_page_size < 0
            || header.dictionary_page_header.num_values < 0)
            return false; /// Malformed header sizes; the data-read path (unbounded) surfaces the error.
        /// The size of the payload `Dictionary::decode` will see, which is what the bound is about:
        /// `decodeDictionaryPageImpl` decompresses a compressed chunk into a `decompressed_buf` of
        /// exactly `uncompressed_page_size` bytes, while for an `UNCOMPRESSED` chunk it points the
        /// dictionary straight at the first `compressed_page_size` prefetched bytes. Never the larger
        /// of the two: the compressed frame is held by `dictionary_page_prefetch` and is already
        /// charged to the pruning stage, so charging it here as well would double-count it and make a
        /// row group whose decoded dictionary fits the budget fall back to a full scan. A codec is
        /// free to expand an incompressible page (`compressed_page_size > uncompressed_page_size`),
        /// and our own writer does not fall back to `UNCOMPRESSED` when it does.
        size_t page_bytes = column.meta->meta_data.codec == parq::CompressionCodec::UNCOMPRESSED
            ? size_t(header.compressed_page_size)
            : size_t(header.uncompressed_page_size);
        reserved_bytes = Dictionary::decodedFootprintUpperBound(
            column.meta->meta_data.codec, header.dictionary_page_header.encoding, column_info.decoder,
            size_t(header.dictionary_page_header.num_values), page_bytes, *column_info.decoded_type);
        if (!reservation.tryReserve(reserved_bytes))
            return false;
    }

    /// Release the reservation on every early-out below; only the successful path keeps the actual
    /// decoded footprint reserved (reduced from the predicted upper bound) and hands it to the caller
    /// via `*held_reserved_bytes`, to be released in `ReadManager::clearColumnChunk`.
    bool committed = false;
    SCOPE_EXIT({ if (bounded && !committed) reservation.release(reserved_bytes); });

    try
    {
        decodeDictionaryPageImpl(header, page_data, column, column_info);
    }
    catch (...)
    {
        /// `decodeDictionaryPageImpl` can throw after it has already allocated into
        /// `column.dictionary` (the decompression buffer, offsets, a partially decoded `col`).
        /// Free those buffers while the reservation is still held, so the shared pruning budget
        /// never undercounts live memory: the SCOPE_EXIT above releases `reserved_bytes` during
        /// unwinding, and other pruning tasks may keep running and reserving until the exception
        /// is published after the batch unwinds (see `ReadManager::runBatchOfTasks`).
        column.dictionary.reset();
        throw;
    }

    if (bounded)
    {
        /// `decodedFootprintUpperBound` is a true upper bound on the memory actually held
        /// (`Dictionary::allocatedBytes`), because it accounts for the `PODArray` capacity rounding and
        /// padding on top of the logical sizes (see the helper). Reconcile the live reservation down to
        /// the real footprint so the amount charged to the shared budget matches what was really
        /// allocated: normally the dictionary is smaller than predicted and we release the difference.
        /// The grow branch is a defensive backstop in case the prediction ever drifts below the real
        /// footprint: reserve the extra and fall back to a full scan if it no longer fits the budget.
        size_t actual_bytes = column.dictionary.allocatedBytes();
        if (actual_bytes > reserved_bytes)
        {
            if (!reservation.tryReserve(actual_bytes - reserved_bytes))
            {
                /// Does not fit the remaining budget: drop the dictionary and let the caller fall back to
                /// a full scan. The `reserved_bytes` reserved before decoding are freed by the SCOPE_EXIT.
                column.dictionary.reset();
                return false;
            }
        }
        else
        {
            reservation.release(reserved_bytes - actual_bytes);
        }
        /// Now holding exactly `actual_bytes`; hand it to the caller to release in `clearColumnChunk`.
        reserved_bytes = actual_bytes;
        if (held_reserved_bytes)
            *held_reserved_bytes = actual_bytes;
    }

    committed = true;
    return true;
}

void Reader::decodeDictionaryPageImpl(const parq::PageHeader & header, std::span<const char> data, ColumnChunk & column, const PrimitiveColumnInfo & column_info)
{
    chassert(header.type == parq::PageType::DICTIONARY_PAGE);

    size_t compressed_page_size = size_t(header.compressed_page_size);
    if (header.compressed_page_size < 0 || compressed_page_size > data.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Dictionary page size out of bounds: {} > {}", header.compressed_page_size, data.size());
    data = data.subspan(0, size_t(header.compressed_page_size));

    checkThriftEnum(column.meta->meta_data.codec, parq::_CompressionCodec_VALUES_TO_NAMES, "compression codec");
    auto codec = column.meta->meta_data.codec;
    if (codec != parq::CompressionCodec::UNCOMPRESSED)
    {
        if (header.uncompressed_page_size < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Negative uncompressed dictionary page size");
        size_t uncompressed_size = size_t(header.uncompressed_page_size);
        auto & buf = column.dictionary.decompressed_buf;
        buf.resize(uncompressed_size);
        decompress(data.data(), data.size(), buf.size(), codec, buf.data());
        data = std::span(buf.data(), buf.size());
    }

    /// Signed i32 from the thrift header; a negative count would sign-extend to a huge size_t and
    /// drive a huge `reserve`/`resize` inside Dictionary::decode.
    if (header.dictionary_page_header.num_values < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Negative number of values in dictionary page");
    column.dictionary.decode(header.dictionary_page_header.encoding, column_info.decoder, size_t(header.dictionary_page_header.num_values), data, *column_info.decoded_type);
}

bool Reader::BloomFilterLookup::findAnyHash(const std::vector<uint64_t> & hashes)
{
    size_t num_blocks = size_t(column.bloom_filter_header.numBytes) / 32;
    for (size_t h : hashes)
    {
        size_t block_idx = ((h >> 32) * num_blocks) >> 32;
        auto it = std::partition_point(column.bloom_filter_blocks.begin(), column.bloom_filter_blocks.end(), [&](const BloomFilterBlock & block) { return block.block_idx < block_idx; });
        /// This value's block was not prefetched. That happens for values from an `IN` set larger than
        /// `bloom_filter_max_set_size`: such sets are hashed only for the exact dictionary filter and
        /// deliberately kept out of `bloom_filter_hashes` (see prepareBloomFilterCondition), so probing
        /// them here would read one filter block per value for little benefit. A bloom filter can only
        /// ever rule a value out, so a value we did not probe must be treated as possibly present.
        if (it == column.bloom_filter_blocks.end() || it->block_idx != block_idx)
            return true;

        auto data = prefetcher.getRangeData(it->prefetch);

        /// https://parquet.apache.org/docs/file-format/bloomfilter/
        static constexpr UInt32 salt[8] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU, 0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};
        bool miss = false;
        for (size_t i = 0; i < 8; ++i)
        {
            size_t bit_idx = UInt32(UInt32(h) * salt[i]) >> 27;
            UInt32 word = unalignedLoad<UInt32>(data.data() + i * 4);
            if (!(word & (1u << bit_idx)))
            {
                miss = true;
                break;
            }
        }
        if (!miss)
            return true;
    }
    return false;
}

bool Reader::columnChunkCanUseDictionaryFilter(const parq::ColumnChunk & column_meta) const
{
    if (options.dictionary_filter_limit_bytes == 0)
        return false;
    /// We deliberately require a declared `dictionary_page_offset`. Some legacy writers omit it and
    /// point `data_page_offset` at the dictionary page instead (the "undeclared dictionary page"
    /// shape handled in `initializeDataPage`); dictionary filtering stays disabled for those files.
    /// Determining the dictionary page's byte range in that shape needs the offset index (its page
    /// locations), which is not available at this pruning stage, and such files also typically lack
    /// the `encoding_stats` that this check requires below. The cost of the limitation is only a
    /// missed optimization (a full row-group scan), never a wrong result.
    if (!column_meta.meta_data.__isset.dictionary_page_offset)
        return false;
    /// We assume that the dictionary page is immediately followed by the first data page.
    size_t dict_page_length = size_t(column_meta.meta_data.data_page_offset) - size_t(column_meta.meta_data.dictionary_page_offset);
    /// The limit is the maximum dictionary page size for which pruning applies, so the boundary is
    /// inclusive: a dictionary page of exactly `dictionary_filter_limit_bytes` is still eligible.
    if (dict_page_length > options.dictionary_filter_limit_bytes)
        return false;
    /// We can only use the dictionary if it holds the complete set of column values, i.e. all data
    /// pages are dictionary-encoded. Without encoding stats we can't tell, so we don't risk it.
    if (!column_meta.meta_data.__isset.encoding_stats)
        return false;
    /// Require positive proof, not just the absence of a contradiction: there must be at least one
    /// non-empty data page and every non-empty data page must use a dictionary encoding. A present
    /// but incomplete list (empty, or describing only the dictionary page and no data pages) does
    /// not prove anything - the column chunk could still contain plain data pages whose values are
    /// not in the dictionary, and pruning from such an incomplete value set would silently drop
    /// matching rows. Empty data pages (count == 0) carry no values, so their encoding is irrelevant.
    bool has_dictionary_data_page = false;
    for (const parq::PageEncodingStats & s : column_meta.meta_data.encoding_stats)
    {
        /// An empty entry (`count == 0`) describes no pages, so nothing about it - neither its
        /// `page_type` nor its `encoding` - is relevant to eligibility. Skip it before validating those
        /// Thrift enums, otherwise a garbage enum value on an advisory empty entry would turn a file
        /// that reads fine (with a full scan) into a hard `INCORRECT_DATA` failure under the default-on
        /// dictionary filter. A negative count is corrupted metadata, but `encoding_stats` is only
        /// advisory input to this eligibility check, so it must not fail the whole read either: like
        /// the unrecognized enum values below, it just makes the chunk ineligible for the optimization.
        if (s.count < 0)
            return false;
        if (s.count == 0)
            continue;
        /// The remaining fields come from Thrift metadata, so a malformed file can carry out-of-range
        /// enum values just like `PageHeader` can. Unlike `PageHeader` (which we must decode to read the
        /// page itself), `encoding_stats` is only advisory input to this optimization's eligibility
        /// check - the page stream may still be perfectly readable via a full scan even if this metadata
        /// is garbage. So an unrecognized value here must not throw and fail the whole read; it should
        /// just make the chunk ineligible for the optimization, the same way a missing `encoding_stats`
        /// does. We read the underlying integer via `memcpy` (in `isValidThriftEnum`), avoiding the
        /// `-fsanitize=enum` undefined behavior of loading an out-of-range enumerator.
        if (!isValidThriftEnum(s.page_type, parq::_PageType_VALUES_TO_NAMES))
            return false;
        if (s.page_type != parq::PageType::DATA_PAGE && s.page_type != parq::PageType::DATA_PAGE_V2)
            continue;
        if (!isValidThriftEnum(s.encoding, parq::_Encoding_VALUES_TO_NAMES))
            return false;
        if (s.encoding != parq::Encoding::PLAIN_DICTIONARY && s.encoding != parq::Encoding::RLE_DICTIONARY)
            return false;
        has_dictionary_data_page = true;
    }
    return has_dictionary_data_page;
}

/// Hash all values of an already-decoded dictionary the same way query constants are hashed for
/// bloom filters, so the two can be compared. Returns nullopt if the values can't be hashed (in
/// which case the dictionary can't be used for filtering).
static std::optional<HashSet<UInt64>> hashDictionaryValues(
    const parq::FileMetaData & file_metadata, const ReadOptions & options,
    Reader::ColumnChunk & column, const Reader::PrimitiveColumnInfo & column_info,
    const PruningMemoryReservation & reservation, size_t & held_pruning_bytes)
{
    held_pruning_bytes = 0;
    chassert(column.dictionary.isInitialized());
    size_t count = column.dictionary.count;

    /// The eligibility check in `columnChunkCanUseDictionaryFilter` bounds only the *compressed*
    /// on-disk dictionary page (`dictionary_filter_limit_bytes`, 1 MiB by default), not the decoded
    /// value set we are about to build here. A highly compressible dictionary can stay under that
    /// limit yet decode to many times more, and constructing the value set below (a materialized
    /// column of all values, a vector of hashes, and a `HashSet` of them) then allocates that much
    /// transient memory - potentially for several row groups in parallel during pruning. Charge it to
    /// the shared pruning-stage budget (`reservation`): the reader's memory high watermark minus what
    /// the pruning stage already holds - the decoded dictionaries charged in `ReadManager::runTask`,
    /// plus the value sets already reserved by other dictionary lookups, whether earlier in this same
    /// row-group filter evaluation or concurrently on another worker thread. Because the reservation is
    /// held live in the shared `BloomFilterBlocksOrDictionary` stage counter (see
    /// `PruningMemoryReservation`), neither a predicate over several dictionary-filtered columns nor
    /// several row groups pruning in parallel can let each value set use the full budget and
    /// collectively overshoot the watermark. If the reservation would exceed the budget, skip the
    /// optimization and fall back to a full scan (reported as "can't rule out a match", the same as an
    /// unhashable type below). The watermark scales down automatically when the query has little memory
    /// to spare (see FormatFactory). This is the decoded-value-set cap the compressed-page limit alone
    /// cannot provide; the decoded dictionary *page* itself is capped against the same budget before it
    /// is used, in `decodeDictionaryPage` on the pruning path.
    /// `estimated_value_set_bytes` must be an upper bound on the peak transient memory allocated below,
    /// so that once the reservation succeeds the value set is guaranteed to stay within budget while it
    /// is built. The `hashes` vector (allocated at exactly `count` capacity by `parquetTryHashColumn`, so
    /// exactly `count * sizeof(UInt64)`) and the resulting `value_hashes` HashSet are always built; the
    /// hashing itself allocates nothing on top - `parquetTryHashColumn` hashes string values in place
    /// from the column's buffers rather than copying each into a `Field` scratch string, and every other
    /// hashable type is stored inline in `Field`. When
    /// the dictionary is not already an `IColumn` (FixedSize / StringPlain modes) the values are first
    /// materialized into a fresh column of `count` values plus an identity `indexes` vector: a
    /// `ColumnString` there reserves only its UInt64 offsets (exactly, via `reserve_exact`) and grows
    /// its `chars` buffer geometrically (up to ~2x the final size) as `insertData` appends, so count
    /// twice the *exact total* value bytes for the payload plus a UInt64 per-value offset. The total
    /// must not be derived from `getAverageValueSize`: flooring its fractional mean and multiplying
    /// back by `count` understates a mixed-length string dictionary by up to `count` bytes. The
    /// `Mode::Column` path hashes the already-decoded (and already-charged) column in place, so it
    /// needs none of the materialization terms.
    ///
    /// The `HashSet` term cannot be approximated per value: `HashSet::reserve` picks a power-of-two
    /// buffer with a maximum fill factor of 0.5 (`HashTableGrowerWithPrecalculation::set`), so the
    /// table holds up to ~4 cells per inserted hash and never fewer than its initial 256 cells.
    /// Compute the buffer size with the set's own growth rule so the reservation matches what
    /// `reserve` really allocates, for the full insert cardinality: all `count` dictionary hashes
    /// plus the one extra default-value hash possibly added under `input_format_null_as_default`
    /// below (reserving for it up front also guarantees that insert never triggers a rehash past the
    /// reservation). Add the set's initial constructor-allocated buffer, which can transiently
    /// coexist with the resized one inside `realloc`.
    using ValueHashSet = HashSet<UInt64>;
    ValueHashSet::grower_type value_set_grower;
    value_set_grower.set(count + 1);
    size_t value_set_buffer_bytes =
        (value_set_grower.bufSize() + ValueHashSet::grower_type::initial_count) * sizeof(ValueHashSet::cell_type);
    size_t per_value_bytes = sizeof(UInt64);    /// `hashes` vector
    size_t materialized_payload_bytes = 0;
    if (column.dictionary.mode != Dictionary::Mode::Column)
    {
        /// Exact total value bytes of the dictionary: `StringPlain` stores each value with a 4-byte
        /// length prefix in `data`, fixed-size modes store `value_size` bytes per value.
        size_t total_value_bytes = column.dictionary.mode == Dictionary::Mode::StringPlain
            ? column.dictionary.data.size() - 4 * count
            : count * column.dictionary.value_size;
        materialized_payload_bytes = 2 * total_value_bytes;    /// materialized column payload, incl. geometric chars growth
        per_value_bytes +=
            sizeof(UInt64)      /// materialized `ColumnString` offsets (0 for fixed types; counted conservatively)
            + sizeof(UInt32);   /// identity `indexes`
    }
    size_t estimated_value_set_bytes = count * per_value_bytes + materialized_payload_bytes + value_set_buffer_bytes;
    /// Reserve the peak footprint before allocating anything. If it does not fit, skip pruning.
    if (!reservation.tryReserve(estimated_value_set_bytes))
        return std::nullopt;
    /// Release the whole reservation on every early-out below; only the successful path keeps the
    /// persistent part reserved (reduced to the actual `HashSet` footprint) and hands it to the caller
    /// via `held_pruning_bytes` (see `DictionaryLookup`, which releases it when the value set is freed).
    bool committed = false;
    SCOPE_EXIT({ if (!committed) reservation.release(estimated_value_set_bytes); });

    /// Hash the dictionary values the same way query constants are hashed (see prepareBloomFilterCondition).
    parquet::ColumnDescriptor desc = makeColumnDescriptor(file_metadata, column_info);
    std::optional<std::vector<uint64_t>> hashes;
    if (column.dictionary.mode == Dictionary::Mode::Column)
    {
        /// The values already exist as a decoded column (built from `column_info.decoded_type`, same
        /// as `values` below), so hash it in place instead of materializing an identical second copy.
        hashes = parquetTryHashColumn(column.dictionary.col.get(), &desc);
    }
    else
    {
        /// FixedSize / StringPlain dictionaries hold raw bytes rather than an `IColumn`, so we must
        /// materialize the values into a column of the decoded type before hashing.
        auto indexes = ColumnUInt32::create();
        auto & indexes_data = indexes->getData();
        indexes_data.resize_exact(count);
        for (size_t i = 0; i < count; ++i)
            indexes_data[i] = static_cast<UInt32>(i);

        auto values = column_info.decoded_type->createColumn();
        values->reserve(count);
        column.dictionary.index(*indexes, *values);
        hashes = parquetTryHashColumn(values.get(), &desc);
    }
    if (!hashes.has_value())
        return std::nullopt;
    ValueHashSet value_hashes;
    /// +1 for the possible extra default-value hash below: when `hashes->size()` lands exactly on the
    /// table's maximum fill, that late insert would otherwise rehash past the reserved buffer.
    value_hashes.reserve(hashes->size() + 1);
    for (UInt64 h : *hashes)
        value_hashes.insert(h);

    /// The dictionary holds only the non-null values of the column chunk, so we must account for how
    /// nulls are read into the output, mirroring the conservative null handling of the min/max path in
    /// `adjustRangeFromIndexIfNeeded`.
    bool nullable = column_info.levels.back().def > 0;
    bool can_be_null = !column.meta->meta_data.statistics.__isset.null_count
        || column.meta->meta_data.statistics.null_count != 0;
    if (nullable && can_be_null && !column_info.output_nullable)
    {
        if (options.format.null_as_default)
        {
            /// Under `input_format_null_as_default`, null values are decoded as the type's default
            /// value, which is not in the dictionary; without accounting for it we'd wrongly skip a
            /// row group whose nulls match the queried default (e.g. `WHERE x = 0` over an optional
            /// column read as non-nullable `UInt64`). So add the default value's hash.
            auto default_hash = parquetTryHashField(column_info.output_type->getDefault(), &desc);
            /// If the default value can't be hashed, we can't rule out a match.
            if (!default_hash.has_value())
                return std::nullopt;
            value_hashes.insert(*default_hash);
        }
        else
        {
            /// Reading a null into a non-nullable column raises `CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN`
            /// during decoding. Skipping the row group would suppress that error and change the query
            /// result, so we must not prune: report that we can't rule out a match.
            return std::nullopt;
        }
    }

    /// Free the transient `hashes` vector (the `indexes`/`values` allocations were already freed by
    /// leaving their scope above) before releasing the reservation that covers it: otherwise a
    /// concurrent pruning task could observe the released budget as free while `hashes` is still
    /// allocated here, transiently exceeding the watermark.
    hashes.reset();

    /// The value set is kept alive (in its `DictionaryLookup`) until this whole row-group filter
    /// evaluation finishes, so keep its persistent footprint - the real `HashSet` buffer - reserved
    /// against the shared budget and hand the amount to the caller to release when the value set is
    /// freed. The transient `indexes`/`values`/`hashes` allocations were already freed above, so
    /// release that part of the reservation now: a second dictionary-filtered column, or another row
    /// group pruning in parallel, then sees only the persistent part still held, keeping the combined
    /// footprint within the watermark without over-reserving for the transients. The estimate above
    /// follows the set's own growth rule, so it never under-predicts the buffer; the grow branch is a
    /// defensive backstop (mirroring `decodeDictionaryPage`) in case the two ever drift apart, falling
    /// back to a full scan if the correction no longer fits the budget.
    size_t persistent_bytes = value_hashes.getBufferSizeInBytes();
    if (persistent_bytes > estimated_value_set_bytes)
    {
        if (!reservation.tryReserve(persistent_bytes - estimated_value_set_bytes))
            return std::nullopt;
    }
    else
    {
        reservation.release(estimated_value_set_bytes - persistent_bytes);
    }
    held_pruning_bytes = persistent_bytes;
    committed = true;

    return value_hashes;
}

struct Reader::DictionaryLookup : public KeyCondition::BloomFilter
{
    Reader & reader;
    ColumnChunk & column;
    const PrimitiveColumnInfo & column_info;
    /// The budget shared by every dictionary lookup in one `applyBloomAndDictionaryFilters` call and,
    /// through its shared stage counter, by every row group pruning in parallel. Each built value set's
    /// persistent footprint is charged here (in `hashDictionaryValues`) and released when this lookup is
    /// destroyed (at the end of the evaluation).
    PruningMemoryReservation reservation;
    size_t reserved_bytes = 0;

    bool computed = false;
    std::optional<HashSet<UInt64>> value_hashes;

    /// Bloom filter of the same column chunk, kept as a fallback for when the exact dictionary value set
    /// can't be built within the pruning memory budget (see `hashDictionaryValues`). Without it such a
    /// chunk would be read in full even though its bloom filter could rule it out. Null when the chunk
    /// has no bloom filter (see `initializePrefetches`).
    std::unique_ptr<BloomFilterLookup> bloom_fallback;

    DictionaryLookup(Reader & reader_, ColumnChunk & column_, const PrimitiveColumnInfo & column_info_, PruningMemoryReservation reservation_)
        : reader(reader_), column(column_), column_info(column_info_), reservation(reservation_) {}

    ~DictionaryLookup() override
    {
        /// Free the value set before releasing the reservation that covers it, so a concurrent pruning
        /// task never observes this budget as free while `value_hashes` is still allocated (members are
        /// destroyed only after this body runs, so the release must clear it explicitly first).
        value_hashes.reset();
        reservation.release(reserved_bytes);
    }

    bool findAnyHash(const std::vector<uint64_t> & hashes) override;
};

bool Reader::DictionaryLookup::findAnyHash(const std::vector<uint64_t> & hashes)
{
    if (!computed)
    {
        value_hashes = hashDictionaryValues(reader.file_metadata, reader.options, column, column_info, reservation, reserved_bytes);
        computed = true;
    }
    /// If the dictionary values couldn't be hashed (e.g. the value set didn't fit the pruning budget),
    /// fall back to the column chunk's bloom filter if it has one; otherwise we can't rule out a match.
    if (!value_hashes.has_value())
        return bloom_fallback ? bloom_fallback->findAnyHash(hashes) : true;
    for (UInt64 h : hashes)
        if (value_hashes->contains(h))
            return true;
    return false;
}

bool Reader::applyBloomAndDictionaryFilters(RowGroup & row_group, PruningMemoryReservation reservation)
{
    /// A single budget shared by every dictionary lookup in this row-group filter evaluation, and -
    /// because it charges the shared `BloomFilterBlocksOrDictionary` stage counter - by every row group
    /// pruning in parallel on other worker threads. Each lookup's value set stays alive (in its
    /// `DictionaryLookup`) until the evaluation finishes, so without shared accounting a predicate over
    /// several dictionary-filtered columns, or several concurrent row groups, would let each value set
    /// use the full budget and collectively overshoot the watermark. Copied into each `DictionaryLookup`
    /// (all copies point at the same atomic stage counter); watermark 0 means unbounded (see
    /// `ReadManager::pruningMemoryReservation` and `PruningMemoryReservation`).
    KeyCondition::ColumnIndexToBloomFilter filter_map;
    for (size_t i = 0; i < row_group.columns.size(); ++i)
    {
        ColumnChunk & column = row_group.columns[i];
        /// The exact dictionary filter takes precedence over the bloom filter. Both can be set for the
        /// same column chunk (see initializePrefetches): the bloom filter is then kept only as a runtime
        /// fallback for when the dictionary path declines because its value set does not fit the pruning
        /// memory budget. `decodeDictionaryPage` failing earlier (in `ReadManager::runTask`) clears
        /// `use_dictionary_filter`, so that case is handled by the `else if` bloom branch below.
        if (column.use_dictionary_filter)
        {
            auto lookup = std::make_unique<DictionaryLookup>(*this, column, primitive_columns[i], reservation);
            if (column.use_bloom_filter && !column.bloom_filter_blocks.empty())
                lookup->bloom_fallback = std::make_unique<BloomFilterLookup>(prefetcher, column);
            filter_map.emplace(primitive_columns[i].idx_in_output_block, std::move(lookup));
        }
        else if (column.use_bloom_filter)
            filter_map.emplace(
                primitive_columns[i].idx_in_output_block,
                std::make_unique<BloomFilterLookup>(prefetcher, column));
    }
    /// We use both the min/max statistics and bloom/dictionary filters. For the case where condition
    /// has something like `x < 42 OR y = 1337`, where `x < 42` is ruled out by min/max, and
    /// `y = 1337` is ruled out by the filter.
    /// (I'm guessing this hardly ever comes up in practice, but it was easy enough to support.)
    return bloom_filter_condition->checkInHyperrectangle(
        row_group.hyperrectangle, extended_sample_block_data_types, filter_map).can_be_true;
}

void Reader::applyColumnIndex(ColumnChunk & column, const PrimitiveColumnInfo & column_info, const RowGroup & row_group)
{
    try
    {
        chassert(column.use_column_index);
        chassert(!column_info.column_index_conditions.empty());

        auto data = prefetcher.getRangeData(column.column_index_prefetch);
        parq::ColumnIndex column_index;
        deserializeThriftStruct(column_index, data.data(), data.size());

        size_t num_pages = column.offset_index.page_locations.size();
        bool nullable = column_info.levels.back().def > 0;
        bool null_as_default = options.format.null_as_default && !column_info.output_nullable;
        if (column_index.min_values.size() != num_pages || column_index.max_values.size() != num_pages ||
            (column_index.null_pages.size() != num_pages && !column_index.null_pages.empty()) ||
            (column_index.__isset.null_counts && column_index.null_counts.size() != num_pages))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected number of pages: {} null_pages, {} null_counts, {} min_values, {} max_values, {} pages in offset index", column_index.null_pages.size(), column_index.null_counts.size(), column_index.min_values.size(), column_index.max_values.size(), num_pages);

        /// The Range must be in terms of the type that checkInHyperrectangle compares it
        /// against, which may differ from decoded_type - see cast_stats_to_output_type.
        const IDataType & output_block_type = *extended_sample_block_data_types.at(column_info.idx_in_output_block);

        Hyperrectangle hyperrectangle(extended_sample_block.columns(), Range::createWholeUniverse());
        size_t prev_row_idx = 0; // start of the latest range of rows that pass filter
        size_t pruned_pages = 0;
        for (size_t page_idx = 0; page_idx < num_pages; ++page_idx)
        {
            Range & range = hyperrectangle[column_info.idx_in_output_block];
            range = Range::createWholeUniverse();

            bool always_null = !column_index.null_pages.empty() && column_index.null_pages[page_idx];
            bool can_be_null = !column_index.__isset.null_counts || column_index.null_counts[page_idx] != 0;

            if (nullable && always_null)
            {
                /// Single-point range containing either the default value or one of the infinities.
                if (null_as_default)
                    range.right = range.left = output_block_type.getDefault();
                else
                    range.right = range.left;
            }
            else
            {
                column_info.decoder.decodeField(column_index.min_values[page_idx], /*is_max=*/ false, *column_info.decoded_type, output_block_type, range.left);
                column_info.decoder.decodeField(column_index.max_values[page_idx], /*is_max=*/ true, *column_info.decoded_type, output_block_type, range.right);

                adjustRangeFromIndexIfNeeded(range, column_info, can_be_null);
            }

            /// All conjunctive predicates on this column (e.g. two `pointInPolygon` calls sharing
            /// the same bbox column) must agree the page can match; any one of them ruling it out
            /// is enough to prune, so an unproductive later `checkInHyperrectangle` call is skipped.
            bool passes_filter = std::all_of(
                column_info.column_index_conditions.begin(), column_info.column_index_conditions.end(),
                [&](const PrimitiveColumnInfo::ColumnIndexCondition & c)
                {
                    /// A `covering.bbox` predicate may only prune when the full bbox is known for
                    /// every row the page covers. A NULL bbox means unknown spatial extent, and
                    /// min/max statistics describe the non-null values only, so the predicate can
                    /// come out false while a NULL-bbox row still matches. Page boundaries are
                    /// per column, so it is not enough that this column's page is null-free: a
                    /// sibling bbox column may hold NULLs on the very rows this page covers.
                    /// Require the same four-column guarantee the row-group path checks, plus
                    /// this page's own null counts (which also fail closed when the column index
                    /// omits `null_counts` or contradicts itself with an all-null page).
                    if (c.spatial_key_condition_idx != SIZE_MAX
                        && (can_be_null || always_null
                            || !spatialBboxStatsHaveNoNulls(*row_group.meta, c.spatial_key_condition_idx)))
                        return true;
                    return c.condition->checkInHyperrectangle(hyperrectangle, extended_sample_block_data_types).can_be_true;
                });

            if (!passes_filter)
            {
                size_t start_row = column.offset_index.page_locations[page_idx].first_row_index;
                size_t end_row = page_idx + 1 < num_pages ? column.offset_index.page_locations[page_idx + 1].first_row_index : row_group.meta->num_rows;
                chassert(end_row > start_row); // validated in decodeOffsetIndex
                if (start_row > prev_row_idx)
                    column.row_ranges_after_column_index.emplace_back(prev_row_idx, start_row);
                prev_row_idx = end_row;
                ++pruned_pages;
            }
        }

        if (size_t(row_group.meta->num_rows) > prev_row_idx)
            column.row_ranges_after_column_index.emplace_back(prev_row_idx, row_group.meta->num_rows);

        if (pruned_pages)
            ProfileEvents::increment(ProfileEvents::ParquetPrunedPages, pruned_pages);
    }
    catch (Exception & e)
    {
        /// A `covering.bbox` column that only carries spatial conditions was injected for this
        /// optimization alone: it is neither an output column nor part of the query's own filter.
        /// Malformed page statistics for it must fail closed (no page pruning for this column)
        /// rather than abort the read, matching `getHyperrectangleForRowGroup`. A bbox column the
        /// query itself reads or filters on has non-spatial conditions too and keeps throwing.
        const bool only_spatial_conditions = std::all_of(
            column_info.column_index_conditions.begin(), column_info.column_index_conditions.end(),
            [](const PrimitiveColumnInfo::ColumnIndexCondition & c) { return c.spatial_key_condition_idx != SIZE_MAX; });
        if (column_info.is_spatial_bbox_column && only_spatial_conditions)
        {
            column.row_ranges_after_column_index.clear();
            if (row_group.meta->num_rows > 0)
                column.row_ranges_after_column_index.emplace_back(0, size_t(row_group.meta->num_rows));
            return;
        }
        e.addMessage("in column index; use input_format_parquet_page_filter_push_down=0 to ignore");
        throw;
    }
}

void Reader::adjustRangeFromIndexIfNeeded(Range & range, const PrimitiveColumnInfo & column_info, bool can_be_null) const
{
    if (accurateLess(range.right, range.left))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Statistics have min_value > max_value: {} > {}.", static_cast<const Field &>(range.left), static_cast<const Field &>(range.right));

    bool nullable = column_info.levels.back().def > 0;
    bool null_as_default = options.format.null_as_default && !column_info.output_nullable;

    if (nullable && can_be_null)
    {
        if (null_as_default)
        {
            /// Note: the default of the output block type, not of output_type, because the range
            /// is in terms of the former - see cast_stats_to_output_type.
            Field default_value = extended_sample_block_data_types.at(column_info.idx_in_output_block)->getDefault();
            /// Make sure the range contains the default value.
            if (!range.left.isNull() && accurateLess(default_value, range.left))
                range.left = default_value;
            if (!range.right.isNull() && accurateLess(range.right, default_value))
                range.right = default_value;
        }
        else
        {
            /// Make sure the range includes NULL.
            /// In Range, NULL is represented as infinity (positive or negative, doesn't matter).
            /// So, make sure the range reaches infinity on at least one side.
            /// We arbitrarily picked negative rather than positive infinity.
            if (!range.left.isNull() && !range.right.isNull())
                range.left = NEGATIVE_INFINITY;
        }
    }
    else
    {
        /// If the column doesn't have nulls, exclude both infinities.
        if (range.left.isNull())
            range.left_included = false;
        if (range.right.isNull())
            range.right_included = false;
    }
}

void Reader::intersectColumnIndexResultsAndInitSubgroups(RowGroup & row_group)
{
    const auto & rows_to_read = format_filter_info->rows_to_read;

    std::vector<std::pair<size_t, size_t>> row_ranges;
    size_t num_rows = 0;
    {
        /// Do a sweep to find the intersection of all per-column row sets.
        std::vector<std::pair<size_t, /*sign*/ int>> events;

        /// Add an extra row set representing the whole row group so that we don't need a separate
        /// code path for when column index is not used.
        int num_range_sets = 1;
        events.emplace_back(0, +1);
        events.emplace_back(size_t(row_group.meta->num_rows), -1);

        if (rows_to_read)
        {
            /// Lazy materialization: one coarse range covering the requested rows of this row group.
            /// The exact row set is applied through the subgroup filters below; page-level reads
            /// stay exact because `determinePagesToPrefetch` checks the filter per page.
            const auto [slice_begin, slice_end] = row_group.requested_rows_slice;
            chassert(slice_begin < slice_end); // row groups with no requested rows are skipped in prefilterAndInitRowGroups
            num_range_sets += 1;
            events.emplace_back((*rows_to_read)[slice_begin] - row_group.start_global_row_idx, +1);
            events.emplace_back((*rows_to_read)[slice_end - 1] - row_group.start_global_row_idx + 1, -1);
        }

        for (auto & col : row_group.columns)
        {
            if (!col.use_column_index)
                continue;
            if (col.row_ranges_after_column_index.empty())
                /// Whole row group was filtered out, leave `subgroups` empty.
                return;

            num_range_sets += 1;
            size_t prev_end = 0;
            for (size_t i = 0; i < col.row_ranges_after_column_index.size(); ++i)
            {
                const auto [start, end] = col.row_ranges_after_column_index[i];
                chassert(start < end);
                chassert(!i || start > prev_end);
                prev_end = end;  /// NOLINT(clang-analyzer-deadcode.DeadStores)

                events.emplace_back(start, +1);
                events.emplace_back(end, -1);
            }

            col.row_ranges_after_column_index = {}; // free some memory
        }

        /// (Important that -1 comes before +1, otherwise we'd get empty ranges in the output.)
        std::sort(events.begin(), events.end());
        int coverage = 0;
        for (size_t i = 0; i < events.size(); ++i)
        {
            coverage += events[i].second;
            chassert(coverage >= 0 && coverage <= num_range_sets);
            if (coverage == num_range_sets)
            {
                row_ranges.emplace_back(events[i].first, events.at(i + 1).first);

                chassert(row_ranges.back().second > row_ranges.back().first);
                chassert(row_ranges.size() == 1 || row_ranges.back().first > row_ranges[row_ranges.size() - 2].second);
                num_rows += row_ranges.back().second - row_ranges.back().first;
            }
        }
    }
    if (num_rows == 0)
        return;

    size_t rows_per_subgroup = num_rows;
    if (options.format.parquet.max_block_size > 0)
        rows_per_subgroup = std::min(rows_per_subgroup, size_t(options.format.parquet.max_block_size));

    if (options.format.parquet.prefer_block_bytes > 0)
    {
        double bytes_per_row = 0;
        for (size_t i = 0; i < primitive_columns.size(); ++i)
            bytes_per_row += estimateColumnMemoryBytesPerRow(row_group.columns.at(i), row_group, primitive_columns.at(i));

        size_t n = size_t(static_cast<double>(options.format.parquet.prefer_block_bytes) / std::max(bytes_per_row, 1.));
        n = std::max(n, size_t(128)); // avoid super tiny blocks if something is wrong with stats
        rows_per_subgroup = std::min(rows_per_subgroup, n);
    }
    chassert(rows_per_subgroup > 0);

    /// (Currently we turn each element of row_ranges into at least one row subgroup. If column index
    ///  filtering produced lots of short row ranges, we'll end up with lots of short row subgroups.
    ///  It seems that this would be very rare in practice. If it turns out to be a problem, it's easy
    ///  to add coalescing of nearby short ranges here, similar to coalescing read ranges, initializing
    ///  `filter` to keep only the rows covered by ranges.)
    for (const auto [start, end] : row_ranges)
    {
        for (size_t substart = start; substart < end; substart += rows_per_subgroup)
        {
            size_t subend = std::min(end, substart + rows_per_subgroup);

            RowSubgroup & row_subgroup = row_group.subgroups.emplace_back();
            row_subgroup.start_row_idx = substart;
            row_subgroup.filter.rows_pass = row_group.need_to_process ? subend - substart : 0;
            row_subgroup.filter.rows_total = subend - substart;

            if (rows_to_read && row_group.need_to_process)
            {
                /// Lazy materialization: initialize the filter to keep only the requested rows,
                /// the same way a prewhere filter would.
                const UInt64 * slice_begin_ptr = rows_to_read->data() + row_group.requested_rows_slice.first;
                const UInt64 * slice_end_ptr = rows_to_read->data() + row_group.requested_rows_slice.second;
                const UInt64 * range_begin = std::lower_bound(slice_begin_ptr, slice_end_ptr, row_group.start_global_row_idx + substart);
                const UInt64 * range_end = std::lower_bound(range_begin, slice_end_ptr, row_group.start_global_row_idx + subend);
                size_t rows_pass = size_t(range_end - range_begin);
                chassert(rows_pass <= row_subgroup.filter.rows_total);
                if (rows_pass != row_subgroup.filter.rows_total)
                {
                    row_subgroup.filter.rows_pass = rows_pass;
                    if (rows_pass != 0)
                    {
                        row_subgroup.filter.filter.resize_fill(row_subgroup.filter.rows_total, 0);
                        for (const UInt64 * it = range_begin; it != range_end; ++it)
                            row_subgroup.filter.filter[*it - row_group.start_global_row_idx - substart] = 1;
                    }
                }
            }

            row_subgroup.columns.resize(primitive_columns.size());
            row_subgroup.output = std::vector<OutputColumnState>(extended_sample_block.columns());
            for (size_t idx = 0; idx < row_subgroup.output.size(); ++idx)
            {
                const auto & output_idx = sample_block_to_output_columns_idx.at(idx);
                if (output_idx.has_value())
                {
                    const auto & info = output_columns.at(*output_idx);
                    row_subgroup.output[idx].primitive_columns_remaining.store(info.primitive_end - info.primitive_start);
                }
            }
            if (options.format.defaults_for_omitted_fields)
                row_subgroup.block_missing_values.init(sample_block->columns());
        }
    }
    row_group.intersected_row_ranges_after_column_index = std::move(row_ranges);
}

void Reader::decodeOffsetIndex(ColumnChunk & column, const RowGroup & row_group)
{
    auto data = prefetcher.getRangeData(column.offset_index_prefetch);
    deserializeThriftStruct(column.offset_index, data.data(), data.size());

    if (column.offset_index.page_locations.empty())
        /// (Other code in this file relies on page_locations being nonempty.)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Empty offset index");
    const auto & locations = column.offset_index.page_locations;

    /// Validate.

    const auto & meta = column.meta->meta_data;
    int64_t end_offset = meta.total_compressed_size + std::min({
            meta.data_page_offset,
            meta.__isset.dictionary_page_offset ? meta.dictionary_page_offset : INT64_MAX,
            meta.__isset.index_page_offset ? meta.index_page_offset : INT64_MAX
        });
    int64_t num_rows = row_group.meta->num_rows;

    int64_t prev_offset = meta.data_page_offset;
    int64_t prev_row_index = -1;
    for (const auto & loc : locations)
    {
        if (loc.offset < prev_offset || loc.first_row_index <= prev_row_index ||
            loc.compressed_page_size <= 0 || loc.compressed_page_size > end_offset - loc.offset ||
            loc.first_row_index >= num_rows)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid offset index: {}, prev offset: {}, prev row: {}, end offset: {}, num rows: {}", thriftToString(loc), prev_offset, prev_row_index, end_offset, num_rows);
        prev_offset = loc.offset + loc.compressed_page_size;
        prev_row_index = loc.first_row_index;
    }
}

void Reader::determinePagesToPrefetch(ColumnChunk & column, const RowSubgroup & row_subgroup, const RowGroup & row_group, std::vector<PrefetchHandle *> & out)
{
    chassert(row_subgroup.filter.rows_pass > 0);
    if (column.offset_index.page_locations.empty())
        return; // no offset index, can't prefetch individual pages

    if (column.data_pages.empty())
    {
        const auto & locations = column.offset_index.page_locations;
        const auto & row_ranges = row_group.intersected_row_ranges_after_column_index;
        chassert(!row_ranges.empty());
        std::vector<std::pair</*global_offset*/ size_t, /*length*/ size_t>> page_byte_ranges;

        /// Some writers don't assign dictionary_page_offset and instead set data_page_offset to
        /// point to the dictionary page. Such undeclared dictionary page is not listed in offset
        /// index. So, if the offset index starts at an offset higher than data_page_offset, we make
        /// a guess that there's a dictionary page at data_page_offset.
        bool has_undeclared_dictionary_page = false;
        if (!column.meta->meta_data.__isset.dictionary_page_offset)
        {
            chassert(!column.dictionary_page_prefetch);
            if (locations.at(0).offset > column.meta->meta_data.data_page_offset)
            {
                page_byte_ranges.emplace_back(
                    size_t(column.meta->meta_data.data_page_offset),
                    size_t(locations[0].offset - column.meta->meta_data.data_page_offset));
                has_undeclared_dictionary_page = true;
            }
        }

        size_t ranges_idx = 0;
        for (size_t page_idx = 0; page_idx < locations.size(); ++page_idx)
        {
            const auto & loc = locations[page_idx];
            while (ranges_idx < row_ranges.size() && row_ranges[ranges_idx].second <= size_t(loc.first_row_index))
                ++ranges_idx;
            size_t page_end = size_t(page_idx + 1 < locations.size() ? locations[page_idx + 1].first_row_index : row_group.meta->num_rows);
            if (ranges_idx < row_ranges.size() && row_ranges[ranges_idx].first < page_end)
            {
                column.data_pages.push_back(DataPage {.meta = &loc, .end_row_idx = page_end});
                page_byte_ranges.emplace_back(size_t(loc.offset), size_t(loc.compressed_page_size));
            }
        }
        chassert(!page_byte_ranges.empty());

        auto handles = prefetcher.splitRange(std::move(column.data_pages_prefetch), page_byte_ranges, /*likely_to_be_used*/ false);

        if (has_undeclared_dictionary_page)
            column.dictionary_page_prefetch = std::move(handles.at(0));
        for (size_t i = 0; i < column.data_pages.size(); ++i)
            column.data_pages[i].prefetch = std::move(handles[i + size_t(has_undeclared_dictionary_page)]);
    }

    size_t subgroup_end = row_subgroup.start_row_idx + row_subgroup.filter.rows_total;
    while (column.data_pages_prefetch_idx < column.data_pages.size())
    {
        auto & page = column.data_pages[column.data_pages_prefetch_idx];
        size_t page_start = size_t(page.meta->first_row_index);
        if (page_start >= subgroup_end)
            break;
        size_t start_row_idx = std::max(page_start, row_subgroup.start_row_idx);
        size_t end_row_idx = std::min(page.end_row_idx, subgroup_end);

        bool passes_filter = row_subgroup.filter.rows_pass > 0 && end_row_idx > start_row_idx;
        if (passes_filter && row_subgroup.filter.rows_pass < row_subgroup.filter.rows_total)
            passes_filter = !memoryIsZero(row_subgroup.filter.filter.data(), start_row_idx - row_subgroup.start_row_idx, end_row_idx - row_subgroup.start_row_idx);

        if (passes_filter)
            out.push_back(&page.prefetch); // this subgroup needs this page
        else if (page.end_row_idx > subgroup_end)
            break; // page continues in next row subgroup
        else
            page.prefetch = {}; // no subgroup needs this page
        ++column.data_pages_prefetch_idx;
    }
}

double Reader::estimateAverageStringLengthPerRow(const ColumnChunk & column, const RowGroup & row_group) const
{
    double column_chunk_bytes = 0;
    if (column.meta->meta_data.__isset.size_statistics &&
        column.meta->meta_data.size_statistics.__isset.unencoded_byte_array_data_bytes)
    {
        /// The writer of the parquet file has helpfully put the total length of the
        /// strings into file metadata. Thanks writer!
        column_chunk_bytes = static_cast<double>(column.meta->meta_data.size_statistics.unencoded_byte_array_data_bytes);
    }
    else if (column.meta->meta_data.__isset.dictionary_page_offset)
    {
        /// Dictionary-encoded strings. No way to know the decoded length in advance.
        double avg_string_length = 0;
        if (column.dictionary.isInitialized())
        {
            /// We've read the dictionary. Use the average string length in the dictionary as a guess
            /// at the average string length in the column chunk.
            avg_string_length = column.dictionary.getAverageValueSize();
        }
        else
        {
            /// We have no idea how long the strings are. Use some made up number (not chosen carefully).
            avg_string_length = 20;
        }
        /// Null values don't contribute to string data. Subtract null_count when available
        /// to avoid massive overestimation for columns with high null rates and large dictionary entries.
        double non_null_values = static_cast<double>(column.meta->meta_data.num_values);
        if (column.meta->meta_data.statistics.__isset.null_count)
            non_null_values = std::max(0., non_null_values - static_cast<double>(column.meta->meta_data.statistics.null_count));
        column_chunk_bytes = avg_string_length * non_null_values;
    }
    else
    {
        /// Non-dictionary-encoded strings.
        column_chunk_bytes = static_cast<double>(column.meta->meta_data.total_uncompressed_size);
    }

    return column_chunk_bytes / static_cast<double>(row_group.meta->num_rows);
}

double Reader::estimateColumnMemoryBytesPerRow(const ColumnChunk & column, const RowGroup & row_group, const PrimitiveColumnInfo & column_info) const
{
    double res = 0;
    if (column_info.output_type->haveMaximumSizeOfValue())
        /// Fixed-size values, e.g. numbers or FixedString.
        res = 1. * static_cast<double>(column_info.output_type->getMaximumSizeOfValueInMemory())
            * static_cast<double>(column.meta->meta_data.num_values) / static_cast<double>(row_group.meta->num_rows);
    else
        res = estimateAverageStringLengthPerRow(column, row_group);

    /// Outer array offsets.
    if (column_info.levels.back().rep > 0)
        res += 8;

    /// Nested array offsets (assume the worst case where the outer arrays are long and inner arrays
    /// are short, so inner arrays have ~num_values total elements rather than ~num_rows).
    if (column_info.levels.back().rep > 1)
        res += (column_info.levels.back().rep - 1) * 8. * static_cast<double>(column.meta->meta_data.num_values) / static_cast<double>(row_group.meta->num_rows);

    return res;
}

void Reader::decodePrimitiveColumn(ColumnChunk & column, const PrimitiveColumnInfo & column_info, ColumnSubchunk & subchunk, const RowGroup & row_group, RowSubgroup & row_subgroup)
{
    /// Allocate columns for values, null map, and array offsets.

    size_t output_num_values_estimate = 0;
    if (column_info.levels.back().rep == 0)
        output_num_values_estimate = row_subgroup.filter.rows_pass; // no arrays, rows == values
    else if (row_subgroup.filter.rows_pass == size_t(row_group.meta->num_rows))
        output_num_values_estimate = column.meta->meta_data.num_values; // whole column chunk
    else
        /// There are arrays, so we can't know exactly how many primitive values there are in
        /// rows that pass the filter. Make a guess using average array length.
        output_num_values_estimate = size_t(1.2 * static_cast<double>(row_subgroup.filter.rows_pass) / static_cast<double>(row_group.meta->num_rows) * static_cast<double>(column.meta->meta_data.num_values));

    subchunk.arrays_offsets.resize(column_info.levels.back().rep);
    for (size_t i = 0; i < subchunk.arrays_offsets.size(); ++i)
    {
        subchunk.arrays_offsets[i] = ColumnArray::ColumnOffsets::create();
        subchunk.arrays_offsets[i]->reserve(i ? output_num_values_estimate : row_subgroup.filter.rows_total);
    }

    if (column.need_null_map)
    {
        subchunk.null_map = ColumnUInt8::create();
        subchunk.null_map->reserve(output_num_values_estimate);
    }

    subchunk.column = column_info.decoded_type->createColumn();
    subchunk.column->reserve(output_num_values_estimate);
    if (auto * string_column = typeid_cast<ColumnString *>(subchunk.column.get()))
    {
        double avg_len = estimateAverageStringLengthPerRow(column, row_group);
        size_t bytes_to_reserve = size_t(1.2 * avg_len * static_cast<double>(row_subgroup.filter.rows_pass));
        string_column->getChars().reserve(bytes_to_reserve);
    }

    /// Find ranges of rows that pass filter and decode them.

    /// When we have per-page prefetches (offset index), some pages may have had their prefetch
    /// handles reset by determinePagesToPrefetch because they are fully filtered out. The
    /// use_filter_in_decoder path reads ALL pages sequentially, so it would crash trying to access
    /// those reset handles. Only use this optimization when reading the whole column chunk
    /// sequentially (no offset index, i.e. data_pages is empty).
    ///
    /// Also disabled for nullable columns (need_null_map): the filter-in-decoder path processes
    /// ALL rows (passing + non-passing) through processDefLevelsForInnermostColumn, so the
    /// null_map ends up with entries for all rows rather than just filtered rows. Additionally,
    /// the decoder applies the filter at consecutive encoded-value indices, but with nulls the
    /// encoded values don't correspond 1:1 to rows (null rows have no encoded values), causing
    /// the filter to be applied at wrong positions. The standard row-range iteration path
    /// correctly handles both issues by only reading rows in passing filter ranges.
    const bool use_filter_in_decoder = (column_info.levels.back().rep == 0) &&
        !row_subgroup.filter.filter.empty() &&
        column.page.initialized &&
        !column.page.is_dictionary_encoded &&
        column.data_pages.empty() &&
        !column.need_null_map;
    const size_t subgroup_end_row_idx = row_subgroup.start_row_idx + row_subgroup.filter.rows_total;

    if (use_filter_in_decoder)
    {
        skipToRowOrNextPage(row_subgroup.start_row_idx, column, column_info);

        while (true) // loop over pages
        {
            readRowsInPage(subgroup_end_row_idx, subchunk, column, column_info, &row_subgroup);

            auto & page = column.page;
            if (page.next_row_idx == subgroup_end_row_idx &&
                (page.value_idx < page.num_values ||
                 page.end_row_idx.has_value() ||
                 column.next_page_offset >= column.data_pages_bytes))
                break;

            chassert(page.value_idx == page.num_values);
            skipToRowOrNextPage(std::nullopt, column, column_info);
            chassert(page.value_idx == 0);
        }
    }
    else
    {
        size_t row_subidx = 0;
        while (true) // loop over row ranges that pass the filter
        {
            size_t num_rows = row_subgroup.filter.rows_total - row_subidx;
            if (!row_subgroup.filter.filter.empty())
            {
                while (row_subidx < row_subgroup.filter.rows_total && !row_subgroup.filter.filter[row_subidx])
                    row_subidx += 1;
                num_rows = 0;
                while (row_subidx + num_rows < row_subgroup.filter.rows_total && row_subgroup.filter.filter[row_subidx + num_rows])
                    num_rows += 1;
            }
            if (!num_rows)
                break;
            size_t start_row_idx = row_subgroup.start_row_idx + row_subidx;
            size_t end_row_idx = start_row_idx + num_rows;
            row_subidx += num_rows;

            skipToRowOrNextPage(start_row_idx, column, column_info);

            while (true) // loop over pages
            {
                readRowsInPage(end_row_idx, subchunk, column, column_info);

                auto & page = column.page;
                if (page.next_row_idx == end_row_idx &&
                    (page.value_idx < page.num_values ||
                     page.end_row_idx.has_value() ||
                     column.next_page_offset >= column.data_pages_bytes))
                    break;

                chassert(page.value_idx == page.num_values);
                skipToRowOrNextPage(std::nullopt, column, column_info);
                chassert(page.value_idx == 0);
            }
        }
    }

    for (const auto & offsets : subchunk.arrays_offsets)
    {
        /// If repetition levels say that the column chunk starts in the middle of an array
        /// (e.g. first rep level is not 0; there are other cases with nested arrays),
        /// processRepDefLevelsForArray will correspondingly reassign the offset of the start of the
        /// first array. That wouldn't be a valid ColumnArray.
        /// This may also indicate a bug where we stopped reading previous column subchunk in the
        /// middle of an array.
        const auto & data = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets).getData();
        if (data[-1] != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid repetition/definition levels for arrays in column {}", column_info.name);
    }

    if (subchunk.null_map && !column_info.output_nullable && !column_info.group_nullable && !options.format.null_as_default)
    {
        const auto & null_map = assert_cast<const ColumnUInt8 &>(*subchunk.null_map).getData();
        /// null_map uses standard ClickHouse convention: 1 = NULL, 0 = NOT NULL.
        /// Check if any values are null — those can't be inserted into a non-Nullable column.
        if (memchr(null_map.data(), 1, null_map.size()) != nullptr)
            throw Exception(ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN, "Cannot convert NULL value to non-Nullable type for column {}", column_info.name);
        subchunk.null_map = nullptr;
    }

    if (subchunk.null_map)
    {
        const auto & null_map = assert_cast<const ColumnUInt8 &>(*subchunk.null_map).getData();
        /// Fill defaults at null rows so the column reaches full size. For a group_nullable leaf,
        /// the null map is the group null map: defaults fill the struct-null rows.
        subchunk.column->expand(null_map, /*inverted*/ true);
    }

    if (column_info.group_nullable && subchunk.null_map)
    {
        /// Leaf of a physically-nullable struct read as Nullable(Tuple(...)): its def-level null map
        /// is the group null map. Move it aside now, before the output_nullable block below can
        /// consume `null_map` into a leaf-level ColumnNullable. formOutputColumn reads it from the
        /// group's first leaf to wrap the assembled ColumnTuple in ColumnNullable. If the leaf is
        /// itself Nullable, it gets a fresh all-non-null map below (the file leaf is REQUIRED, so it
        /// has no element-level nulls; the struct nulls are represented by the outer ColumnNullable).
        subchunk.group_null_map = std::move(subchunk.null_map);
        subchunk.null_map.reset();
    }

    if (subchunk.arrays_offsets.empty() && subchunk.column->size() != row_subgroup.filter.rows_pass)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected number of rows in column subchunk {} {}", subchunk.column->size(), row_subgroup.filter.rows_pass);

    if (column_info.output_nullable)
    {
        if (!subchunk.null_map)
            subchunk.null_map = ColumnUInt8::create(subchunk.column->size(), false);
        subchunk.column = ColumnNullable::create(std::move(subchunk.column), std::move(subchunk.null_map));
        subchunk.null_map.reset();
    }

    chassert(subchunk.column->getDataType() == column_info.output_type->getColumnType());

    OutputColumnState & state = row_subgroup.output.at(column_info.idx_in_output_block);
    chassert(!state.column);
    size_t prev_count = state.primitive_columns_remaining.fetch_sub(1);
    chassert(prev_count > 0);
    if (prev_count == 1)
    {
        const auto & output_idx = sample_block_to_output_columns_idx.at(column_info.idx_in_output_block);
        state.column = formOutputColumn(row_subgroup, output_idx.value(), row_subgroup.filter.rows_pass);
    }
}

void Reader::skipToRowOrNextPage(std::optional<size_t> row_idx, ColumnChunk & column, const PrimitiveColumnInfo & column_info)
{
    /// True if column.page is initialized and contains the requested row_idx.
    bool found_page = false;
    auto & page = column.page;

    if (!row_idx.has_value())
        chassert(page.initialized);

    if (row_idx.has_value() && page.initialized && page.value_idx < page.num_values &&
        page.end_row_idx.has_value() && *page.end_row_idx > *row_idx)
        /// Fast path: we're just continuing reading the same page as before.
        found_page = true;

    if (!found_page && !column.data_pages.empty())
    {
        /// If we have offset index, find the row index there and jump to the correct page.
        if (!row_idx.has_value())
            row_idx = column.data_pages[column.data_pages_idx].end_row_idx;
        while (column.data_pages_idx < column.data_pages.size() &&
               column.data_pages[column.data_pages_idx].end_row_idx <= *row_idx)
            ++column.data_pages_idx;
        if (column.data_pages_idx == column.data_pages.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet offset index covers too few rows");
        const auto & page_info = column.data_pages[column.data_pages_idx];
        size_t first_row_idx = size_t(page_info.meta->first_row_index);
        if (first_row_idx > *row_idx)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Row passes filters but its page was not selected for reading. This is a bug.");

        auto data = prefetcher.getRangeData(page_info.prefetch);
        const char * ptr = data.data();
        if (!initializeDataPage(ptr, ptr + data.size(), first_row_idx, page_info.end_row_idx, *row_idx, column, column_info))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Page doesn't contain requested row");
        found_page = true;
    }

    while (true)
    {
        /// Skip rows inside the page.
        if (row_idx.has_value() && page.initialized && page.value_idx < page.num_values &&
            skipRowsInPage(*row_idx, page, column, column_info))
            return;

        if (found_page)
            /// This was supposed to be the correct page.
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected end of page");

        /// Advance to the next page.
        chassert(column.data_pages.empty());
        auto all_pages = prefetcher.getRangeData(column.data_pages_prefetch);
        chassert(column.next_page_offset <= all_pages.size());
        const char * ptr = all_pages.data() + column.next_page_offset;
        const char * end = all_pages.data() + all_pages.size();
        initializeDataPage(ptr, end, page.next_row_idx, /*end_row_idx=*/ std::nullopt, row_idx.value_or(page.next_row_idx), column, column_info);
        column.next_page_offset = ptr - all_pages.data();
        if (!row_idx.has_value())
            return;
    }
}

std::tuple<parq::PageHeader, std::span<const char>> Reader::decodeAndCheckPageHeader(const char * & data_ptr, const char * data_end) const
{
    parq::PageHeader header;
    data_ptr += deserializeThriftStruct(header, data_ptr, data_end - data_ptr);

    /// Validate enum fields before anything loads them (a malformed file can carry out-of-range
    /// values, and loading an unscoped enum out of range is undefined behavior).
    checkThriftEnum(header.type, parq::_PageType_VALUES_TO_NAMES, "page type");
    switch (header.type)
    {
        case parq::PageType::DATA_PAGE:
            checkThriftEnum(header.data_page_header.encoding, parq::_Encoding_VALUES_TO_NAMES, "encoding");
            checkThriftEnum(header.data_page_header.definition_level_encoding, parq::_Encoding_VALUES_TO_NAMES, "definition level encoding");
            checkThriftEnum(header.data_page_header.repetition_level_encoding, parq::_Encoding_VALUES_TO_NAMES, "repetition level encoding");
            break;
        case parq::PageType::DATA_PAGE_V2:
            checkThriftEnum(header.data_page_header_v2.encoding, parq::_Encoding_VALUES_TO_NAMES, "encoding");
            break;
        case parq::PageType::DICTIONARY_PAGE:
            checkThriftEnum(header.dictionary_page_header.encoding, parq::_Encoding_VALUES_TO_NAMES, "encoding");
            break;
        default:
            break;
    }

    size_t compressed_page_size = size_t(header.compressed_page_size);
    if (header.compressed_page_size < 0 || compressed_page_size > size_t(data_end - data_ptr))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Page size out of bounds: {} > {}", header.compressed_page_size, data_end - data_ptr);

    std::span page_data(data_ptr, compressed_page_size);
    data_ptr += compressed_page_size;

    if (header.__isset.crc && options.format.parquet.verify_checksums)
    {
        uint32_t crc = arrow::internal::crc32(0, page_data.data(), page_data.size());
        if (crc != uint32_t(header.crc))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Page CRC checksum verification failed");
    }

    return {header, page_data};
}

bool Reader::initializeDataPage(const char * & data_ptr, const char * data_end, size_t next_row_idx, std::optional<size_t> end_row_idx, size_t target_row_idx, ColumnChunk & column, const PrimitiveColumnInfo & column_info)
{
    PageState & page = column.page;
    /// We reuse PageState instance across pages to reuse memory in buffers like decompressed_buf.
    page.initialized = false;
    page.decoder.reset();
    page.decompressed_buf.clear();
    page.rep.clear();
    page.def.clear();
    page.value_idx = 0;

    page.next_row_idx = next_row_idx;
    page.end_row_idx = end_row_idx;

    /// Decode page header.

    parq::PageHeader header;
    std::tie(header, page.data) = decodeAndCheckPageHeader(data_ptr, data_end);

    /// Check if all rows of the page are filtered out, if we have enough information.

    /// These signed i32 row counts are consumed below to compute `page.end_row_idx` (and the
    /// row-skip shortcut returns before the later num_values check). A negative value would
    /// sign-extend to a huge size_t and wrap `next_row_idx + num_rows`, moving the row cursor
    /// backwards or skipping the page instead of failing, so reject it here.
    std::optional<size_t> num_rows_in_page;
    if (header.type == parq::PageType::DATA_PAGE_V2)
    {
        if (header.data_page_header_v2.num_rows < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Negative number of rows in DataPageV2");
        num_rows_in_page = header.data_page_header_v2.num_rows;
    }
    else if (header.type == parq::PageType::DATA_PAGE &&
             column_info.levels.back().rep == 0) // no arrays => num_values == num_rows
    {
        if (header.data_page_header.num_values < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Negative number of values in data page");
        num_rows_in_page = header.data_page_header.num_values;
    }

    if (num_rows_in_page.has_value())
    {
        if (end_row_idx.has_value() && *end_row_idx - next_row_idx != *num_rows_in_page)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Number of rows in page header doesn't match offset index: {} != {}", *num_rows_in_page, *end_row_idx - next_row_idx);

        page.end_row_idx = next_row_idx + *num_rows_in_page;

        if (*page.end_row_idx <= target_row_idx)
        {
            page.next_row_idx = *page.end_row_idx;
            return false;
        }
    }

    /// Get information about page layout and encoding out of page header.

    checkThriftEnum(column.meta->meta_data.codec, parq::_CompressionCodec_VALUES_TO_NAMES, "compression codec");
    page.codec = column.meta->meta_data.codec;
    /// Signed i32 from the thrift header; a negative value would sign-extend to a huge size_t and
    /// later cause a huge allocation in decompressPageIfCompressed.
    if (header.uncompressed_page_size < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Negative uncompressed page size");
    page.values_uncompressed_size = size_t(header.uncompressed_page_size);

    if (page.codec == parq::CompressionCodec::UNCOMPRESSED && header.uncompressed_page_size != header.compressed_page_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compression, but compressed and uncompressed page size are different");

    const char * encoded_rep = nullptr; // uncompressed
    const char * encoded_def = nullptr; // uncompressed
    size_t encoded_rep_size = 0;
    size_t encoded_def_size = 0;
    parq::Encoding::type def_encoding = parq::Encoding::RLE;
    parq::Encoding::type rep_encoding = parq::Encoding::RLE;

    if (header.type == parq::PageType::DATA_PAGE)
    {
        if (header.data_page_header.num_values < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Negative number of values in data page");
        page.num_values = size_t(header.data_page_header.num_values);
        page.encoding = header.data_page_header.encoding;
        def_encoding = header.data_page_header.definition_level_encoding;
        rep_encoding = header.data_page_header.repetition_level_encoding;

        if (column_info.levels.size() == 1)
        {
            /// No rep/def levels, the whole page is values.
        }
        else
        {
            /// Rep/def levels and values are compressed together. Decompress and split.
            /// Format (lengths are 4 bytes):
            /// <def length> <def> [<rep length> <rep>] <values>
            decompressPageIfCompressed(page);

            UInt32 n = 0;
            if (column_info.levels.back().rep > 0)
            {
                if (page.data.size() < 4)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Decompressed data is too short (rep size)");
                memcpy(&n, page.data.data(), 4);
                if (n > page.data.size() - 4)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Decompressed data is too short (rep)");
                encoded_rep = page.data.data() + 4;
                encoded_rep_size = n;
                page.data = page.data.subspan(4 + n);
            }

            if (page.data.size() < 4)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Decompressed data is too short (def size)");
            memcpy(&n, page.data.data(), 4);
            if (n > page.data.size() - 4)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Decompressed data is too short (def)");
            encoded_def = page.data.data() + 4;
            encoded_def_size = n;
            page.data = page.data.subspan(4 + n);
        }
    }
    else if (header.type == parq::PageType::DATA_PAGE_V2)
    {
        if (header.data_page_header_v2.num_values < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Negative number of values in data page");
        page.num_values = size_t(header.data_page_header_v2.num_values);
        page.encoding = header.data_page_header_v2.encoding;

        /// These come from the thrift header as signed i32. A negative value would sign-extend to a
        /// huge size_t, so reject it before assigning to the size_t fields. Otherwise the bounds
        /// check below could be bypassed by integer overflow, and a huge std::span would reach the
        /// rep/def level decoder, reading far past the page buffer (heap out-of-bounds read).
        if (header.data_page_header_v2.definition_levels_byte_length < 0 ||
            header.data_page_header_v2.repetition_levels_byte_length < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Negative definition/repetition levels byte length in DataPageV2");
        encoded_def_size = size_t(header.data_page_header_v2.definition_levels_byte_length);
        encoded_rep_size = size_t(header.data_page_header_v2.repetition_levels_byte_length);

        if (header.data_page_header_v2.__isset.is_compressed &&
            !header.data_page_header_v2.is_compressed)
        {
            page.codec = parq::CompressionCodec::UNCOMPRESSED;
        }

        /// Non-wrapping bounds check: `encoded_def_size + encoded_rep_size` could overflow.
        if (encoded_rep_size > page.data.size() || encoded_def_size > page.data.size() - encoded_rep_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Page data is too short (def+rep)");
        encoded_rep = page.data.data();
        encoded_def = page.data.data() + encoded_rep_size;
        size_t uncompressed_part = encoded_def_size + encoded_rep_size;
        page.data = page.data.subspan(uncompressed_part);
        if (page.values_uncompressed_size < uncompressed_part)
            throw Exception(ErrorCodes::INCORRECT_DATA, "DataPageV2 uncompressed page size is smaller than the rep/def levels");
        page.values_uncompressed_size -= uncompressed_part;
    }
    else if (header.type == parq::PageType::DICTIONARY_PAGE)
    {
        if (column.dictionary.isInitialized())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Column chunk has multiple dictionary pages or inaccurate data_page_offset");

        /// There's a dictionary page, but there was no dictionary_page_offset in ColumnMetaData.
        /// This is probably not allowed, but we have to support it because some writers wrote such
        /// files, see comment in readFileMetaData.
        decodeDictionaryPageImpl(header, page.data, column, column_info);
        return false;
    }
    else if (header.type == parq::PageType::INDEX_PAGE)
    {
        /// Skip index page quietly, although it's probably not expected amid data pages.
        /// (This page type is currently unused in parquet.)
        return false;
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected page type: {}", thriftToString(header));
    }

    if (page.encoding == parq::Encoding::PLAIN_DICTIONARY)
        page.encoding = parq::Encoding::RLE_DICTIONARY;

    page.is_dictionary_encoded = page.encoding == parq::Encoding::RLE_DICTIONARY;
    if (page.is_dictionary_encoded && !column.dictionary.isInitialized())
    {
        if (column.meta->meta_data.__isset.dictionary_page_offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary not initialized");
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected dictionary-encoded page in column chunk with no dictionary");
    }

    /// Decode rep/def levels.

    UInt8 max_def = column_info.levels.back().def;
    UInt8 max_rep = column_info.levels.back().rep;

    decodeRepOrDefLevels(rep_encoding, max_rep, page.num_values, std::span(encoded_rep, encoded_rep_size), page.rep);

    /// Don't decode def levels in the common case of non-array column that's declared nullable but
    /// contains no nulls.
    if (max_rep > 0 || column.need_null_map)
        decodeRepOrDefLevels(def_encoding, max_def, page.num_values, std::span(encoded_def, encoded_def_size), page.def);

    ProfileEvents::increment(ProfileEvents::ParquetReadPages);
    page.initialized = true;
    return true;
}

/// Advances page.{value_idx, next_row_idx}. The caller must advance page.data (encoded values).
static void advanceValueIdxUntilRow(size_t end_row_idx, Reader::PageState & page)
{
    size_t new_value_idx = page.value_idx;
    if (page.rep.empty())
    {
        new_value_idx = std::min(page.num_values, page.value_idx + (end_row_idx - page.next_row_idx));
        page.next_row_idx += new_value_idx - page.value_idx;
    }
    else
    {
        while (new_value_idx < page.num_values)
        {
            if (page.rep[new_value_idx] == 0)
            {
                if (page.next_row_idx == end_row_idx)
                    break;
                page.next_row_idx += 1;
            }
            new_value_idx += 1;
        }
    }
    page.value_idx = new_value_idx;
}

void Reader::createPageDecoder(PageState & page, ColumnChunk & column, const PrimitiveColumnInfo & column_info)
{
    if (page.is_dictionary_encoded)
        page.decoder = makeDictionaryIndicesDecoder(page.encoding, column.dictionary.count, page.data);
    else
        page.decoder = column_info.decoder.makeDecoder(page.encoding, page.data);
}

/// Returns true if this row is found in this page, and value_idx is at the first value of this row.
/// False if we reached the end of the page without reaching this row index; next_row_idx is set
/// accordingly.
bool Reader::skipRowsInPage(size_t target_row_idx, PageState & page, ColumnChunk & column, const PrimitiveColumnInfo & column_info)
{
    chassert(target_row_idx >= page.next_row_idx);

    size_t prev_value_idx = page.value_idx;
    advanceValueIdxUntilRow(target_row_idx, page);

    if (page.value_idx == page.num_values)
    {
        page.decoder.reset();
        return false;
    }

    size_t encoded_values_to_skip = 0;
    if (page.def.empty())
    {
        encoded_values_to_skip = page.value_idx - prev_value_idx;
    }
    else
    {
        UInt8 max_def = column_info.levels.back().def;
        for (size_t i = prev_value_idx; i < page.value_idx; ++i)
            encoded_values_to_skip += page.def[i] == max_def;
    }

    if (encoded_values_to_skip > 0)
    {
        decompressPageIfCompressed(page);
        if (!page.decoder)
            createPageDecoder(page, column, column_info);
        page.decoder->skip(encoded_values_to_skip);
    }

    return true;
}

/// *** Rep/def level explanation ***
///
/// Functions for interpreting repetition and definition levels. This is tricky.
/// The levels have information about nulls, array lengths, and row boundaries.
///
/// Context: at this stage we're looking at one primitive parquet column.
/// (Things like tuples and maps are assembled out of such columns separately.)
///
/// ClickHouse type looks like e.g. Array(Array(Nullable(String)),
/// i.e. 0+ Array-s, then maybe Nullable, then primitive column.
/// Represented as a primitive IColumn + null mask + array offsets for each Array level.
///
/// Parquet type looks like e.g. Array(Nullable(Nullable(Array(Nullable(Nullable(String)))))),
/// i.e. any sequence of Array-s and Nullable-s.
/// Represented as definition and repetition levels + encoded non-null primitive values.
///
/// We have to convert levels to array offsets and null map, while ignoring nullables in
/// places clickhouse doesn't support (nullable arrays and nullable nullables).
///
/// Concepts:
///  * "Value" is an element in logical rep/def levels arrays (the two arrays are parallel).
///    (If max_def or max_rep is 0, the corresponding array is not stored, but logically it's
///     an array of zeroes.)
///    See below for what values can represent.
///  * "Encoded value" is a non-null primitive value actually stored in the parquet file.
///    Corresponds to non-null element of innermost array.
///  * "Row" is a row in the table. Corresponds to a range of values.
///    (If not array, exactly one value.)
///
/// Values (aka elements of definition levels array) fall in 3 categories:
///  * Non-null element of innermost array: def[i] == max_def.
///    Corresponds to an encoded value.
///    null_map->push_back(0).
///  * Null element of innermost array: max_array_def <= def[i] < max_def.
///    No encoded value. A default value needs to be inserted into IColumn.
///    null_map->push_back(1).
///  * Empty array or null array: def[i] < max_array_def.
///    No encoded value, no IColumn or null_map element.
///
/// rep[i] == k indicates start of a new array element for the array at level k (<= max_rep).
/// rep[i] == 0 indicates first value of a new row (column chunk can be seen as array of rows).
///
/// A row may have values in multiple pages (unless DataPage V2 is used or offset index is present).
///
/// With all of that in mind, for a given page we have to produce:
///  * null_map, as described above.
///  * num_encoded_values - just count def[i] == max_def.
///  * Array offsets for each array level (rep = 1..max_rep).
///    (Array may have elements in multiple pages.)
///  * Advance value_idx and next_row_idx by correct amounts, keeping them in sync.
template <bool has_arrays, bool has_nulls>
static void processDefLevelsForInnermostColumn(
    size_t num_values, const UInt8 * def, UInt8 max_def, UInt8 max_array_def, size_t & out_num_encoded_values, ColumnUInt8::Container * out_null_map)
{
    size_t num_encoded_values = 0;
    for (size_t i = 0; i < num_values; ++i)
    {
        if constexpr (has_arrays)
            if (def[i] < max_array_def)
                continue; // empty array

        bool is_null = false;
        if constexpr (has_nulls)
        {
            is_null = def[i] != max_def;
            out_null_map->push_back(is_null);
        }

        num_encoded_values += !is_null;
    }
    out_num_encoded_values = num_encoded_values;
}

/// Produces array offsets at a given level of nested arrays.
/// TODO [parquet]: Try simdifying.
///
/// Instead of calling this for array_rep = 1..max_rep, we could probably process all array levels
/// in one loop over rep/def levels (doing something like arrays_offsets[rep[i]].push_back(...)).
/// But I expect it would be slower because (a) simd would be less effective (especially after we
/// simdify this implementation), (b) usually there's only one level of arrays.
static void processRepDefLevelsForArray(
    size_t num_values, const UInt8 * def, const UInt8 * rep, UInt8 array_rep, UInt8 array_def,
    UInt8 parent_array_def, PaddedPODArray<UInt64> & out_offsets)
{
    UInt64 offset = out_offsets.back(); // may take -1-st element, PaddedPODArray allows that
    for (size_t i = 0; i < num_values; ++i)
    {
        if (def[i] < parent_array_def)
            /// Some ancestor is null or empty array.
            /// In particular:
            ///  * `def[i] == array_def - 1` means this array is empty,
            ///  * `parent_array_def <= def[i] < array_def - 1` means this array is null,
            ///    which we convert to empty array because clickhouse doesn't support nullable arrays.
            ///    TODO [parquet]: Should we throw an error in this case if !options.format.null_as_default?
            continue;

        if (rep[i] < array_rep)
        {
            /// Previous array instance ended and a new array instance started.

            /// May assign -1-st element, but normally only sets it to 0; if we set it to nonzero
            /// because of invalid rep levels, the caller will notice and throw.
            out_offsets.back() = offset;
            out_offsets.resize(out_offsets.size() + 1);
        }

        offset += rep[i] <= array_rep && def[i] >= array_def;
    }
    /// Note that the array may continue in the next page. In that case the next call to this
    /// function will read this offset back, add to it, and assign it again.
    out_offsets.back() = offset;
}

void Reader::readRowsInPage(size_t end_row_idx, ColumnSubchunk & subchunk, ColumnChunk & column, const PrimitiveColumnInfo & column_info, const RowSubgroup * row_subgroup)
{
    PageState & page = column.page;
    chassert(page.initialized && page.value_idx < page.num_values);

    /// Note: end_row_idx == page.next_row_idx doesn't necessarily mean we're done. E.g. suppose the
    /// row end_row_idx-1 contains an array that starts in page 0 and ends inside page 1.
    /// readRowsInPage in page 0 will reach end of page, with next_row_idx == end_row_idx. Then
    /// readRowsInPage in page 1 will continue until it sees the end of the array, i.e. the start of
    /// the next row (rep == 0), still with next_row_idx == end_row_idx.
    chassert(end_row_idx >= page.next_row_idx);

    size_t first_row_idx = page.next_row_idx;

    /// Convert number of rows to number of values.
    size_t prev_value_idx = page.value_idx;
    advanceValueIdxUntilRow(end_row_idx, page);

    /// Produce array offsets.
    if (!page.rep.empty())
    {
        UInt8 parent_array_def = 0;
        for (size_t level_idx = 1; level_idx < column_info.levels.size(); ++level_idx)
        {
            const LevelInfo & level = column_info.levels[level_idx];
            if (!level.is_array)
                continue;

            auto & offsets = assert_cast<ColumnArray::ColumnOffsets &>(*subchunk.arrays_offsets.at(level.rep - 1)).getData();
            processRepDefLevelsForArray(
                page.value_idx - prev_value_idx, page.def.data() + prev_value_idx,
                page.rep.data() + prev_value_idx, level.rep, level.def, parent_array_def, offsets);

            parent_array_def = level.def;
        }
    }

    /// Populate null map and find how many encoded values to read.
    size_t encoded_values_to_read = 0;
    if (page.def.empty())
    {
        /// No nulls or arrays in this page.
        encoded_values_to_read = page.value_idx - prev_value_idx;
    }
    else
    {
        /// Dispatch to a version of the hot loop with unneeded features disabled.
#define X(has_arrays, has_nulls, null_map) \
            processDefLevelsForInnermostColumn<has_arrays, has_nulls>( \
                page.value_idx - prev_value_idx, page.def.data() + prev_value_idx, \
                column_info.levels.back().def, column_info.max_array_def, encoded_values_to_read, \
                null_map)
        if (subchunk.null_map)
        {
            auto & null_map = assert_cast<ColumnUInt8 &>(*subchunk.null_map).getData();
            if (column_info.max_array_def)
                X(true, true, &null_map);
            else
                X(false, true, &null_map);
        }
        else
        {
            if (column_info.max_array_def)
                X(true, false, nullptr);
            else
                X(false, false, nullptr);
        }
    }

    /// Decode values.

    /// See if we can decompress the whole page directly into IColumn's memory.
    /// Skip when filter is set: direct read bypasses decode and would write all values without applying the filter.
    const bool has_filter = row_subgroup && !row_subgroup->filter.filter.empty();
    if (!has_filter && !page.is_dictionary_encoded && prev_value_idx == 0 && page.value_idx == page.num_values &&
        page.codec != parq::CompressionCodec::UNCOMPRESSED)
    {
        std::span<char> span;
        if (column_info.decoder.canReadDirectlyIntoColumn(page.encoding, encoded_values_to_read, *subchunk.column, span))
        {
            if (span.size() != page.values_uncompressed_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected uncompressed page size");
            decompress(page.data.data(), page.data.size(), span.size(), page.codec, span.data());
            return;
        }
    }

    if (encoded_values_to_read > 0)
    {
        decompressPageIfCompressed(page);
        if (!page.decoder)
            createPageDecoder(page, column, column_info);

        const UInt8 * filter = nullptr;
        size_t filter_offset = 0;
        if (row_subgroup && !row_subgroup->filter.filter.empty())
        {
            chassert(first_row_idx >= row_subgroup->start_row_idx);
            filter_offset = first_row_idx - row_subgroup->start_row_idx;
            filter = row_subgroup->filter.filter.data();
        }

        if (page.is_dictionary_encoded)
        {
            if (!page.indices_column)
                page.indices_column = ColumnUInt32::create();
            auto & indices_column_uint32 = assert_cast<ColumnUInt32 &>(*page.indices_column);
            auto & data = indices_column_uint32.getData();
            chassert(data.empty());
            chassert(!filter);
            page.decoder->decode(encoded_values_to_read, *page.indices_column, nullptr, 0);
            column.dictionary.index(indices_column_uint32, *subchunk.column);
            data.clear();
        }
        else
        {
            page.decoder->decode(encoded_values_to_read, *subchunk.column, filter, filter_offset);
        }
    }

    if (page.value_idx == page.num_values)
        page.decoder.reset();
}

void Reader::decompressPageIfCompressed(PageState & page)
{
    if (page.codec == parq::CompressionCodec::UNCOMPRESSED)
        return;
    page.decompressed_buf.resize(page.values_uncompressed_size);
    decompress(page.data.data(), page.data.size(), page.decompressed_buf.size(), page.codec, page.decompressed_buf.data());
    page.data = std::span(page.decompressed_buf.data(), page.decompressed_buf.size());
    page.codec = parq::CompressionCodec::UNCOMPRESSED;
}

MutableColumnPtr Reader::formOutputColumn(RowSubgroup & row_subgroup, size_t output_column_idx, size_t num_rows)
{
    /// Recurses over the nested output column tree, whose depth is bounded by SchemaConverter's
    /// recursion limit; guard the native stack here too as defense in depth.
    checkStackSize();

    const OutputColumnInfo & output_info = output_columns.at(output_column_idx);
    MutableColumnPtr res;

    if (output_info.is_missing_column)
    {
        res = output_info.output_type->createColumn();
        res->insertManyDefaults(num_rows);

        if (output_info.idx_in_output_block.has_value() &&
            /// If block_missing_values is enabled (not empty), and this column is not prewhere-only
            /// (idx < sample_block->columns()).
            *output_info.idx_in_output_block < row_subgroup.block_missing_values.getNumColumns())
        {
            row_subgroup.block_missing_values.setBits(*output_info.idx_in_output_block, num_rows);
        }

        return res;
    }

    /// Physically-nullable struct read as Nullable(Tuple(...)). input_type is Nullable(Tuple), but
    /// we assemble the inner ColumnTuple from the leaves and then wrap it in ColumnNullable using
    /// the group null map. Every leaf shares the same def-level null map (the subtree is
    /// all-REQUIRED), which decodePrimitiveColumn moved into `group_null_map` on each leaf before
    /// any leaf-level Nullable wrapping could consume it. Take it from the first leaf. Dispatch on
    /// the unwrapped type.
    MutableColumnPtr nullable_group_null_map;
    if (output_info.nullable_group)
    {
        ColumnSubchunk & first_leaf = row_subgroup.columns.at(output_info.primitive_start);
        if (first_leaf.group_null_map)
            nullable_group_null_map = IColumn::mutate(std::move(first_leaf.group_null_map));
        else
            /// No struct-level nulls (all rows defined): all-non-null map.
            nullable_group_null_map = ColumnUInt8::create(num_rows, UInt8(0));
    }

    TypeIndex kind = output_info.nullable_group
        ? removeNullable(output_info.input_type)->getColumnType()
        : output_info.input_type->getColumnType();

    if (output_info.is_primitive)
    {
        /// Primitive column.
        chassert(output_info.primitive_start + 1 == output_info.primitive_end);
        size_t primitive_idx = output_info.primitive_start;
        ColumnSubchunk & subchunk = row_subgroup.columns.at(primitive_idx);
        res = std::move(subchunk.column);

        if (output_info.idx_in_output_block.has_value() &&
            *output_info.idx_in_output_block < row_subgroup.block_missing_values.getNumColumns() &&
            subchunk.null_map)
        {
            const auto & null_map = assert_cast<const ColumnUInt8 &>(*subchunk.null_map.get()).getData();
            row_subgroup.block_missing_values.setBitsFromNullMap(*output_info.idx_in_output_block, null_map);
        }
        subchunk.null_map.reset();
    }
    else if (kind == TypeIndex::Array)
    {
        chassert(output_info.nested_columns.size() == 1);
        MutableColumnPtr offsets_column;
        if (output_info.primitive_start < output_info.primitive_end)
            offsets_column = std::move(row_subgroup.columns.at(output_info.primitive_start).arrays_offsets.at(output_info.rep - 1));
        else
            /// All subcolumns inside the Array are missing. E.g. Array(Tuple(nonexistent_column Int64)).
            offsets_column = ColumnUInt64::create(num_rows, 0);

        /// If it's an array of tuples, every tuple element should have the same array offsets.
        const auto & offsets = assert_cast<const ColumnUInt64 &>(*offsets_column).getData();
        for (size_t i = output_info.primitive_start + 1; i < output_info.primitive_end; ++i)
        {
            const auto other_offsets_column = std::move(row_subgroup.columns.at(i).arrays_offsets.at(output_info.rep - 1));
            const auto & other_offsets = assert_cast<const ColumnUInt64 &>(*other_offsets_column).getData();
            if (offsets != other_offsets)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid array of tuples: tuple elements {} and {} have different array lengths", primitive_columns.at(output_info.primitive_start).name, primitive_columns.at(i).name);
        }

        MutableColumnPtr nested = formOutputColumn(row_subgroup, output_info.nested_columns.at(0), offsets.back());
        res = ColumnArray::create(std::move(nested), std::move(offsets_column));
    }
    else if (kind == TypeIndex::Tuple)
    {
        MutableColumns columns;
        for (size_t idx : output_info.nested_columns)
            columns.push_back(formOutputColumn(row_subgroup, idx, num_rows));
        if (columns.empty())
            res = ColumnTuple::create(num_rows);
        else
            res = ColumnTuple::create(std::move(columns));
    }
    else
    {
        chassert(kind == TypeIndex::Map);
        chassert(output_info.nested_columns.size() == 1);
        MutableColumnPtr nested = formOutputColumn(row_subgroup, output_info.nested_columns.at(0), num_rows);
        res = ColumnMap::create(std::move(nested));
    }

    if (output_info.nullable_group)
    {
        /// Wrap the assembled ColumnTuple in ColumnNullable using the reconstructed group null map.
        chassert(nullable_group_null_map->size() == res->size());
        res = ColumnNullable::create(std::move(res), std::move(nullable_group_null_map));
    }

    chassert(res->getDataType() == output_info.input_type->getColumnType());

    if (output_info.needs_cast)
    {
        auto col = castColumn(
            {std::move(res), output_info.input_type, output_info.name}, output_info.output_type);
        chassert(col->use_count() == 1);
        res = IColumn::mutate(std::move(col));
    }

    return res;
}

ColumnPtr & Reader::getOrFormOutputColumn(RowSubgroup & row_subgroup, size_t idx_in_output_block)
{
    chassert(row_subgroup.filter.rows_pass > 0);
    const auto & output_idx = sample_block_to_output_columns_idx.at(idx_in_output_block);
    OutputColumnState & state = row_subgroup.output.at(idx_in_output_block);
    chassert(state.primitive_columns_remaining.load() == 0);
    if (output_idx.has_value())
    {
        const auto & info = output_columns[*output_idx];
        /// Normally output column is formed by decodePrimitiveColumn. But if the column is missing
        /// in the file, and we're returning default values, we form it here, i.e. during prewhere or delivery.
        chassert(state.column || (info.primitive_start == info.primitive_end));
        if (!state.column)
            state.column = formOutputColumn(row_subgroup, *output_idx, row_subgroup.filter.rows_pass);
    }
    chassert(state.column);
    chassert(state.column->size() == row_subgroup.filter.rows_pass);
    return state.column;
}

void Reader::applyPrewhere(RowSubgroup & row_subgroup, const RowGroup & row_group, size_t step_idx)
{
    {
        const Step & step = steps.at(step_idx - 1);

        Block block;
        for (size_t idx_in_output_block : step.input_idxs)
        {
            const ColumnWithTypeAndName & col = extended_sample_block.getByPosition(idx_in_output_block);
            block.insert({getOrFormOutputColumn(row_subgroup, idx_in_output_block), col.type, col.name});
        }
        addDummyColumnWithRowCount(block, row_subgroup.filter.rows_pass);

        ProfileEvents::increment(ProfileEvents::ParquetRowsFilterExpression, block.rows());
        ProfileEvents::increment(ProfileEvents::ParquetColumnsFilterExpression, block.columns());

        if (block.rows() == 0)
        {
            row_subgroup.filter.rows_pass = 0;
            return;
        }
        step.actions.execute(block);

        for (const auto & [name, idx] : step.idxs_in_output_block)
        {
            OutputColumnState & state = row_subgroup.output.at(idx);
            chassert(!state.column);
            state.column = block.getByName(name).column;
        }

        /// If it's the last prewhere step, deallocate the columns that were only needed for prewhere.
        if (step_idx == steps.size())
        {
            while (row_subgroup.output.size() > sample_block->columns())
                row_subgroup.output.pop_back(); // because OutputColumnState has no move constructor
        }

        if (!step.filter_column_name.has_value())
            return;

        ColumnPtr filter_column = block.getByName(step.filter_column_name.value()).column;
        filter_column = FilterDescription::preprocessFilterColumn(std::move(filter_column));
        const IColumnFilter & filter = typeid_cast<const ColumnUInt8 &>(*filter_column).getData();
        chassert(filter.size() == row_subgroup.filter.rows_pass);
        size_t rows_pass = countBytesInFilter(filter.data(), 0, filter.size());
        if (rows_pass == 0 || !row_group.need_to_process)
        {
            /// Whole row group was filtered out.
            row_subgroup.filter.rows_pass = 0;
            return;
        }

        /// Filter columns that were already read.
        for (auto & state : row_subgroup.output)
            if (state.column)
                state.column = state.column->filter(filter, /*result_size_hint=*/ rows_pass);

        /// Expand the filter to correspond to all column subchunk rows, rather than only rows that
        /// passed previous filters (previous prewhere steps).
        auto mut_col = IColumn::mutate(std::move(filter_column));
        auto & mut_filter = typeid_cast<ColumnUInt8 &>(*mut_col);
        if (row_subgroup.filter.rows_pass != row_subgroup.filter.rows_total)
            mut_filter.expand(row_subgroup.filter.filter, /*inverted*/ false);
        row_subgroup.filter.filter = std::move(mut_filter.getData());
        row_subgroup.filter.rows_pass = rows_pass;
    }
}

Reader::PrimitiveColumnInfo::~PrimitiveColumnInfo() = default;

}
