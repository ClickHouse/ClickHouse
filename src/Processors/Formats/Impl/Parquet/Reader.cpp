#include <Processors/Formats/Impl/Parquet/Reader.h>
#include <Processors/Formats/Impl/Parquet/Decoding.h>
#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/castColumn.h>
#include <lz4.h>
#if USE_SNAPPY
#include <snappy.h>
#endif

namespace DB::ErrorCodes
{
    extern const int CANNOT_DECOMPRESS;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
}

namespace DB::Parquet
{

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
            size_t actual_uncompressed_size;
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
            method = CompressionMethod::Gzip;
            break;
        case parq::CompressionCodec::LZO:
            /// Arrow also doesn't support it.
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "LZO decompression is not supported");
        case parq::CompressionCodec::BROTLI:
            method = CompressionMethod::Brotli;
            break;
        case parq::CompressionCodec::LZ4:
            /// LZ4 framed. In parquet it's deprecated in favor of LZ4_RAW.
            method = CompressionMethod::Lz4;
            break;
        case parq::CompressionCodec::ZSTD:
            method = CompressionMethod::Zstd;
            break;
        case parq::CompressionCodec::LZ4_RAW:
        {
            /// LZ4 block.
            if (compressed_size > INT32_MAX || uncompressed_size > INT32_MAX)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Compressed page is too long");
            int n = LZ4_decompress_safe(data, out, int(compressed_size), int(uncompressed_size));
            if (n < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed compressed page");
            if (size_t(n) != uncompressed_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected uncompressed page size");
            return;
        }
    }
    if (method == CompressionMethod::None)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected compression codec in parquet: {}", thriftToString(codec));

    auto mem_buf = std::make_unique<ReadBuffer>(const_cast<char *>(data), compressed_size, 0);
    std::unique_ptr<ReadBuffer> decompressor = wrapReadBufferWithCompressionMethod(
        std::move(mem_buf),
        method,
        /*zstd_window_log_max*/ 0,
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

void Reader::init(const ReadOptions & options_, const Block & sample_block_, std::shared_ptr<const KeyCondition> key_condition_)
{
    options = options_;
    sample_block = &sample_block_;
    key_condition = key_condition_;
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
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not a parquet file (wrong magic bytes at the end of file)");

    int32_t metadata_size_i32;
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

    return file_metadata;
}

size_t Reader::estimateColumnMemoryUsage(const ColumnChunk & column) const
{
    //TODO: estimate better, the encoded size may be considerably smaller because of fancy encodings
    //TODO: include arrays_offsets
    if (column.pages.empty())
    {
        return column.meta->meta_data.total_uncompressed_size;
    }
    else
    {
        size_t pages_compressed_size = 0;
        for (const auto & page : column.pages)
            pages_compressed_size += page.meta->compressed_page_size;
        return size_t(1. * column.meta->meta_data.total_uncompressed_size / column.meta->meta_data.total_compressed_size * pages_compressed_size);
    }
}

void Reader::prefilterAndInitRowGroups()
{
    if (file_metadata.row_groups.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet file has no row groups");

    SchemaConverter schemer(file_metadata, options, sample_block);
    schemer.prepareForReading();
    primitive_columns = std::move(schemer.primitive_columns);
    total_primitive_column_count = schemer.primitive_column_idx;
    output_columns = std::move(schemer.output_columns);

    if (key_condition)
    {
        //TODO: assign PrimitiveColumnInfo:: use_bloom and use_column_index; possibly:
        /// Expect that either all or none of the column chunks have indexes written.
        /// (If that's not the case, nothing breaks, we may just pick suboptimal options here.)
    }

    //TODO: fill out use_prewhere in PrimitiveColumnInfo based on SelectQueryInfo;
    //      check that prewhere involves at least one column (otherwise manager won't run it)
    /// Note that we can't disable use_prewhere here, even if there's no performance benefit to it.
    /// If the query pipeline relies on us to do PREWHERE, we must do it, otherwise the unfiltered
    /// results will be shown to the user.

    bool have_row_filtering = std::any_of(primitive_columns.begin(), primitive_columns.end(), [](const auto & c) { return c.use_column_index || c.use_prewhere; });

    for (size_t row_group_idx = 0; row_group_idx < file_metadata.row_groups.size(); ++row_group_idx)
    {
        const auto * meta = &file_metadata.row_groups[row_group_idx];
        if (meta->num_rows <= 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Row group {} has <= 0 rows: {}", row_group_idx, meta->num_rows);
        if (meta->columns.size() != total_primitive_column_count)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Row group {} has unexpected number of columns: {} != {}", row_group_idx, meta->columns.size(), total_primitive_column_count);

        //TODO: filtering
        RowGroup & row_group = row_groups.emplace_back();
        row_group.meta = meta;
        row_group.row_group_idx = row_group_idx;
        row_group.filter.rows_total = meta->num_rows;
        row_group.filter.rows_pass = meta->num_rows;
        row_group.columns.resize(primitive_columns.size());

        for (size_t column_idx = 0; column_idx < primitive_columns.size(); ++column_idx)
        {
            ColumnChunk & column = row_group.columns[column_idx];
            size_t parquet_column_idx = primitive_columns[column_idx].column_idx;
            column.meta = &meta->columns.at(parquet_column_idx);

            if (have_row_filtering &&
                column.meta->__isset.offset_index_offset && column.meta->__isset.offset_index_length &&
                column.meta->offset_index_offset >= 0 && column.meta->offset_index_length > 0)
            {
                column.offset_index_prefetch = prefetcher.registerRange(
                    size_t(column.meta->offset_index_offset),
                    size_t(column.meta->offset_index_length), /*likely_to_be_used*/ true);
            }

            if (primitive_columns[column_idx].use_bloom_filter &&
                column.meta->meta_data.__isset.bloom_filter_offset &&
                column.meta->meta_data.bloom_filter_offset >= 0)
            {
                //TODO: support missing length
                if (column.meta->meta_data.__isset.bloom_filter_length &&
                    column.meta->meta_data.bloom_filter_length > 0)
                {
                    column.bloom_filter_prefetch = prefetcher.registerRange(
                        size_t(column.meta->meta_data.bloom_filter_offset),
                        size_t(column.meta->meta_data.bloom_filter_length),
                        /*likely_to_be_used*/ true);
                }
            }

            if (primitive_columns[column_idx].use_column_index &&
                column.meta->__isset.column_index_offset && column.meta->__isset.column_index_length &&
                column.meta->column_index_offset >= 0 && column.meta->column_index_length > 0)
            {
                column.column_index_prefetch = prefetcher.registerRange(
                    size_t(column.meta->column_index_offset),
                    size_t(column.meta->column_index_length), /*likely_to_be_used*/ true);
            }

            column.data_prefetch = prefetcher.registerRange(
                column.meta->meta_data.__isset.dictionary_page_offset
                    ? column.meta->meta_data.dictionary_page_offset
                    : column.meta->meta_data.data_page_offset,
                column.meta->meta_data.total_compressed_size,
                /*likely_to_be_used*/ true);
        }
    }

    prefetcher.finalizeRanges();
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
/// i.e. 0+ Array-s, then 0-1 Nullable-s, then primitive column.
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
///  * "Value" is an element in rep/def levels arrays (the two arrays are parallel).
///    (If max_def or max_rep is 0, the corresponding array is not stored, but logically it's
///     an array of zeroes.)
///    See below for what values can represent.
///  * "Encoded value" is a non-null primitive value occupying some bytes in the parquet file.
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
/// A row may have values in multiple pages (unless DataPage V2 is used).
///
/// With all of that in mind, for a given page we have to produce:
///  * page_value_filter, based on page_row_filter.
///    page_row_filter elements correspond to *rows*, while page_value_filter elements correspond
///    to *encoded values* (def[i] == max_def). So, each element needs to be repeated some
///    number of times (possibly 0, if empty array).
///  * null_map, as described above.
///  * num_encoded_values - just count def[i] == max_def.
///  * Array offsets for each array level (rep = 1..max_rep).
///    (Array may have elements in multiple pages.)
///  * Advance row_idx from last value of previous page to last value of current page.
///    (Row may have values in multiple pages.)

/// Hot inner loop that does all rep/def stuff except array offsets.
/// The template lets us avoid doing unnecessary checks in common special cases.
/// TODO: Try simdifying, maybe the whole thing, maybe some special cases.
/// TODO: Try replacing push_back-s with direct writes by pointer, to avoid a dereference.
template <bool HAS_FILTER, bool HAS_ARRAYS, bool HAS_NULLS>
static void processRepDefLevelsForInnermostColumn(
    size_t num_values, const UInt8 * def, const UInt8 * rep, UInt8 max_def, UInt8 max_array_def,
    size_t page_num_rows, const UInt8 * page_row_filter,
    size_t & out_num_encoded_values, size_t & out_num_filtered_values,
    PaddedPODArray<UInt8> & out_page_value_filter, ColumnUInt8::Container * out_null_map)
{
    size_t rows_seen = 0;
    size_t num_encoded_values = 0;
    size_t num_filtered_values = 0;
    for (size_t i = 0; i < num_values; ++i)
    {
        if constexpr (HAS_ARRAYS)
            rows_seen += rep[i] == 0;
        else
            rows_seen = i + 1;

        if constexpr (HAS_ARRAYS)
            if (def[i] < max_array_def)
                continue;

        UInt8 passes_filter = 1;
        if constexpr (HAS_FILTER)
        passes_filter = page_row_filter[rows_seen];

        UInt8 not_null = 1;
        if constexpr (HAS_NULLS)
        {
            not_null = def[i] == max_def;

            /// (We hope that the compiler will optimize out the `if` in HAS_FILTER = 0 instantiation.
            ///  Similarly for the `if (not_null)` below.)
            if (passes_filter)
                out_null_map->push_back(!not_null);
        }

        if (not_null)
        {
            if constexpr (HAS_FILTER)
            {
                out_page_value_filter.push_back(passes_filter);
                num_filtered_values += passes_filter;
            }
            num_encoded_values += 1;
        }
    }
    chassert(rows_seen == page_num_rows);
    out_num_encoded_values = num_encoded_values;
    if constexpr (HAS_FILTER)
        out_num_filtered_values = num_filtered_values;
}

/// Produces array offsets at a given level of nested arrays.
/// TODO: Try simdifying.
///
/// Instead of calling this for array_rep = 1..max_rep, we could probably process all array levels
/// in one loop over rep/def levels (doing something like arrays_offsets[rep[i]].push_back(...)).
/// But I expect it would be slower because (a) simd would be less effective (especially after we
/// simdify this implementation), (b) usually there's only one level of arrays.
template <bool HAS_FILTER>
static void processRepDefLevelsForArray(
    size_t num_values, const UInt8 * def, const UInt8 * rep, UInt8 array_rep, UInt8 array_def,
    size_t page_num_rows, const UInt8 * page_row_filter,
    PaddedPODArray<UInt64> & out_offsets)
{
    UInt64 offset = out_offsets.back(); // may take -1-st element, PaddedPODArray allows that
    size_t rows_seen = 0;
    for (size_t i = 0; i < num_values; ++i)
    {
        if constexpr (HAS_FILTER)
        {
            rows_seen += rep[i] == 0;
            if (!page_row_filter[rows_seen])
                continue;
        }

        if (def[i] < array_def)
            /// Some ancestor is null or empty-array.
            continue;

        if (rep[i] < array_rep)
        {
            /// Previous array instance ended and a new array instance started.

            /// May assign -1-st element, but normally only sets it to 0; if we set it to nonzero
            /// because of invalid rep levels, the caller will notice and throw.
            out_offsets.back() = offset;
            out_offsets.resize(out_offsets.size() + 1);
        }

        offset += rep[i] <= array_rep;
    }
    /// Note that the array may continue in the next page. In that case the next call to this
    /// function will read this offset back, add to it, and assign it again.
    out_offsets.back() = offset;

    if constexpr (HAS_FILTER)
        chassert(rows_seen == page_num_rows);
}

void Reader::parsePrimitiveColumn(ColumnChunk & column_chunk, const PrimitiveColumnInfo & column_info, const RowSet & filter)
{
    //TODO: handle selective page reading

    MutableColumnPtr dictionary_column;
    MutableColumnPtr index_column;
    MutableColumnPtr full_column;
    std::unique_ptr<ValueDecoder> index_decoder;

    /// TODO: Instead of using ColumnLowCardinality for dictionary-encoded data, at least for strings
    ///       add a custom dictionary type to avoid a memcpy: instead of copying strings from
    ///       plain-encoded dictionary page into ColumnString, make an array of ranges pointing into
    ///       the decompressed page buffer.
    /// TODO: Instead of reading all indices into an array, have a fast path that does dictionary
    ///       lookup right in the middle of decodeBitPackedRLE (through template callback). This
    ///       would avoid repeated dictionary lookups in RLE runs and memory bandwidth of scanning
    ///       the indices array if page doesn't fit in cache.
    ///       But then have a fallback where we switch from that to LowCardinality if the column
    ///       grows too big; then chunk delivery would have to concatenate the two columns.
    auto make_low_cardinality_column = [&]
    {
        chassert(!full_column);
        DataTypePtr type = column_info.raw_decoded_type;
        auto uniq = DataTypeLowCardinality::createColumnUnique(*type, std::move(dictionary_column));
        auto lc = ColumnLowCardinality::create(std::move(uniq), std::move(index_column), /*is_shared*/ false);
        dictionary_column.reset();
        index_column.reset();
        return lc;
    };

    size_t output_num_values_estimate = column_chunk.meta->meta_data.num_values;
    if (filter.rows_pass < filter.rows_total)
    {
        if (column_info.levels.back().rep == 0)
            output_num_values_estimate = filter.rows_pass;
        else
            /// There are arrays, so we can't know exactly how many primitive values there are in
            /// rows that pass the filter. Make a guess using average array length.
            output_num_values_estimate = size_t(1.2 * filter.rows_pass / filter.rows_total * output_num_values_estimate);
    }

    PaddedPODArray<char> decompressed_buffer;
    PaddedPODArray<UInt8> rep;
    PaddedPODArray<UInt8> def;
    PaddedPODArray<UInt8> page_value_filter_buf;
    MutableColumnPtr null_map_column;
    ColumnUInt8::Container * null_map = nullptr;

    /// Whether the innermost array element type is nullable. Nullable arrays don't count because
    /// clickhouse doesn't support that; we convert null arrays to empty arrays.
    bool is_nullable = !column_info.levels.back().is_array;
    /// If false, there's no distinction between "rows" and "values".
    bool has_arrays = column_info.levels.back().rep > 0;
    bool null_count_is_known_to_be_zero =
        column_chunk.meta->meta_data.statistics.__isset.null_count &&
        column_chunk.meta->meta_data.statistics.null_count == 0;
    /// If false, there's no distinction between "rows", "values", and "encoded values".
    /// In particular covers the common case where the column is declared as nullable, but there are
    /// no nulls (and the writer wrote column statistics that say that).
    /// (In presence of arrays, parquet's concept of "null" probably includes empty arrays.)
    bool has_nulls_or_arrays = has_arrays || (is_nullable && !null_count_is_known_to_be_zero);

    if (is_nullable && has_nulls_or_arrays)
    {
        null_map_column = ColumnUInt8::create();
        null_map = &assert_cast<ColumnUInt8 &>(*null_map_column).getData();
        null_map->reserve(output_num_values_estimate);
    }

    std::vector<ColumnUInt64::Container *> arrays_offsets(column_info.levels.back().rep);
    column_chunk.arrays_offsets.resize(arrays_offsets.size());
    for (size_t i = 0; i < arrays_offsets.size(); ++i)
    {
        column_chunk.arrays_offsets[i] = ColumnArray::ColumnOffsets::create();
        arrays_offsets[i] = &assert_cast<ColumnUInt64 &>(*column_chunk.arrays_offsets[i]).getData();
        arrays_offsets[i]->reserve(i ? output_num_values_estimate : filter.rows_total);
    }

    const auto data = prefetcher.getRangeData(column_chunk.data_prefetch);
    /// Last row in the previous page. (Repetition levels indicate the first value of each row, but
    /// not the last one. So we can't always tell whether the first value of the next page belongs
    /// to the same row as last value of current page.)
    size_t row_idx = size_t(-1);
    size_t pos = 0;
    while (pos < data.size())
    {
        decompressed_buffer.clear();
        parq::PageHeader header;
        pos += deserializeThriftStruct(header, data.data() + pos, data.size() - pos);
        size_t compressed_page_size = size_t(header.compressed_page_size);
        if (pos + compressed_page_size > data.size() || header.compressed_page_size < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Page size out of bounds: offset {} + size {} > column chunk size {}", pos, compressed_page_size, data.size());

        const char * compressed_page_data = data.data() + pos;
        pos += compressed_page_size;

        if (header.type == parq::PageType::INDEX_PAGE)
            continue;

        /// (On first iteration of the loop row_idx is -1. It's ok because filter is padded on both sides.)
        const UInt8 * page_row_filter = filter.filter ? filter.filter.data() + row_idx : nullptr;

        if (page_row_filter && (header.type == parq::PageType::DATA_PAGE_V2 || !has_arrays))
        {
            /// Check if all rows of the page are filtered out.
            /// (Can't do this in case of DataPage v1 with arrays because row may cross page bounaries.)
            size_t page_num_rows = size_t(header.type == parq::PageType::DATA_PAGE_V2 ? header.data_page_header_v2.num_rows : header.data_page_header.num_values);
            if (row_idx + 1 + page_num_rows > filter.rows_total)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Pages of column chunk have unexpectedly many rows in total: {} + {} > {}", row_idx + 1, page_num_rows, filter.rows_total);

            /// TODO: Instead of two memchr-s consider doing one fast accumulate, with result also
            ///       reused below for num_filtered_values in !has_nulls_or_arrays case.
            if (memchr(page_row_filter + 1, 1, page_num_rows) == nullptr)
            {
                /// All 0, skip the page.
                row_idx += page_num_rows;
                continue;
            }

            if (memchr(page_row_filter + 1, 0, page_num_rows) == nullptr)
            {
                /// All 1, no need to filter.
                page_row_filter = nullptr;
            }
        }

        /// Pages of all 3 types boil down to encoded values + optional rep/def levels.
        /// Extract that information.

        parq::CompressionCodec::type codec = column_chunk.meta->meta_data.codec;
        const char * encoded_values = compressed_page_data; // compressed with `codec`
        size_t encoded_values_compressed_size = compressed_page_size;
        size_t encoded_values_uncompressed_size = header.uncompressed_page_size;
        const char * encoded_rep = nullptr; // uncompressed
        const char * encoded_def = nullptr; // uncompressed
        /// num_values is "Number of values, including NULLs". Aka number of definition levels.
        /// Number of actually encoded values is `num_values - num_nulls`, where num_nulls is count
        /// of def[i] < max_def. (Parquet "NULLs" include empty arrays.)
        size_t num_values = 0;
        size_t encoded_rep_size = 0;
        size_t encoded_def_size = 0;
        parq::Encoding::type values_encoding = parq::Encoding::PLAIN;
        parq::Encoding::type def_encoding = parq::Encoding::RLE;
        parq::Encoding::type rep_encoding = parq::Encoding::RLE;

        if (header.type == parq::PageType::DATA_PAGE)
        {
            num_values = header.data_page_header.num_values;
            values_encoding = header.data_page_header.encoding;
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
                const char * ptr;
                size_t size = size_t(header.uncompressed_page_size);
                if (codec == parq::CompressionCodec::UNCOMPRESSED)
                {
                    if (size != compressed_page_size)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "No compression, but compressed and uncompressed page size are different");
                    ptr = compressed_page_data;
                }
                else
                {
                    decompressed_buffer.resize(size);
                    decompress(compressed_page_data, compressed_page_size, size, codec, decompressed_buffer.data());
                    ptr = decompressed_buffer.data();
                    codec = parq::CompressionCodec::UNCOMPRESSED;
                }

                if (size < 4)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Decompressed data is too short (def size)");
                UInt32 n;
                memcpy(&n, ptr, 4);
                if (n > size - 4)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Decompressed data is too short (def)");
                encoded_def = ptr + 4;
                encoded_def_size = n;
                ptr += 4 + encoded_def_size;
                size -= 4 + encoded_def_size;

                if (column_info.levels.back().rep > 0)
                {
                    if (size < 4)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Decompressed data is too short (rep size)");
                    memcpy(&n, ptr, 4);
                    if (n > size - 4)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Decompressed data is too short (rep)");
                    encoded_rep = ptr + 4;
                    encoded_rep_size = n;
                    ptr += 4 + encoded_rep_size;
                    size -= 4 + encoded_rep_size;
                }

                encoded_values = ptr;
                encoded_values_compressed_size = size;
                encoded_values_uncompressed_size = size;
            }
        }
        else if (header.type == parq::PageType::DATA_PAGE_V2)
        {
            num_values = header.data_page_header_v2.num_values;
            values_encoding = header.data_page_header_v2.encoding;
            encoded_def_size = header.data_page_header_v2.definition_levels_byte_length;
            encoded_rep_size = header.data_page_header_v2.repetition_levels_byte_length;

            if (header.data_page_header_v2.__isset.is_compressed &&
                !header.data_page_header_v2.is_compressed)
            {
                codec = parq::CompressionCodec::UNCOMPRESSED;
            }

            if (encoded_def_size + encoded_rep_size > compressed_page_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Page data is too short (def+rep)");
            encoded_def = compressed_page_data;
            encoded_rep = compressed_page_data + encoded_def_size;
            size_t uncompressed_part = encoded_def_size + encoded_rep_size;
            encoded_values = compressed_page_data + uncompressed_part;
            encoded_values_compressed_size = compressed_page_size - uncompressed_part;
            encoded_values_uncompressed_size = size_t(header.uncompressed_page_size) - uncompressed_part;
        }
        else if (header.type == parq::PageType::DICTIONARY_PAGE)
        {
            num_values = header.dictionary_page_header.num_values;
            values_encoding = header.dictionary_page_header.encoding;
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected page type: {}", thriftToString(header));
        }

        if (values_encoding == parq::Encoding::PLAIN_DICTIONARY)
            values_encoding = header.type == parq::PageType::DICTIONARY_PAGE
                ? parq::Encoding::PLAIN : parq::Encoding::RLE_DICTIONARY;

        /// Determine what we're decoding: dictionary, indices into dictionary, or full column.
        IColumn * destination = nullptr;
        const ValueDecoder * decoder = column_info.decoder.get();
        if (header.type == parq::PageType::DICTIONARY_PAGE)
        {
            if (dictionary_column || full_column)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected extra dictionary page");
            dictionary_column = column_info.raw_decoded_type->createColumn();
            dictionary_column->reserve(num_values + 2);
            dictionary_column->insertManyDefaults(2);
            destination = dictionary_column.get();
        }
        else if (values_encoding == parq::Encoding::RLE_DICTIONARY)
        {
            if (!dictionary_column)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected dictionary-encoded page without dictionary");
            if (full_column)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected dictionary-encoded page after non-dictionary-encoded pages");

            if (!index_column)
            {
                size_t dict_size = dictionary_column->size();
                chassert(dict_size >= 2);
                if (dict_size <= (1 << 8))
                {
                    index_column = ColumnUInt8::create();
                    index_decoder = std::make_unique<DictionaryIndexDecoder<UInt8>>(dict_size - 2);
                }
                else if (dict_size <= (1 << 16))
                {
                    index_column = ColumnUInt16::create();
                    index_decoder = std::make_unique<DictionaryIndexDecoder<UInt16>>(dict_size - 2);
                }
                else if (dict_size <= (1ul << 32))
                {
                    index_column = ColumnUInt32::create();
                    index_decoder = std::make_unique<DictionaryIndexDecoder<UInt32>>(dict_size - 2);
                }
                else
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpectedly large dictionary");

                index_column->reserve(output_num_values_estimate);
            }

            destination = index_column.get();
            decoder = index_decoder.get();
        }
        else
        {
            if (index_column)
            {
                /// Switching from dictionary to non-dictionary encoding.
                /// Convert the dictionary-encoded data to full column.
                //TODO: test this code path
                auto lc = make_low_cardinality_column();
                full_column = IColumn::mutate(lc->convertToFullColumn());

                full_column->reserve(output_num_values_estimate);

                dictionary_column.reset();
                index_column.reset();
            }

            if (!full_column)
            {
                full_column = column_info.raw_decoded_type->createColumn();
                full_column->reserve(output_num_values_estimate);
            }

            destination = full_column.get();
        }

        /// Decode rep/def levels.
        UInt8 max_def = column_info.levels.back().def;
        UInt8 max_rep = column_info.levels.back().rep;
        if (max_def > 0)
        {
            def.resize(num_values);
            size_t def_bits = 32 - __builtin_clz(UInt32(max_def));
            decodeBitPackedRLE<UInt8>(max_def, def_bits, num_values, encoded_def, encoded_def_size, /*filter*/ nullptr, def.data());
        }
        if (column_info.levels.back().rep > 0)
        {
            rep.resize(num_values);
            size_t rep_bits = 32 - __builtin_clz(UInt32(max_rep));
            decodeBitPackedRLE<UInt8>(max_rep, rep_bits, num_values, encoded_rep, encoded_rep_size, /*filter*/ nullptr, rep.data());
        }

        /// Process rep/def levels. See "Rep/def level explanation" comment.

        /// Handle arrays.
        size_t page_num_rows = num_values;
        if (has_arrays)
        {
            /// Count rows.
            /// (What does "page_num_rows" mean when arrays cross page boundaries? The number of
            ///  values with rep == 0 in the page. Each such value is the first one in its row.)
            /// TODO: Try combining it with outer array offsets if no filter (would need bounds check
            ///       on each iteration).
            page_num_rows = 0;
            for (size_t i = 0; i < num_values; ++i)
                page_num_rows += rep[i] == 0;

            if (row_idx + page_num_rows >= filter.rows_total)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Page has too many rows");

            /// Produce array offsets.
            for (const LevelInfo & level : column_info.levels)
            {
                if (!level.is_array)
                {
                    /// TODO: If !options.null_as_default, check that there are no null arrays.
                    continue;
                }

                if (page_row_filter)
                    processRepDefLevelsForArray</*HAS_FILTER*/ true>(
                        num_values, def.data(), rep.data(), level.rep, level.def,
                        page_num_rows, page_row_filter, arrays_offsets.at(level.rep - 1));
                else
                    processRepDefLevelsForArray</*HAS_FILTER*/ false>(
                        num_values, def.data(), rep.data(), level.rep, level.def,
                        page_num_rows, page_row_filter, arrays_offsets.at(level.rep - 1));
            }

            chassert(arrays_offsets.at(0).size() == page_num_rows);
        }

        UInt8 max_array_def = 0;
        for (size_t i = column_info.levels.size() - 1; i >= 0; --i)
        {
            if (column_info.levels[i].is_array)
            {
                max_array_def = column_info.levels[i].def;
                break;
            }
        }

        /// Handle the inner primitive+nullable column: value filter, null_map, etc.
        const UInt8 * page_value_filter = nullptr;
        size_t num_encoded_values = num_values;
        size_t num_filtered_values = num_values;
        if (!has_nulls_or_arrays)
        {
            /// Special fast path where we can use row filter directly as value filter.
            /// Row == value == encoded value.
            if (page_row_filter)
            {
                page_value_filter = page_row_filter;
                num_filtered_values = std::accumulate(page_value_filter, page_value_filter + num_values, 0ul);
            }
        }
        else
        {
            if (page_row_filter)
            {
                page_value_filter_buf.clear();
                page_value_filter_buf.reserve(num_values);
            }

            /// Dispatch to a version of the hot loop with unneeded features disabled.
#define X(has_filter, has_arrays, has_nulls) \
                processRepDefLevelsForInnermostColumn<has_filter, has_arrays, has_nulls>( \
                    num_values, def.data(), rep.data(), max_def, max_array_def, \
                    page_num_rows, page_row_filter, num_encoded_values, num_filtered_values, \
                    page_value_filter_buf, null_map)

            if (page_row_filter)
                if (has_arrays)
                    if (null_map)
                        X(true, true, true);
                    else
                        X(true, true, false);
                else
                    if (null_map)
                        X(true, false, true);
                    else
                        X(true, false, false);
            else
                if (has_arrays)
                    if (null_map)
                        X(false, true, true);
                    else
                        X(false, true, false);
                else
                    if (null_map)
                        X(false, false, true);
                    else
                        X(false, false, false);
#undef X

            if (page_row_filter)
                page_value_filter = page_value_filter_buf.data();
        }

        row_idx += page_num_rows;

        /// Finally decode the values.

        char * dest_ptr;
        size_t dest_size;
        size_t prev_destination_size = destination->size();
        if (!page_value_filter && decoder->canReadDirectlyIntoColumn(values_encoding, num_encoded_values, *destination, &dest_ptr, &dest_size))
        {
            if (dest_size > encoded_values_uncompressed_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Page data is too short (values)");
            if (codec == parq::CompressionCodec::UNCOMPRESSED)
                memcpy(dest_ptr, encoded_values, dest_size);
            else
                /// Decompress directly into IColumn, avoid memcpy, yay.
                decompress(encoded_values, encoded_values_compressed_size, encoded_values_uncompressed_size, codec, dest_ptr);
        }
        else
        {
            if (codec != parq::CompressionCodec::UNCOMPRESSED)
            {
                chassert(decompressed_buffer.empty());
                decompressed_buffer.resize(encoded_values_uncompressed_size);
                decompress(encoded_values, encoded_values_compressed_size, encoded_values_uncompressed_size, codec, decompressed_buffer.data());
                encoded_values = decompressed_buffer.data();
            }

            decoder->decodePage(values_encoding, encoded_values, encoded_values_uncompressed_size, num_encoded_values, num_filtered_values, *destination, page_value_filter);
        }

        if (destination->size() - prev_destination_size != num_filtered_values)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected number of decoded values");
    }

    for (size_t i = 0; i < arrays_offsets.size(); ++i)
    {
        /// If repetition levels say that the column chunk starts in the middle of an array
        /// (e.g. first rep level is not 0; there are other cases with nested arrays),
        /// processRepDefLevelsForArray will correspondingly reassign the offset of the start of the
        /// first array. That wouldn't be a valid ColumnArray.
        if (arrays_offsets[i][-1] != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid repetition/definition levels for arrays in column {}", column_info.name);
    }

    if (null_map && !column_info.output_nullable && !options.null_as_default)
    {
        if (memchr(null_map.data(), 0, null_map.size()) != nullptr)
            throw Exception(ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN, "Cannot convert NULL value to non-Nullable type for column {}", column_info.name);
        null_map = nullptr;
        null_map_column.reset();
    }

    asdqwe;
    if (index_column)
    {
        if (null_map)
            /// TODO: Consider expanding after each page to reduce cache misses (same for the other expand below).
            index_column->expand(*null_map, /*inverted*/ false);

        auto lc = make_low_cardinality_column();

        //TODO: assign zip_bombness; use SizeStatistics.unencoded_byte_array_data_bytes when available

        if (column_info.output_low_cardinality || column_chunk.zip_bombness > 1)
        {
            /// Keep LowCardinality.

            if (column_info.output_nullable)
                assert_cast<ColumnLowCardinality &>(*lc).nestedToNullable();

            column_chunk.column = std::move(lc);
        }
        else
        {
            column_chunk.column = lc->convertToFullColumn();

            if (column_info.output_nullable)
                column_chunk.column = ColumnNullable::create(std::move(column_chunk.column), std::move(null_map_column));
        }
    }
    else if (full_column)
    {
        if (null_map)
            full_column->expand(*null_map, /*inverted*/ false);
        column_chunk.column = std::move(full_column);
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Column chunk has no data pages");
    }

    asdqwe convert to nullable and/or lc if needed;
}

size_t Reader::decideNumRowsPerChunk(RowGroup & row_group)
{
    size_t num_chunks = 1;
    for (auto & c : row_group.columns)
        num_chunks = std::max(num_chunks, c.zip_bombness);
    return (row_group.filter.rows_pass + num_chunks - 1) / num_chunks;
}

ColumnPtr Reader::formOutputColumn(RowGroup & row_group, size_t output_column_idx, size_t start_row, size_t num_rows)
{
    //TODO: arrays etc

    const OutputColumnInfo & output_info = output_columns[output_column_idx];
    chassert(output_info.primitive_start + 1 == output_info.primitive_end);
    size_t primitive_idx = output_info.primitive_start;
    const PrimitiveColumnInfo & primitive_info = primitive_columns.at(primitive_idx);

    ColumnPtr col = row_group.columns.at(primitive_idx).column;
    ColumnPtr res;

    asdqwe change everytying;
    //TODO: check handling of nullable and lc

    if (!primitive_info.output_low_cardinality)
    {
        const auto * lc = typeid_cast<const ColumnLowCardinality *>(col.get());
        if (lc)
        {
            ColumnPtr indexes = lc->getIndexesPtr();
            auto indexes_range = indexes->cloneEmpty();
            indexes_range->insertRangeFrom(*indexes, start_row, num_rows);
            res = lc->getDictionary().getNestedColumn()->index(*indexes_range, 0);
        }
    }

    if (!res)
    {
        if (start_row == 0 && num_rows == col->size())
        {
            res = col;
        }
        else
        {
            auto range = col->cloneEmpty();
            range->insertRangeFrom(*col, start_row, num_rows);
            res = std::move(range);
        }
    }

    /// (Comparing pointers is ok here.)
    if (output_info.need_cast)
    {
        /// TODO: Try InternalCastFunctionCache.
        const String & name = sample_block->getByPosition(output_info.idx_in_output_block.value()).name;
        res = castColumn({res, output_info.decoded_type, name}, output_info.final_type);
    }

    return res;
}

Reader::PrimitiveColumnInfo::~PrimitiveColumnInfo() = default;

}
