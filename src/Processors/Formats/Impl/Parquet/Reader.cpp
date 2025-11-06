#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/FilterDescription.h>
#include <Common/FieldAccurateComparison.h>
#include <Formats/FormatFilterInfo.h>
#include <Interpreters/castColumn.h>
#include <IO/CompressionMethod.h>
#include <Processors/Formats/Impl/Parquet/Decoding.h>
#include <Processors/Formats/Impl/Parquet/parquetBloomFilterHash.h>
#include <Processors/Formats/Impl/Parquet/Reader.h>
#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>
#include <Storages/SelectQueryInfo.h>

#include <lz4.h>

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

namespace DB::Parquet
{

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

void Reader::init(const ReadOptions & options_, const Block & sample_block_, FormatFilterInfoPtr format_filter_info_)
{
    options = options_;
    sample_block = &sample_block_;
    format_filter_info = format_filter_info_;
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
    /// We work around (1) in initializePage by allowing dictionary page in place of data page.
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

void Reader::getHyperrectangleForRowGroup(const parq::RowGroup * meta, Hyperrectangle & hyperrectangle) const
{
    for (const PrimitiveColumnInfo & column_info : primitive_columns)
    {
        if (!column_info.used_by_key_condition.has_value())
            continue;
        if (!column_info.decoder.allow_stats)
            continue;
        try
        {
            const auto & column_meta = meta->columns.at(column_info.column_idx).meta_data;
            if (!column_meta.__isset.statistics)
                continue;

            Range & range = hyperrectangle[*column_info.used_by_key_condition];

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
                    range.right = range.left = column_info.final_type->getDefault();
                else
                    range.right = range.left;
                continue;
            }

            if (column_meta.statistics.__isset.min_value)
                column_info.decoder.decodeField(column_meta.statistics.min_value, /*is_max=*/ false, range.left);
            if (column_meta.statistics.__isset.max_value)
                column_info.decoder.decodeField(column_meta.statistics.max_value, /*is_max=*/ true, range.right);

            adjustRangeFromIndexIfNeeded(range, column_info, can_be_null);
        }
        catch (Exception & e)
        {
            e.addMessage("in column chunk statistics for column '{}'; use input_format_parquet_filter_push_down=0 to ignore", column_info.name);
            throw;
        }
    }
}

void Reader::prefilterAndInitRowGroups()
{
    extended_sample_block = *sample_block;
    for (const auto & col : format_filter_info->additional_columns)
        extended_sample_block.insert(col);
    extended_sample_block_data_types = extended_sample_block.getDataTypes();
    const auto & row_level_filter = format_filter_info->row_level_filter;
    const auto & prewhere_info = format_filter_info->prewhere_info;

    /// Process schema.
    SchemaConverter schemer(file_metadata, options, &extended_sample_block);
    if (row_level_filter && !row_level_filter->do_remove_column)
        schemer.external_columns.push_back(row_level_filter->column_name);
    if (prewhere_info && !prewhere_info->remove_prewhere_column)
        schemer.external_columns.push_back(prewhere_info->prewhere_column_name);
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
            if (!output_idx.has_value())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "KeyCondition uses PREWHERE output");
            const OutputColumnInfo & output_info = output_columns[output_idx.value()];

            if (output_info.is_primitive)
                primitive_columns[output_info.primitive_start].used_by_key_condition = idx_in_output_block;
        }
    }

    /// Populate row_groups. Skip row groups based on column chunk min/max statistics.
    size_t total_rows = 0;
    for (size_t row_group_idx = 0; row_group_idx < file_metadata.row_groups.size(); ++row_group_idx)
    {
        const auto * meta = &file_metadata.row_groups[row_group_idx];
        if (meta->num_rows <= 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Row group {} has <= 0 rows: {}", row_group_idx, meta->num_rows);
        if (meta->columns.size() != total_primitive_columns_in_file)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Row group {} has unexpected number of columns: {} != {}", row_group_idx, meta->columns.size(), total_primitive_columns_in_file);

        total_rows += size_t(meta->num_rows); // before potentially skipping the row group

        Hyperrectangle hyperrectangle(extended_sample_block.columns(), Range::createWholeUniverse());
        if (options.format.parquet.filter_push_down && format_filter_info->key_condition)
        {
            getHyperrectangleForRowGroup(meta, hyperrectangle);
            if (!format_filter_info->key_condition->checkInHyperrectangle(
                    hyperrectangle, extended_sample_block_data_types).can_be_true)
                continue;
        }

        RowGroup & row_group = row_groups.emplace_back();
        row_group.meta = meta;
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

    if (row_groups.empty())
        return; // all row groups were skipped

    if (options.format.parquet.bloom_filter_push_down && format_filter_info->key_condition)
        prepareBloomFilterCondition();

    if (options.format.parquet.page_filter_push_down)
    {
        const auto & column_conditions = static_cast<FilterInfoExt *>(format_filter_info->opaque.get())->column_conditions;
        for (const auto & [idx_in_output_block, key_condition] : column_conditions)
        {
            const auto & output_idx = sample_block_to_output_columns_idx.at(idx_in_output_block);
            if (!output_idx.has_value())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column condition uses PREWHERE output");
            const OutputColumnInfo & output_info = output_columns[output_idx.value()];

            if (!output_info.is_primitive)
                continue;
            primitive_columns[output_info.primitive_start].column_index_condition = key_condition.get();
        }
    }

    initializePrefetches();
}

void Reader::prepareBloomFilterCondition()
{
    /// Index in output block -> arrow column info.
    std::vector<std::optional<std::pair</*primitive_idx*/ size_t, parquet::ColumnDescriptor>>>
        bf_eligible_columns(extended_sample_block.columns());
    bool any_column_eligible_for_bf = false;
    for (size_t primitive_idx = 0; primitive_idx < primitive_columns.size(); ++primitive_idx)
    {
        const PrimitiveColumnInfo & column_info = primitive_columns[primitive_idx];
        const auto & idx_in_output_block = column_info.used_by_key_condition;
        if (!idx_in_output_block.has_value())
            continue;

        /// Check for presence of bloom filter only in first row group, expecting that usually
        /// either all or none of the row groups have bloom filter for any given column.
        const parq::ColumnChunk * column_chunk_meta = row_groups[0].columns[primitive_idx].meta;
        if (!column_chunk_meta->meta_data.__isset.bloom_filter_offset)
            continue;

        /// Glue to convert thrift types to equivalent arrow types because arrow felt the need to
        /// duplicate them for some reason. Our parquetTryHashColumn is called from both the
        /// arrow-based reader v0 and this reader v3, so arrow types are the common denominator.
        /// Warning: this requires that we use the same thrift-generated types as arrow; if we
        /// ever switch to thrift-generating our own code from parquet.thrift (e.g. to use a
        /// newer version), this will stop working.
        const parquet::format::SchemaElement * schema_element = &file_metadata.schema.at(column_info.schema_idx);
        auto node = parquet::schema::PrimitiveNode::FromParquet(static_cast<const void *>(schema_element));
        parquet::ColumnDescriptor desc(std::move(node), column_info.levels.back().def, column_info.levels.back().rep);
        bf_eligible_columns[*idx_in_output_block].emplace(primitive_idx, std::move(desc));
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
            if (column->size() > options.bloom_filter_max_set_size)
                return std::nullopt;
            const auto & [primitive_idx, descriptor] = *pair;
            auto hashes = parquetTryHashColumn(column.get(), &descriptor);
            if (!hashes.has_value())
                return std::nullopt;

            PrimitiveColumnInfo & column_info = primitive_columns[primitive_idx];
            column_info.use_bloom_filter = true;
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
        || std::any_of(primitive_columns.begin(), primitive_columns.end(), [](const auto & c) { return c.column_index_condition; });
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

                /// Dictionary filter.
                if (primitive_columns[column_idx].used_by_key_condition.has_value() &&
                    dict_page_length < options.dictionary_filter_limit_bytes &&
                    column.meta->meta_data.__isset.encoding_stats)
                {
                    bool all_pages_are_dictionary_encoded = true;
                    for (const parq::PageEncodingStats & s : column.meta->meta_data.encoding_stats)
                        all_pages_are_dictionary_encoded &=
                            (s.page_type != parq::PageType::DATA_PAGE && s.page_type != parq::PageType::DATA_PAGE_V2) ||
                            s.encoding == parq::Encoding::PLAIN_DICTIONARY ||
                            s.encoding == parq::Encoding::RLE_DICTIONARY ||
                            s.count == 0;
                    column.use_dictionary_filter = all_pages_are_dictionary_encoded;
                }
            }

            /// Bloom filter.
            if (!column.use_dictionary_filter &&
                primitive_columns[column_idx].use_bloom_filter &&
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
            column.use_column_index = primitive_columns[column_idx].column_index_condition
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
    if (row_level_filter || prewhere_info)
    {
        /// TODO [parquet]: We currently run prewhere after reading all prewhere columns of the row
        ///     subgroup, in one thread per row group. Instead, we could extract single-column conditions
        ///     and run them after decoding the corresponding columns, in parallel.
        ///     (Still run multi-column conditions, like `col1 = 42 or col2 = 'yes'`, after reading all columns.)
        ///     Probably reuse tryBuildPrewhereSteps from MergeTree for splitting the expression.

        /// Convert ActionsDAG to ExpressionActions.
        ExpressionActionsSettings actions_settings;
        if (row_level_filter)
        {
            ExpressionActions actions(row_level_filter->actions.clone(), actions_settings);
            prewhere_steps.push_back(PrewhereStep
                {
                    .actions = std::move(actions),
                    .result_column_name = row_level_filter->column_name,
                });

            if (!row_level_filter->do_remove_column)
                prewhere_steps.back().idx_in_output_block = sample_block->getPositionByName(row_level_filter->column_name);
        }

        if (prewhere_info)
        {
            ExpressionActions actions(prewhere_info->prewhere_actions.clone(), actions_settings);
            prewhere_steps.push_back(PrewhereStep
                {
                    .actions = std::move(actions),
                    .result_column_name = prewhere_info->prewhere_column_name,
                    .need_filter = prewhere_info->need_filter,
                });

            if (!prewhere_info->remove_prewhere_column)
                prewhere_steps.back().idx_in_output_block = sample_block->getPositionByName(prewhere_info->prewhere_column_name);
        }
    }

    /// Look up expression inputs in extended_sample_block.
    for (PrewhereStep & step : prewhere_steps)
    {
        for (const auto & col : step.actions.getRequiredColumnsWithTypes())
        {
            size_t idx_in_output_block = extended_sample_block.getPositionByName(col.name, /* case_insensitive= */ false);
            const auto & output_idx = sample_block_to_output_columns_idx.at(idx_in_output_block);
            if (!output_idx.has_value())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "PREWHERE appears to use its own output as input");
            OutputColumnInfo & output_info = output_columns[output_idx.value()];

            output_info.use_prewhere = true;
            bool only_for_prewhere = idx_in_output_block >= sample_block->columns();

            for (size_t primitive_idx = output_info.primitive_start; primitive_idx < output_info.primitive_end; ++primitive_idx)
            {
                primitive_columns[primitive_idx].use_prewhere = true;
                primitive_columns[primitive_idx].only_for_prewhere = only_for_prewhere;
            }

            step.input_column_idxs.push_back(output_idx.value());
        }
    }

    /// Assert that sample_block_to_output_columns_idx is valid.
    for (size_t i = 0; i < sample_block_to_output_columns_idx.size(); ++i)
    {
        /// (`prewhere_steps` has at most two elements)
        size_t is_prewhere_output = std::count_if(prewhere_steps.begin(), prewhere_steps.end(),
            [&](const PrewhereStep & step) { return step.idx_in_output_block == i; });
        if (is_prewhere_output > 1 ||
            /// Column must appear in exactly one of {output_columns, prewhere output}.
            sample_block_to_output_columns_idx[i].has_value() != !is_prewhere_output)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column in sample block: {}", extended_sample_block.getByPosition(i).name);
        }
    }
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
    auto prefetches = prefetcher.splitRange(std::move(column.bloom_filter_data_prefetch), subranges, /*likely_to_be_used*/ false);

    column.bloom_filter_blocks.reserve(block_idxs.size());
    for (size_t i = 0; i < block_idxs.size(); ++i)
    {
        BloomFilterBlock & block = column.bloom_filter_blocks.emplace_back();
        block.block_idx = block_idxs[i];
        block.prefetch = std::move(prefetches[i]);
    }
}

bool Reader::decodeDictionaryPage(ColumnChunk & column, const PrimitiveColumnInfo & column_info)
{
    auto data = prefetcher.getRangeData(column.dictionary_page_prefetch);
    parq::PageHeader header;
    size_t header_size = deserializeThriftStruct(header, data.data(), data.size());

    if (header.type != parq::PageType::DICTIONARY_PAGE)
    {
        if (column.meta->meta_data.__isset.dictionary_page_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected dictionary page type: {}", thriftToString(header.type));

        /// Parquet metadata didn't specifically say that this byte range is a dictionary page.
        return false;
    }

    decodeDictionaryPageImpl(header, data.subspan(header_size), column, column_info);
    return true;
}

void Reader::decodeDictionaryPageImpl(const parq::PageHeader & header, std::span<const char> data, ColumnChunk & column, const PrimitiveColumnInfo & column_info)
{
    chassert(header.type == parq::PageType::DICTIONARY_PAGE);

    /// TODO [parquet]: Check checksum.
    size_t compressed_page_size = size_t(header.compressed_page_size);
    if (header.compressed_page_size < 0 || compressed_page_size > data.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Dictionary page size out of bounds: {} > {}", header.compressed_page_size, data.size());
    data = data.subspan(0, size_t(header.compressed_page_size));

    auto codec = column.meta->meta_data.codec;
    if (codec != parq::CompressionCodec::UNCOMPRESSED)
    {
        size_t uncompressed_size = size_t(header.uncompressed_page_size);
        auto & buf = column.dictionary.decompressed_buf;
        buf.resize(uncompressed_size);
        decompress(data.data(), data.size(), buf.size(), codec, buf.data());
        data = std::span(buf.data(), buf.size());
    }

    column.dictionary.decode(header.dictionary_page_header.encoding, column_info.decoder, size_t(header.dictionary_page_header.num_values), data, *column_info.raw_decoded_type);
}

bool Reader::BloomFilterLookup::findAnyHash(const std::vector<uint64_t> & hashes)
{
    size_t num_blocks = size_t(column.bloom_filter_header.numBytes) / 32;
    for (size_t h : hashes)
    {
        size_t block_idx = ((h >> 32) * num_blocks) >> 32;
        auto it = std::partition_point(column.bloom_filter_blocks.begin(), column.bloom_filter_blocks.end(), [&](const BloomFilterBlock & block) { return block.block_idx < block_idx; });
        /// All hashes must've been preregistered in bloom_filter_hashes, and their blocks prefetched.
        if (it == column.bloom_filter_blocks.end() || it->block_idx != block_idx)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected hash in bloom filter lookup");

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

bool Reader::applyBloomAndDictionaryFilters(RowGroup & row_group)
{
    /// TODO [parquet]: Dictionary filter.

    KeyCondition::ColumnIndexToBloomFilter filter_map;
    for (size_t i = 0; i < row_group.columns.size(); ++i)
    {
        if (row_group.columns[i].use_bloom_filter)
            filter_map.emplace(
                primitive_columns[i].used_by_key_condition.value(),
                std::make_unique<BloomFilterLookup>(prefetcher, row_group.columns[i]));
    }
    /// We use both the min/max statistics and bloom filter. For the case where condition has
    /// something like `x < 42 OR y = 1337`, where `x < 42` is ruled out by min/max, and `y = 1337`
    /// is ruled out by bloom filter.
    /// (I'm guessing this hardly ever comes up in practice, but it was easy enough to support.)
    return bloom_filter_condition->checkInHyperrectangle(
        row_group.hyperrectangle, extended_sample_block_data_types, filter_map).can_be_true;
}

void Reader::applyColumnIndex(ColumnChunk & column, const PrimitiveColumnInfo & column_info, const RowGroup & row_group)
{
    try
    {
        chassert(column.use_column_index);
        chassert(column_info.column_index_condition);
        size_t idx_in_output_block = column_info.used_by_key_condition.value();

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

        Hyperrectangle hyperrectangle(extended_sample_block.columns(), Range::createWholeUniverse());
        size_t prev_row_idx = 0; // start of the latest range of rows that pass filter
        for (size_t page_idx = 0; page_idx < num_pages; ++page_idx)
        {
            Range & range = hyperrectangle[idx_in_output_block];
            range = Range::createWholeUniverse();

            bool always_null = !column_index.null_pages.empty() && column_index.null_pages[page_idx];
            bool can_be_null = !column_index.__isset.null_counts || column_index.null_counts[page_idx] != 0;

            if (nullable && always_null)
            {
                /// Single-point range containing either the default value or one of the infinities.
                if (null_as_default)
                    range.right = range.left = column_info.final_type->getDefault();
                else
                    range.right = range.left;
            }
            else
            {
                column_info.decoder.decodeField(column_index.min_values[page_idx], /*is_max=*/ false, range.left);
                column_info.decoder.decodeField(column_index.max_values[page_idx], /*is_max=*/ true, range.right);

                adjustRangeFromIndexIfNeeded(range, column_info, can_be_null);
            }

            bool passes_filter = column_info.column_index_condition->checkInHyperrectangle(
                hyperrectangle, extended_sample_block_data_types).can_be_true;

            if (!passes_filter)
            {
                size_t start_row = column.offset_index.page_locations[page_idx].first_row_index;
                size_t end_row = page_idx + 1 < num_pages ? column.offset_index.page_locations[page_idx + 1].first_row_index : row_group.meta->num_rows;
                chassert(end_row > start_row); // validated in decodeOffsetIndex
                if (start_row > prev_row_idx)
                    column.row_ranges_after_column_index.emplace_back(prev_row_idx, start_row);
                prev_row_idx = end_row;
            }
        }

        if (size_t(row_group.meta->num_rows) > prev_row_idx)
            column.row_ranges_after_column_index.emplace_back(prev_row_idx, row_group.meta->num_rows);
    }
    catch (Exception & e)
    {
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
            Field default_value = column_info.final_type->getDefault();
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

        size_t n = size_t(options.format.parquet.prefer_block_bytes / std::max(bytes_per_row, 1.));
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
            row_subgroup.filter.rows_pass = subend - substart;
            row_subgroup.filter.rows_total = row_subgroup.filter.rows_pass;

            row_subgroup.columns.resize(primitive_columns.size());
            row_subgroup.output.resize(extended_sample_block.columns());
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
    double column_chunk_bytes;
    if (column.meta->meta_data.__isset.size_statistics &&
        column.meta->meta_data.size_statistics.__isset.unencoded_byte_array_data_bytes)
    {
        /// The writer of the parquet file has helpfully put the total length of the
        /// strings into file metadata. Thanks writer!
        column_chunk_bytes = column.meta->meta_data.size_statistics.unencoded_byte_array_data_bytes;
    }
    else if (column.meta->meta_data.__isset.dictionary_page_offset)
    {
        /// Dictionary-encoded strings. No way to know the decoded length in advance.
        double avg_string_length;
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
        column_chunk_bytes = avg_string_length * column.meta->meta_data.num_values;
    }
    else
    {
        /// Non-dictionary-encoded strings.
        column_chunk_bytes = column.meta->meta_data.total_uncompressed_size;
    }

    return column_chunk_bytes / row_group.meta->num_rows;
}

double Reader::estimateColumnMemoryBytesPerRow(const ColumnChunk & column, const RowGroup & row_group, const PrimitiveColumnInfo & column_info) const
{
    double res;
    if (column_info.final_type->haveMaximumSizeOfValue())
        /// Fixed-size values, e.g. numbers or FixedString.
        res = 1. * column_info.final_type->getMaximumSizeOfValueInMemory() * column.meta->meta_data.num_values / row_group.meta->num_rows;
    else
        res = estimateAverageStringLengthPerRow(column, row_group);

    /// Outer array offsets.
    if (column_info.levels.back().rep > 0)
        res += 8;

    /// Nested array offsets (assume the worst case where the outer arrays are long and inner arrays
    /// are short, so inner arrays have ~num_values total elements rather than ~num_rows).
    if (column_info.levels.back().rep > 1)
        res += (column_info.levels.back().rep - 1) * 8. * column.meta->meta_data.num_values / row_group.meta->num_rows;

    return res;
}

void Reader::decodePrimitiveColumn(ColumnChunk & column, const PrimitiveColumnInfo & column_info, ColumnSubchunk & subchunk, const RowGroup & row_group, const RowSubgroup & row_subgroup)
{
    /// Allocate columns for values, null map, and array offsets.

    size_t output_num_values_estimate;
    if (column_info.levels.back().rep == 0)
        output_num_values_estimate = row_subgroup.filter.rows_pass; // no arrays, rows == values
    else if (row_subgroup.filter.rows_pass == size_t(row_group.meta->num_rows))
        output_num_values_estimate = column.meta->meta_data.num_values; // whole column chunk
    else
        /// There are arrays, so we can't know exactly how many primitive values there are in
        /// rows that pass the filter. Make a guess using average array length.
        output_num_values_estimate = size_t(1.2 * row_subgroup.filter.rows_pass / row_group.meta->num_rows * column.meta->meta_data.num_values);

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

    subchunk.column = column_info.raw_decoded_type->createColumn();
    subchunk.column->reserve(output_num_values_estimate);
    if (auto * string_column = typeid_cast<ColumnString *>(subchunk.column.get()))
    {
        double avg_len = estimateAverageStringLengthPerRow(column, row_group);
        size_t bytes_to_reserve = size_t(1.2 * avg_len * row_subgroup.filter.rows_pass);
        string_column->getChars().reserve(bytes_to_reserve);
    }

    /// Find ranges of rows that pass filter and decode them.

    size_t row_subidx = 0;
    while (true) // loop over row ranges that pass the filter
    {
        /// Find a range of rows that pass filter.
        /// TODO [parquet]: We call decoder for each such range separately, with a bunch of overhead per call.
        ///       This will probably be slow on something like `PREWHERE idx%2=0`.
        ///       If it's too slow and/or comes up in practice or benchmarks, make decoders accept
        ///       a (optional) filter mask, like e.g. in commit c1a361d176507a19c2fdc49f0f1d6dc7e2cd539e.
        size_t num_rows = row_subgroup.filter.rows_total - row_subidx;
        if (!row_subgroup.filter.filter.empty())
        {
            /// TODO [parquet]: simd or something
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

        skipToRow(start_row_idx, column, column_info);

        while (true) // loop over pages
        {
            readRowsInPage(end_row_idx, subchunk, column, column_info);

            /// We're done if we've reached end_row_idx and we're at a row boundary.
            auto & page = column.page;
            if (page.next_row_idx == end_row_idx &&
                (page.value_idx < page.num_values ||
                 page.end_row_idx.has_value() || // page ends on row boundary
                 column.next_page_offset >= column.data_pages_bytes))
                break;

            /// Advance to next page.
            chassert(page.value_idx == page.num_values);
            skipToRow(page.next_row_idx, column, column_info);
            chassert(page.value_idx == 0);
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

    if (subchunk.null_map && !column_info.output_nullable && !options.format.null_as_default)
    {
        const auto & null_map = assert_cast<const ColumnUInt8 &>(*subchunk.null_map).getData();
        if (memchr(null_map.data(), 0, null_map.size()) != nullptr)
            throw Exception(ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN, "Cannot convert NULL value to non-Nullable type for column {}", column_info.name);
        subchunk.null_map = nullptr;
    }

    if (subchunk.null_map)
    {
        const auto & null_map = assert_cast<const ColumnUInt8 &>(*subchunk.null_map).getData();
        subchunk.column->expand(null_map, /*inverted*/ true);
    }

    if (subchunk.arrays_offsets.empty() && subchunk.column->size() != row_subgroup.filter.rows_pass)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected number of rows in column subchunk");

    if (column_info.output_nullable)
    {
        if (!subchunk.null_map)
            subchunk.null_map = ColumnUInt8::create(subchunk.column->size(), 0);
        subchunk.column = ColumnNullable::create(std::move(subchunk.column), std::move(subchunk.null_map));
        subchunk.null_map.reset();
    }

    chassert(subchunk.column->getDataType() == column_info.intermediate_type->getColumnType());

    if (column_info.needs_cast)
    {
        auto col = castColumn(
            {std::move(subchunk.column), column_info.intermediate_type, column_info.name},
             column_info.final_type);
        chassert(col->use_count() == 1);
        subchunk.column = IColumn::mutate(std::move(col));
    }
}

void Reader::skipToRow(size_t row_idx, ColumnChunk & column, const PrimitiveColumnInfo & column_info)
{
    /// True if column.page is initialized and contains the requested row_idx.
    bool found_page = false;
    auto & page = column.page;

    if (page.initialized && page.value_idx < page.num_values && page.end_row_idx.has_value() && *page.end_row_idx > row_idx)
        /// Fast path: we're just continuing reading the same page as before.
        found_page = true;

    if (!found_page && !column.data_pages.empty())
    {
        /// If we have offset index, find the row index there and jump to the correct page.
        while (column.data_pages_idx < column.data_pages.size() &&
            column.data_pages[column.data_pages_idx].end_row_idx <= row_idx)
            ++column.data_pages_idx;
        if (column.data_pages_idx == column.data_pages.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet offset index covers too few rows");
        const auto & page_info = column.data_pages[column.data_pages_idx];
        size_t first_row_idx = size_t(page_info.meta->first_row_index);
        if (first_row_idx > row_idx)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Row passes filters but its page was not selected for reading. This is a bug.");

        auto data = prefetcher.getRangeData(page_info.prefetch);
        const char * ptr = data.data();
        if (!initializePage(ptr, ptr + data.size(), first_row_idx, page_info.end_row_idx, row_idx, column, column_info))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Page doesn't contain requested row");
        found_page = true;
    }

    while (true)
    {
        /// Skip rows inside the page.
        if (page.initialized && page.value_idx < page.num_values &&
            skipRowsInPage(row_idx, page, column, column_info))
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
        initializePage(ptr, end, page.next_row_idx, /*end_row_idx=*/ std::nullopt, row_idx, column, column_info);
        column.next_page_offset = ptr - all_pages.data();
    }
}

bool Reader::initializePage(const char * & data_ptr, const char * data_end, size_t next_row_idx, std::optional<size_t> end_row_idx, size_t target_row_idx, ColumnChunk & column, const PrimitiveColumnInfo & column_info)
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
    data_ptr += deserializeThriftStruct(header, data_ptr, data_end - data_ptr);
    /// TODO [parquet]: Check checksum.
    size_t compressed_page_size = size_t(header.compressed_page_size);
    if (header.compressed_page_size < 0 || compressed_page_size > size_t(data_end - data_ptr))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Page size out of bounds: {} > {}", header.compressed_page_size, data_end - data_ptr);
    page.data = std::span(data_ptr, compressed_page_size);
    data_ptr += compressed_page_size;

    /// Check if all rows of the page are filtered out, if we have enough information.

    std::optional<size_t> num_rows_in_page;
    if (header.type == parq::PageType::DATA_PAGE_V2)
        num_rows_in_page = header.data_page_header_v2.num_rows;
    else if (header.type == parq::PageType::DATA_PAGE &&
             column_info.levels.back().rep == 0) // no arrays => num_values == num_rows
        num_rows_in_page = header.data_page_header.num_values;

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

    page.codec = column.meta->meta_data.codec;
    page.values_uncompressed_size = header.uncompressed_page_size;

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
        page.num_values = header.data_page_header.num_values;
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

            UInt32 n;
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
        page.num_values = header.data_page_header_v2.num_values;
        page.encoding = header.data_page_header_v2.encoding;
        encoded_def_size = header.data_page_header_v2.definition_levels_byte_length;
        encoded_rep_size = header.data_page_header_v2.repetition_levels_byte_length;

        if (header.data_page_header_v2.__isset.is_compressed &&
            !header.data_page_header_v2.is_compressed)
        {
            page.codec = parq::CompressionCodec::UNCOMPRESSED;
        }

        if (encoded_def_size + encoded_rep_size > compressed_page_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Page data is too short (def+rep)");
        encoded_rep = page.data.data();
        encoded_def = page.data.data() + encoded_rep_size;
        size_t uncompressed_part = encoded_def_size + encoded_rep_size;
        page.data = page.data.subspan(uncompressed_part);
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

void Reader::readRowsInPage(size_t end_row_idx, ColumnSubchunk & subchunk, ColumnChunk & column, const PrimitiveColumnInfo & column_info)
{
    PageState & page = column.page;
    chassert(page.initialized && page.value_idx < page.num_values);

    /// Note: end_row_idx == page.next_row_idx doesn't necessarily mean we're done. E.g. suppose the
    /// row end_row_idx-1 contains an array that starts in page 0 and ends inside page 1.
    /// readRowsInPage in page 0 will reach end of page, with next_row_idx == end_row_idx. Then
    /// readRowsInPage in page 1 will continue until it sees the end of the array, i.e. the start of
    /// the next row (rep == 0), still with next_row_idx == end_row_idx.
    chassert(end_row_idx >= page.next_row_idx);

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
    if (!page.is_dictionary_encoded && prev_value_idx == 0 && page.value_idx == page.num_values &&
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

        if (page.is_dictionary_encoded)
        {
            if (!page.indices_column)
                page.indices_column = ColumnUInt32::create();
            auto & indices_column_uint32 = assert_cast<ColumnUInt32 &>(*page.indices_column);
            auto & data = indices_column_uint32.getData();
            chassert(data.empty());
            page.decoder->decode(encoded_values_to_read, *page.indices_column);
            column.dictionary.index(indices_column_uint32, *subchunk.column);
            data.clear();
        }
        else
        {
            page.decoder->decode(encoded_values_to_read, *subchunk.column);
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
    const OutputColumnInfo & output_info = output_columns.at(output_column_idx);
    TypeIndex kind = output_info.type->getColumnType();
    MutableColumnPtr res;

    if (output_info.is_missing_column)
    {
        res = output_info.type->createColumn();
        res->insertManyDefaults(num_rows);

        if (output_info.idx_in_output_block.has_value() &&
            /// If block_missing_values is enabled (not empty), and this column is not prewhere-only
            /// (idx < sample_block->columns()).
            *output_info.idx_in_output_block < row_subgroup.block_missing_values.getNumColumns())
        {
            row_subgroup.block_missing_values.setBits(*output_info.idx_in_output_block, num_rows);
        }
    }
    else if (output_info.is_primitive)
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
        res = ColumnTuple::create(std::move(columns));
    }
    else
    {
        chassert(kind == TypeIndex::Map);
        chassert(output_info.nested_columns.size() == 1);
        MutableColumnPtr nested = formOutputColumn(row_subgroup, output_info.nested_columns.at(0), num_rows);
        res = ColumnMap::create(std::move(nested));
    }

    chassert(res->getDataType() == output_info.type->getColumnType());
    return res;
}

void Reader::applyPrewhere(RowSubgroup & row_subgroup)
{
    for (size_t step_idx = 0; step_idx < prewhere_steps.size(); ++step_idx)
    {
        const PrewhereStep & step = prewhere_steps.at(step_idx);

        Block block;
        for (size_t output_idx : step.input_column_idxs)
        {
            const auto & output_info = output_columns.at(output_idx);
            auto & col = row_subgroup.output.at(output_info.idx_in_output_block.value());
            if (!col)
                col = formOutputColumn(row_subgroup, output_idx, row_subgroup.filter.rows_total);
            block.insert({col, output_info.type, output_info.name});
        }
        addDummyColumnWithRowCount(block, row_subgroup.filter.rows_total);

        step.actions.execute(block);

        ColumnPtr filter_column = block.getByName(step.result_column_name).column;

        if (step.idx_in_output_block.has_value())
            row_subgroup.output.at(step.idx_in_output_block.value()) = filter_column;

        /// If it's the last prewhere step, deallocate the columns that were only needed for prewhere.
        if (step_idx == prewhere_steps.size() - 1)
            row_subgroup.output.resize(sample_block->columns());

        if (!step.need_filter)
            continue;

        filter_column = FilterDescription::preprocessFilterColumn(std::move(filter_column));
        const IColumnFilter & filter = typeid_cast<const ColumnUInt8 &>(*filter_column).getData();
        chassert(filter.size() == row_subgroup.filter.rows_pass);

        size_t rows_pass = countBytesInFilter(filter.data(), 0, filter.size());
        if (rows_pass == 0)
        {
            /// Whole row group was filtered out.
            row_subgroup.filter.rows_pass = 0;
            return;
        }
        if (rows_pass == filter.size())
            /// Nothing was filtered out.
            continue;

        /// Filter columns that were already read.

        for (auto & col : row_subgroup.output)
            if (col)
                col = col->filter(filter, /*result_size_hint=*/ rows_pass);

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
