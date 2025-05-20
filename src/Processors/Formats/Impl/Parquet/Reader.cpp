#include <Processors/Formats/Impl/Parquet/Reader.h>
#include <Processors/Formats/Impl/Parquet/Decoding.h>
#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/castColumn.h>
#include <Common/thread_local_rng.h>
#include <Storages/SelectQueryInfo.h>
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

void Reader::init(const ReadOptions & options_, const Block & sample_block_, std::shared_ptr<const KeyCondition> key_condition_, PrewhereInfoPtr prewhere_info_)
{
    options = options_;
    sample_block = &sample_block_;
    key_condition = key_condition_;
    prewhere_info = prewhere_info_;
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

void Reader::prefilterAndInitRowGroups()
{
    extended_sample_block = *sample_block;
    if (prewhere_info)
    {
        auto add_columns = [&](const ActionsDAG & dag)
        {
            for (const auto & col : dag.getRequiredColumns())
            {
                if (!extended_sample_block.has(col.name))
                    extended_sample_block.insert({col.type->createColumn(), col.type, col.name});
            }
        };
        if (prewhere_info->row_level_filter.has_value())
            add_columns(prewhere_info->row_level_filter.value());
        add_columns(prewhere_info->prewhere_actions);
    }

    SchemaConverter schemer(file_metadata, options, &extended_sample_block);
    if (prewhere_info && !prewhere_info->remove_prewhere_column)
        schemer.external_columns.push_back(prewhere_info->prewhere_column_name);
    schemer.prepareForReading();
    primitive_columns = std::move(schemer.primitive_columns);
    total_primitive_columns_in_file = schemer.primitive_column_idx;
    output_columns = std::move(schemer.output_columns);

    if (key_condition)
    {
        /// TODO [parquet]: assign PrimitiveColumnInfo:: use_bloom_filter and use_column_index; possibly:
        /// Expect that either all or none of the column chunks have indexes written.
        /// (If that's not the case, nothing breaks, we may just pick suboptimal options here.)
    }

    bool use_offset_index = options.always_use_offset_index || prewhere_info
        || std::any_of(primitive_columns.begin(), primitive_columns.end(), [](const auto & c) { return c.use_column_index; });
    bool need_to_find_bloom_filter_lengths_the_hard_way = false;

    for (size_t row_group_idx = 0; row_group_idx < file_metadata.row_groups.size(); ++row_group_idx)
    {
        const auto * meta = &file_metadata.row_groups[row_group_idx];
        if (meta->num_rows <= 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Row group {} has <= 0 rows: {}", row_group_idx, meta->num_rows);
        if (meta->columns.size() != total_primitive_columns_in_file)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Row group {} has unexpected number of columns: {} != {}", row_group_idx, meta->columns.size(), total_primitive_columns_in_file);

        /// TODO [parquet]: Filtering.
        RowGroup & row_group = row_groups.emplace_back();
        row_group.meta = meta;
        row_group.row_group_idx = row_group_idx;
        row_group.columns.resize(primitive_columns.size());

        /// Initialize column chunks, mostly prefetches.
        for (size_t column_idx = 0; column_idx < primitive_columns.size(); ++column_idx)
        {
            ColumnChunk & column = row_group.columns[column_idx];
            size_t parquet_column_idx = primitive_columns[column_idx].column_idx;
            column.meta = &meta->columns.at(parquet_column_idx);

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
                if (dict_page_length < options.dictionary_filter_limit_bytes &&
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
            column.use_column_index = primitive_columns[column_idx].use_column_index
                && column.offset_index_prefetch
                && column.meta->__isset.column_index_offset && column.meta->__isset.column_index_length;
            if (column.use_column_index)
                column.column_index_prefetch = prefetcher.registerRange(
                    size_t(column.meta->column_index_offset),
                    size_t(column.meta->column_index_length), /*likely_to_be_used=*/ true);

            /// Data pages.
            column.data_pages_prefetch = prefetcher.registerRange(
                size_t(column.meta->meta_data.data_page_offset),
                size_t(column.meta->meta_data.total_compressed_size) - dict_page_length,
                /*likely_to_be_used=*/ true);

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

    if (need_to_find_bloom_filter_lengths_the_hard_way)
    {
        /// Parquet metadata doesn't have bloom filter sizes, but we want to know them (at least an
        /// upper bound) in advance, so that Prefetcher can coalesce it with other reads if it's small.
        /// Bloom filter ends when something else starts (or earlier). So we list all possible
        /// "something else" offsets and do binary search for each bloom filter to find where it ends.
        std::vector<size_t> all_offsets;
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
                if (!column.bloom_filter_header_prefetch)
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
    if (!prewhere_info)
        return;

    /// TODO: We currently run prewhere after reading all columns of the row group, in one thread
    ///       per row group. Instead, we could extract single-column conditions and run them after
    ///       decoding the corresponding columns, in parallel. (Still run multi-column conditions,
    ///       like `col1 = 42 or col2 = 'yes'`, after reading all columns.)
    ///       Probably reuse tryBuildPrewhereSteps from MergeTree for splitting the expression.

    /// Convert ActionsDAG to ExpressionActions.
    ExpressionActionsSettings actions_settings;
    if (prewhere_info->row_level_filter.has_value())
    {
        ExpressionActions actions(prewhere_info->row_level_filter->clone(), actions_settings);
        prewhere_steps.push_back(PrewhereStep
            {
                .actions = std::move(actions),
                .result_column_name = prewhere_info->row_level_column_name
            });
    }
    ExpressionActions actions(prewhere_info->prewhere_actions.clone(), actions_settings);
    prewhere_steps.push_back(PrewhereStep
        {
            .actions = std::move(actions),
            .result_column_name = prewhere_info->prewhere_column_name,
            .need_filter = prewhere_info->need_filter,
        });
    if (!prewhere_info->remove_prewhere_column)
        prewhere_steps.back().idx_in_output_block = sample_block->getPositionByName(prewhere_info->prewhere_column_name);

    /// Look up expression inputs in extended_sample_block.
    std::unordered_multimap</*idx in extended_sample_block*/ size_t, /*idx in prewhere_steps*/ size_t> required_columns;
    for (size_t step_idx = 0; step_idx < prewhere_steps.size(); ++step_idx)
    {
        auto & step = prewhere_steps[step_idx];
        for (const auto & col : step.actions.getRequiredColumnsWithTypes())
        {
            size_t idx = extended_sample_block.getPositionByName(col.name, /* case_insensitive= */ false);
            required_columns.emplace(idx, step_idx);
        }
    }

    /// Look up expression inputs in output_columns. Mark columns as required for prewhere.
    for (size_t output_idx = 0; output_idx < output_columns.size(); ++output_idx)
    {
        auto & col = output_columns[output_idx];
        if (!col.idx_in_output_block.has_value())
            continue;
        const auto steps = required_columns.equal_range(*col.idx_in_output_block);
        if (steps.first == steps.second)
            continue;

        col.use_prewhere = true;
        bool only_for_prewhere = *col.idx_in_output_block >= sample_block->columns();

        for (size_t primitive_idx = col.primitive_start; primitive_idx < col.primitive_end; ++primitive_idx)
        {
            primitive_columns[primitive_idx].use_prewhere = true;
            primitive_columns[primitive_idx].only_for_prewhere = only_for_prewhere;
        }

        for (auto it = steps.first; it != steps.second; ++it)
            prewhere_steps[it->second].input_column_idxs.push_back(output_idx);
    }
}

void Reader::processBloomFilterHeader(ColumnChunk & /*column*/, const PrimitiveColumnInfo & /*column_info*/)
{
    volatile bool f = true;
    if (f)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Bloom filters not implemented");
}

void Reader::decodeBloomFilterBlocks(ColumnChunk & /*column*/, const PrimitiveColumnInfo & /*column_info*/)
{
    volatile bool f = true;
    if (f)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Bloom filters not implemented");
}

void Reader::decodeDictionaryPage(ColumnChunk & column, const PrimitiveColumnInfo & column_info)
{
    auto data = prefetcher.getRangeData(column.dictionary_page_prefetch);
    parq::PageHeader header;
    size_t header_size = deserializeThriftStruct(header, data.data(), data.size());
    data = data.subspan(header_size);
    /// TODO: Check checksum.
    size_t compressed_page_size = size_t(header.compressed_page_size);
    if (header.compressed_page_size < 0 || compressed_page_size > data.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Dictionary page size out of bounds: {} > {}", header.compressed_page_size, data.size());
    data = data.subspan(0, size_t(header.compressed_page_size));

    if (header.type != parq::PageType::DICTIONARY_PAGE)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected dictionary page type: {}", thriftToString(header.type));

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

bool Reader::applyBloomAndDictionaryFilters(RowGroup & /*row_group*/)
{
    volatile bool f = true;
    if (f)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Bloom filters not implemented");
    return true;
}

void Reader::applyColumnIndex(ColumnChunk & /*column*/, const PrimitiveColumnInfo & /*column_info*/)
{
    volatile bool f = true;
    if (f)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Column index not implemented");
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

        for (const auto & col : row_group.columns)
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
                prev_end = end;

                events.emplace_back(start, +1);
                events.emplace_back(end, -1);
            }
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
    if (options.max_block_size > 0)
        rows_per_subgroup = std::min(rows_per_subgroup, options.max_block_size);

    if (options.preferred_block_size_bytes > 0)
    {
        double bytes_per_row = 0;
        for (size_t i = 0; i < primitive_columns.size(); ++i)
            bytes_per_row += estimateColumnMemoryBytesPerRow(row_group.columns.at(i), row_group, primitive_columns.at(i));

        size_t n = size_t(options.preferred_block_size_bytes / bytes_per_row);
        rows_per_subgroup = std::min(rows_per_subgroup, std::max(n, 1ul));
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
        }
    }
}

void Reader::decodeOffsetIndex(ColumnChunk & /*column*/)
{
    volatile bool f = true;
    if (f)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Offset index not implemented");
}

void Reader::determinePagesToRead(ColumnSubchunk & /*subchunk*/, RowSubgroup & /*row_subgroup*/, RowGroup & /*row_group*/)
{
    // TODO [parquet]: Use offset index and filter.
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
            /// at average string length in the column chunk.
            avg_string_length = column.dictionary.getAverageValueSize();
        }
        else
        {
            /// We have no idea how long the strings are.
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

void Reader::decodePrimitiveColumn(ColumnChunk & column_chunk, const PrimitiveColumnInfo & column_info, ColumnSubchunk & subchunk, const RowGroup & row_group, const RowSubgroup & row_subgroup)
{
    /// Allocate columns for values, null map, and array offsets.

    size_t output_num_values_estimate;
    if (column_info.levels.back().rep == 0)
        output_num_values_estimate = row_subgroup.filter.rows_pass; // no arrays, rows == values
    else if (row_subgroup.filter.rows_pass == size_t(row_group.meta->num_rows))
        output_num_values_estimate = column_chunk.meta->meta_data.num_values; // whole column chunk
    else
        /// There are arrays, so we can't know exactly how many primitive values there are in
        /// rows that pass the filter. Make a guess using average array length.
        output_num_values_estimate = size_t(1.2 * row_subgroup.filter.rows_pass / row_group.meta->num_rows * column_chunk.meta->meta_data.num_values);

    subchunk.arrays_offsets.resize(column_info.levels.back().rep);
    for (size_t i = 0; i < subchunk.arrays_offsets.size(); ++i)
    {
        subchunk.arrays_offsets[i] = ColumnArray::ColumnOffsets::create();
        subchunk.arrays_offsets[i]->reserve(i ? output_num_values_estimate : row_subgroup.filter.rows_total);
    }

    if (column_chunk.need_null_map)
    {
        subchunk.null_map = ColumnUInt8::create();
        subchunk.null_map->reserve(output_num_values_estimate);
    }

    subchunk.column = column_info.raw_decoded_type->createColumn();
    subchunk.column->reserve(output_num_values_estimate);
    if (auto * string_column = typeid_cast<ColumnString *>(subchunk.column.get()))
    {
        double avg_len = estimateAverageStringLengthPerRow(column_chunk, row_group);
        size_t bytes_to_reserve = size_t(1.2 * (avg_len + 1) * row_subgroup.filter.rows_pass);
        string_column->getChars().reserve(bytes_to_reserve);
    }

    /// Find ranges of rows that pass filter and decode them.

    size_t row_subidx = 0;
    while (true) // loop over row ranges that pass the filter
    {
        /// Find a range of rows that pass filter.
        /// TODO: We call decoder for each such range separately, with a bunch of overhead per call.
        ///       This will probably be slow on something like `PREWHERE idx%2=0`.
        ///       If it's too slow and/or comes up in practice or benchmarks, make decoders accept
        ///       a (optional) filter mask, like e.g. in commit c1a361d176507a19c2fdc49f0f1d6dc7e2cd539e.
        size_t num_rows = row_subgroup.filter.rows_total - row_subidx;
        if (!row_subgroup.filter.filter.empty())
        {
            /// TODO: simd or something
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

        skipToRow(start_row_idx, column_chunk, column_info);

        while (true) // loop over pages
        {
            readRowsInPage(end_row_idx, subchunk, column_chunk, column_info);

            /// We're done if we've reached end_row_idx and we're at a row boundary.
            auto & page = column_chunk.page;
            if (page.next_row_idx == end_row_idx &&
                (page.value_idx < page.num_values ||
                 page.end_row_idx.has_value() || // page ends on row boundary
                 column_chunk.next_page_offset == prefetcher.getRangeData(column_chunk.data_pages_prefetch).size()))
                break;

            /// Advance to next page.
            chassert(page.value_idx == page.num_values);
            skipToRow(page.next_row_idx, column_chunk, column_info);
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

    if (subchunk.null_map && !column_info.output_nullable && !options.null_as_default)
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

    if (subchunk.column->size() != row_subgroup.filter.rows_pass)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected number of rows in column subchunk");

    if (column_info.output_nullable)
    {
        if (!subchunk.null_map)
            subchunk.null_map = ColumnUInt8::create(subchunk.column->size(), 0);
        subchunk.column = ColumnNullable::create(std::move(subchunk.column), std::move(subchunk.null_map));
    }
    else
    {
        /// TODO [parquet]: Turn null_map into BlockMissingValues.
    }
    subchunk.null_map.reset();

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

void Reader::skipToRow(size_t row_idx, ColumnChunk & column_chunk, const PrimitiveColumnInfo & column_info)
{
    /// True if column_chunk.page is initialized and contains the requested row_idx.
    bool found_page = false;
    auto & page = column_chunk.page;

    if (page.initialized && page.value_idx < page.num_values && page.end_row_idx.has_value() && *page.end_row_idx > row_idx)
        /// Fast path: we're just continuing reading the same page as before.
        found_page = true;

    if (!found_page && !column_chunk.data_pages.empty())
    {
        /// If we have offset index, find the row index there and jump to the correct page.
        while (column_chunk.data_pages_idx < column_chunk.data_pages.size() &&
            column_chunk.data_pages[column_chunk.data_pages_idx].end_row_idx <= row_idx)
            ++column_chunk.data_pages_idx;
        if (column_chunk.data_pages_idx == column_chunk.data_pages.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet offset index covers too few rows");
        const auto & page_info = column_chunk.data_pages[column_chunk.data_pages_idx];
        size_t first_row_idx = size_t(page_info.meta->first_row_index);
        /// TODO [parquet]: Remember to check that row ranges don't overlap when loading offset index.
        if (first_row_idx > row_idx)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Row passes filters but its page was not selected for reading. This is a bug.");

        auto data = prefetcher.getRangeData(page_info.prefetch);
        const char * ptr = data.data();
        if (!initializePage(ptr, ptr + data.size(), first_row_idx, page_info.end_row_idx, row_idx, column_chunk, column_info))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Page doesn't contain requested row");
        found_page = true;
    }

    while (true)
    {
        /// Skip rows inside the page.
        if (page.initialized && page.value_idx < page.num_values &&
            skipRowsInPage(row_idx, page, column_chunk, column_info))
            return;

        if (found_page)
            /// This was supposed to be the correct page.
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected end of page");

        /// Advance to the next page.
        chassert(column_chunk.data_pages.empty());
        auto all_pages = prefetcher.getRangeData(column_chunk.data_pages_prefetch);
        chassert(column_chunk.next_page_offset <= all_pages.size());
        const char * ptr = all_pages.data() + column_chunk.next_page_offset;
        const char * end = all_pages.data() + all_pages.size();
        initializePage(ptr, end, page.next_row_idx, /*end_row_idx=*/ std::nullopt, row_idx, column_chunk, column_info);
        column_chunk.next_page_offset = ptr - all_pages.data();
    }
}

bool Reader::initializePage(const char * & data_ptr, const char * data_end, size_t next_row_idx, std::optional<size_t> end_row_idx, size_t target_row_idx, ColumnChunk & column_chunk, const PrimitiveColumnInfo & column_info)
{
    PageState & page = column_chunk.page;
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
    /// TODO: Check checksum.
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

        if (next_row_idx + *num_rows_in_page <= target_row_idx)
            return false;

        page.end_row_idx = next_row_idx + *num_rows_in_page;
    }

    /// Get information about page layout and encoding out of page header.

    page.codec = column_chunk.meta->meta_data.codec;
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
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected dictionary page");
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
    if (page.is_dictionary_encoded && !column_chunk.dictionary.isInitialized())
    {
        if (column_chunk.meta->meta_data.__isset.dictionary_page_offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary not initialized");
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected dictionary-encoded page in column chunk with no dictionary");
    }

    /// Decode rep/def levels.

    /// TODO: Maybe skip decoding def if column statistics say there are not nulls, and max_rep == 0.
    UInt8 max_def = column_info.levels.back().def;
    UInt8 max_rep = column_info.levels.back().rep;

    decodeRepOrDefLevels(rep_encoding, max_rep, page.num_values, std::span(encoded_rep, encoded_rep_size), page.rep);

    /// Don't decode def levels in the common case of non-array column that's declared nullable but
    /// contains no nulls.
    if (max_rep > 0 || column_chunk.need_null_map)
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

void Reader::createPageDecoder(PageState & page, ColumnChunk & column_chunk, const PrimitiveColumnInfo & column_info)
{
    if (page.is_dictionary_encoded)
        page.decoder = makeDictionaryIndicesDecoder(page.encoding, column_chunk.dictionary.count, page.data);
    else
        page.decoder = column_info.decoder.makeDecoder(page.encoding, page.data);
}

/// Returns true if this row is found in this page, and value_idx is at the first value of this row.
/// False if we reached the end of the page without reaching this row index; next_row_idx is set
/// accordingly.
bool Reader::skipRowsInPage(size_t target_row_idx, PageState & page, ColumnChunk & column_chunk, const PrimitiveColumnInfo & column_info)
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
            createPageDecoder(page, column_chunk, column_info);
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
template <bool HAS_ARRAYS, bool HAS_NULLS>
static void processDefLevelsForInnermostColumn(
    size_t num_values, const UInt8 * def, UInt8 max_def, UInt8 max_array_def, size_t & out_num_encoded_values, ColumnUInt8::Container * out_null_map)
{
    size_t num_encoded_values = 0;
    for (size_t i = 0; i < num_values; ++i)
    {
        if constexpr (HAS_ARRAYS)
            if (def[i] < max_array_def)
                continue; // empty array

        bool is_null = false;
        if constexpr (HAS_NULLS)
        {
            is_null = def[i] != max_def;
            out_null_map->push_back(is_null);
        }

        num_encoded_values += !is_null;
    }
    out_num_encoded_values = num_encoded_values;
}

/// Produces array offsets at a given level of nested arrays.
/// TODO: Try simdifying.
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
            ///    TODO: Should we throw an error in this case if !options.null_as_default?
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

void Reader::readRowsInPage(size_t end_row_idx, ColumnSubchunk & subchunk, ColumnChunk & column_chunk, const PrimitiveColumnInfo & column_info)
{
    PageState & page = column_chunk.page;
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
#define X(HAS_ARRAYS, HAS_NULLS, null_map) \
            processDefLevelsForInnermostColumn<HAS_ARRAYS, HAS_NULLS>( \
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

    decompressPageIfCompressed(page);
    if (!page.decoder)
        createPageDecoder(page, column_chunk, column_info);

    if (page.is_dictionary_encoded)
    {
        if (!page.indices_column)
            page.indices_column = ColumnUInt32::create();
        auto & data = assert_cast<ColumnUInt32 &>(*page.indices_column).getData();
        chassert(data.empty());
        page.decoder->decode(encoded_values_to_read, *page.indices_column);
        column_chunk.dictionary.index(data, *subchunk.column);
        data.clear();
    }
    else
    {
        page.decoder->decode(encoded_values_to_read, *subchunk.column);
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

MutableColumnPtr Reader::formOutputColumn(RowSubgroup & row_subgroup, size_t output_column_idx)
{
    const OutputColumnInfo & output_info = output_columns.at(output_column_idx);
    chassert(output_info.primitive_start < output_info.primitive_end);
    TypeIndex kind = output_info.type->getColumnType();
    MutableColumnPtr res;

    if (output_info.is_primitive)
    {
        /// Primitive column.
        chassert(output_info.primitive_start + 1 == output_info.primitive_end);
        size_t primitive_idx = output_info.primitive_start;
        ColumnSubchunk & subchunk = row_subgroup.columns.at(primitive_idx);
        res = std::move(subchunk.column);
    }
    else if (kind == TypeIndex::Array)
    {
        chassert(output_info.nested_columns.size() == 1);
        auto offsets_column = std::move(row_subgroup.columns.at(output_info.primitive_start).arrays_offsets.at(output_info.rep - 1));
        const auto & offsets = assert_cast<const ColumnUInt64 &>(*offsets_column).getData();

        /// If it's an array of tuples, every tuple element should have the same array offsets.
        for (size_t i = output_info.primitive_start + 1; i < output_info.primitive_end; ++i)
        {
            const auto other_offsets_column = std::move(row_subgroup.columns.at(i).arrays_offsets.at(output_info.rep - 1));
            const auto & other_offsets = assert_cast<const ColumnUInt64 &>(*other_offsets_column).getData();
            if (offsets != other_offsets)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid array of tuples: tuple elements {} and {} have different array lengths", primitive_columns.at(output_info.primitive_start).name, primitive_columns.at(i).name);
        }

        MutableColumnPtr nested = formOutputColumn(row_subgroup, output_info.nested_columns.at(0));
        res = ColumnArray::create(std::move(nested), std::move(offsets_column));
    }
    else if (kind == TypeIndex::Tuple)
    {
        MutableColumns columns;
        for (size_t idx : output_info.nested_columns)
            columns.push_back(formOutputColumn(row_subgroup, idx));
        res = ColumnTuple::create(std::move(columns));
    }
    else
    {
        chassert(kind == TypeIndex::Map);
        chassert(output_info.nested_columns.size() == 1);
        MutableColumnPtr nested = formOutputColumn(row_subgroup, output_info.nested_columns.at(0));
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
                col = formOutputColumn(row_subgroup, output_idx);
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

        filter_column = filter_column->convertToFullIfNeeded();
        if (filter_column->isNullable())
        {
            /// Calculate `filter->nested & !filter->null_map`.
            auto col = IColumn::mutate(std::move(filter_column));
            auto & nullable = typeid_cast<ColumnNullable &>(*col);
            const auto & null_map = nullable.getNullMapData();
            auto nested_col = IColumn::mutate(std::move(nullable.getNestedColumnPtr()));
            auto & nested_data = typeid_cast<ColumnUInt8 &>(*nested_col).getData();
            chassert(nested_data.size() == null_map.size());
            for (size_t i = 0; i < nested_data.size(); ++i)
                nested_data[i] &= !null_map[i];
            nullable.getNullMapColumnPtr().reset();
            filter_column = std::move(nested_col);
        }

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
