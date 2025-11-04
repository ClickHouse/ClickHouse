#pragma once

#include <Columns/IColumn.h>
#include <Core/BlockMissingValues.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/Parquet/Decoding.h>
#include <Processors/Formats/Impl/Parquet/Prefetcher.h>
#include <Processors/Formats/Impl/Parquet/ReadCommon.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <deque>
#include <optional>

namespace DB
{
class Block;
struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;
}

namespace DB::Parquet
{

// TODO [parquet]:
//  * either multistage PREWHERE or make query optimizer selectively move parts of the condition to prewhere instead of the whole condition
//  * test on files from https://github.com/apache/parquet-testing and https://www.timestored.com/data/sample/parquet
//  * look at issues in 00900_long_parquet_load.sh
//  * check fields for false sharing, add cacheline padding as needed
//  * make sure userspace page cache read buffer supports readBigAt
//  * support newer parquet versions: https://github.com/apache/parquet-format/blob/master/CHANGES.md
//  * make writer write DataPageV2
//  * make writer write PageEncodingStats
//  * make writer write DELTA_LENGTH_BYTE_ARRAY
//  * try adding [[unlikely]] to all ifs
//  * try adding __restrict to pointers on hot paths
//  * support or deprecate the preserve-order setting
//  * add comments everywhere
//  * progress indication and estimating bytes to read; allow negative total_bytes_to_read?
//  * cache FileMetaData in something like SchemaCache
//  * TSA
//  * test with tsan
//  * use dictionary page instead of bloom filter when possible
//  * test performance with prewhere i%2=0, to have lots of range skips
//  * check if prewhere works with datalakes
//  * remember to test these:
//     - expression appearing both in prewhere and in select list (presumably remove_prewhere_column = false)
//     - prewhere with all 3 storages (maybe have the main big test pick random storage)
//     - prewhere and where together
//     - reading tuple element without reading the whole tuple
//     - `select t, t.x`, `select t.x.x, t.x`, `select t.x.dyn_col`, `select t.`1``
//        -- somehow assert that only the expected subcolumns are read
//        -- also tuples inside arrays:
//           insert into function file('p4.parquet') select [((number*100 + 10,number*100+11),number*100+20)::Tuple(x Tuple(x Int64, y String), y Int64)] as a from numbers(3)
//     - prewhere with tuple element access (prepareReadingFromFormat looks like it won't work)
//     - prewhere with expression string equal to some (weird) column name in the file
//     - prewhere with expression that's just a (UInt8) column name, present or not present in select
//     - prewhere condition that uses no columns (e.g. rand()%2=0)
//     - prewhere condition that uses all columns (e.g. select x prewhere x%2=0)
//     - no columns to read outside prewhere
//     - no columns to read, but not trivial count either
//     - ROW POLICY, with and without prewhere, with old and new reader
//     - prewhere with defaults (it probably doesn't fill them correctly, see MergeTreeRangeReader::executeActionsBeforePrewhere)
//     - prewhere on virtual columns (do they end up in additional_columns?)
//     - prewhere with weird filter type (LowCardinality(UInt8), Nullable(UInt8), const UInt8)
//     - prewhere involving arrays and tuples
//     - prewhere with url() and s3()
//     - tuple elements with url() and s3()
//     - IN with subqueries with and without prewhere
//     - compare performance to MergeTree (full scan, prewhere, skipping granules)
//     - `insert into function file('t.parquet') select number as k, toString(number)||':'||randomPrintableASCII(1000) as v from numbers(1000000) settings engine_file_truncate_on_insert=1; select count(), sum(length(v)) from file('t.parquet')` - new reader is slower than default
//     - array touching >2 pages
//     - bf and page filtering on prewhere-only columns
//     - all types of filtering by IS NULL / IS NOT NULL
//     - Bool type
//     - looking for NaN using min/max indices; try it with merge tree too
//     - input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference, make a file by hacking PrepareForWrite.cpp to use garbage physical type for some data type (~/t/invalid_type.parquet has column `invalid` that's FixedString(5) but has type=42 instead of type=FIXED_LEN_BYTE_ARRAY)
//  * write a comment explaining the advantage of the weird complicated two-step scheduling
//    (tasks_to_schedule -> task queue -> run) and per-stage memory accounting - maximizing prefetch
//    parallelism; contrast with a simpler strategy of having no queues, and worker threads e.g.
//    choosing the column with least <row group idx, subgroup idx> among runnable ones, with one total
//    memory budget; but also think about memory locality - is it worse than for simple solution?
//    is there a way to make it better or to support simple solution as special case?
//  * lazy materialization (not feasible because of unaligned pages?)
//  * add prewhere check next to addNumRowsToCache calls, in addition to key condition check
//  * make sure ~all reader and writer settings are randomized in tests

/// Components of this parquet reader implementation:
///  * Prefetcher is responsible for coalescing nearby short reads into bigger reads.
///    It needs to know an approximate set of all needed ranges in advance, which we can produce
///    from parquet file metadata.
///  * FormatParserSharedResources can be shared across multiple parquet file readers belonging
///    to the same query, e.g. when doing `SELECT * FROM url('.../part_{0..999}.parquet')`.
///    Splits the memory and thread count budgets among the readers. Important because we want to
///    use much more memory per reader when reading one file than when reading 100 files in parallel.
///  * SchemaConverter traverses parquet schema tree and figures out which primitive columns to read,
///    how to assemble them into compound columns (e.g. arrays or tuples), and what typecasts are
///    needed.
///  * Decoding.{h,cpp} implement parsing of all the parquet data encodings.
///  * Reader implements decoding, filtering, and assembling primitive columns into final columns.
///    Doesn't have a clean API, needs to be micromanaged by ReadManager (to minimize boilerplate and
///    distractions in Reader, which is the most complex part).
///  * ReadManager drives the Reader. Responsible for scheduling work to threads, thread safety,
///    limiting memory usage, and delivering output.

struct Reader
{
    /// Every parquet primitive column can be thought of as wrapped in zero or more levels of
    /// Nullable or Array types.
    /// Each level has a definition level number, and each Array level has a repetition level number,
    /// numbered from outer to inner. This struct describes one of these levels.
    /// Additionally, for convenience, we add a "root" LevelInfo with def = rep = 0, as if the whole
    /// column chunk is an Array of rows.
    /// E.g. Array(Nullable(Nullable(Array(Array(Nullable(Int64)))))) has `levels`:
    ///  +-----+-----+----------+
    ///  | def | rep | is_array |
    ///  +-----+-----+----------+
    ///  |  0  |  0  |  true    |  (fake root element for convenience)
    ///  |  1  |  1  |  true    |  (outermost Array)
    ///  |  2  |  1  |  false   |
    ///  |  3  |  1  |  false   |
    ///  |  4  |  2  |  true    |
    ///  |  5  |  3  |  true    |
    ///  |  6  |  3  |  false   |  (innermost Nullable)
    ///  +-----+-----+----------+
    ///
    /// All compound types reduce to this structure. E.g. Map is an array of 2-tuples, and array of
    /// tuples is a groups of parallel Array columns.
    ///
    /// ClickHouse doesn't support Nullable non-primitive columns, so we turn NULL arrays into
    /// empty arrays at decoding time. This transformation can be done just by modifying
    /// repetition/definition levels.
    ///
    /// For each primitive parquet column, we produce a primitive ClickHouse column (possibly Nullable)
    /// and offsets for each Array level. After decoding all primitive columns, we bundle them into
    /// compound columns as needed.
    struct LevelInfo
    {
        UInt8 def = 0; // equal to index in `levels`
        UInt8 rep = 0;
        /// If true, it's an Array level. If false, it's a Nullable level.
        /// Also true for the root level - the whole table can be seen as an array of rows.
        bool is_array = false;
    };

    /// Primitive (e.g. integer or string) parquet column that we need to read.
    /// Note that the resulting clickhouse column is not necessarily primitive, e.g. we may parse
    /// json or geoparquet column from parquet string column.
    struct PrimitiveColumnInfo
    {
        /// Primitive column index in parquet file. NOT index in primitive_columns array.
        size_t column_idx;
        /// Index in parquet `schema` (in FileMetaData).
        size_t schema_idx;
        String name; // possibly mapped by ColumnMapper (e.g. using iceberg metadata)
        PageDecoderInfo decoder;

        DataTypePtr raw_decoded_type; // not Nullable
        DataTypePtr intermediate_type; // maybe Nullable
        DataTypePtr final_type; // castColumn to this type
        bool output_nullable = false;
        /// TODO [parquet]: Consider also adding output_low_cardinality to allow producing LowCardinality
        ///       column directly from parquet dictionary+indices. This is not straightforward
        ///       because ColumnLowCardinality requires values to be unique and the first value to
        ///       be default. So we'd need to validate uniqueness and add/move default value
        ///       (adjusting indices and dictionary).
        bool needs_cast = false; // if final_type is different from intermediate_type

        /// How to interpret repetition/definition levels.
        std::vector<LevelInfo> levels;
        /// Definition level of innermost array. I.e. max levels[i].def where levels[i].is_array.
        UInt8 max_array_def = 0;

        bool use_bloom_filter = false;
        const KeyCondition * column_index_condition = nullptr;
        bool use_prewhere = false;
        bool only_for_prewhere = false; // can remove this column after applying prewhere

        std::optional<size_t> used_by_key_condition; // index in extended_sample_block

        /// If use_bloom_filter, these are the values that we need to find in bloom filter.
        std::vector<UInt64> bloom_filter_hashes;

        PrimitiveColumnInfo() = default;
        PrimitiveColumnInfo(PrimitiveColumnInfo &&) = default;
        ~PrimitiveColumnInfo();
    };

    struct OutputColumnInfo
    {
        String name; // possibly mapped by ColumnMapper
        /// Range in primitive_columns.
        size_t primitive_start = 0;
        size_t primitive_end = 0;
        DataTypePtr type;
        std::optional<size_t> idx_in_output_block;
        std::vector<size_t> nested_columns;
        bool is_primitive = false;
        /// Column not in the file, fill it with default values.
        bool is_missing_column = false;

        /// If type is Array, this is the repetition level of that array.
        /// `rep - 1` is index in ColumnChunk::arrays_offsets.
        UInt8 rep = 0;

        bool use_prewhere = false;
    };

    struct RowSet
    {
        size_t rows_total = 0;
        size_t rows_pass = 0;
        /// Can be empty if rows_pass is equal to 0 or rows_total.
        /// TODO [parquet]: Consider bitmask for faster range operations. See also: ColumnsCommon.h
        IColumnFilter filter;

        MemoryUsageToken memory;

        void clear(MemoryUsageDiff * diff)
        {
            filter = {};
            memory.reset(diff);
        }
    };

    struct BloomFilterBlock
    {
        size_t block_idx;
        PrefetchHandle prefetch;
    };

    struct DataPage
    {
        const parq::PageLocation * meta;
        size_t end_row_idx = 0;
        PrefetchHandle prefetch {};
    };

    struct PageState
    {
        bool initialized = false;

        /// Due to the nature of rep/def levels, row numbering is a little weird here.
        /// Value at `value_idx - 1` has row index `next_row_idx - 1`.
        /// (That value may be in previous page.)
        /// (If we're at the start of the first page, next_row_idx = 0.)
        /// If value_idx is the start of a row (i.e. rep[value_idx] == 0) then value at value_idx
        /// has row index next_row_idx.
        /// In particular, if there are no arrays (max_rep == 0) then "row" and "value" mean the
        /// same thing, and `next_row_idx` is just the row index (numbered from the start of
        /// column chunk) corresponding to `value_idx` (numbered from the start of page).
        /// (Why not just have row_idx corresponding to value at value_idx? Because when we reach the
        ///  end of a page we don't know whether the first value of next page starts a new row or not.
        ///  At the end of a page we end up with value_idx == num_values, next_row_idx - last row of
        ///  this page. Then we start the next page with the same next_row_idx, and value_idx == 0.)
        size_t next_row_idx = 0;
        std::optional<size_t> end_row_idx;

        /// Points either into `decompressed` or into Prefetcher's memory (kept alive by
        /// PrefetchHandle in ColumnChunk::data_pages or data_pages_prefetch).
        /// Either way the data is padded for simd.
        std::span<const char> data;
        parq::Encoding::type encoding;

        std::unique_ptr<PageDecoder> decoder;
        bool is_dictionary_encoded = false;

        /// If data_state is still compressed. We always decompress it before calling the decoder.
        /// Decompression is deferred a little to see if we can decompress directly into IColumn.
        parq::CompressionCodec::type codec;
        size_t values_uncompressed_size = 0;

        /// Empty if the corresponding max rep/def level is 0.
        /// `def` can also be empty if there are no nulls, i.e. def[i] == max_def should be assumed.
        PaddedPODArray<UInt8> def;
        PaddedPODArray<UInt8> rep;

        size_t value_idx = 0;
        /// num_values is "Number of values, including NULLs". Aka number of definition levels.
        /// Number of actually encoded values is `num_values - num_nulls`, where num_nulls is count
        /// of def[i] < max_def. (Parquet "NULLs" include empty arrays.)
        size_t num_values = 0;

        PaddedPODArray<char> decompressed_buf;
        MutableColumnPtr indices_column; // if is_dictionary_encoded; ColumnUInt32
    };

    struct ColumnChunk
    {
        const parq::ColumnChunk * meta;

        bool use_bloom_filter = false;
        bool use_dictionary_filter = false;
        bool use_column_index = false;
        bool need_null_map = false;

        /// Prefetches.
        /// TODO [parquet]: Check that all handles and tokens are reset after correct stages.
        PrefetchHandle bloom_filter_header_prefetch;
        PrefetchHandle bloom_filter_data_prefetch;
        PrefetchHandle dictionary_page_prefetch;
        PrefetchHandle column_index_prefetch;
        PrefetchHandle offset_index_prefetch;
        PrefetchHandle data_pages_prefetch;
        size_t data_pages_bytes = 0;

        /// Smaller prefetches for parts of bloom_filter_data_prefetch that we actually need.
        std::vector<BloomFilterBlock> bloom_filter_blocks;

        /// Smaller prefetches for pages inside data_pages_prefetch that we actually need.
        /// Based on offset index.
        /// Empty if we're not using offset index and should use data_pages_prefetch instead.
        /// We preregister data_pages_prefetch in Prefetcher before we know page byte ranges,
        /// then split the range into smaller ranges if needed. If the whole data_pages_prefetch
        /// is small and very close to other ranges (e.g. if column data is right next to offset
        /// index), Prefetcher may read it incidentally; then the `pages` prefetch ranges won't
        /// do any additional reading and will just point into the already-read bigger range.
        std::vector<DataPage> data_pages;

        parq::BloomFilterHeader bloom_filter_header;
        parq::OffsetIndex offset_index;
        /// Dictionary page contents.
        /// May be loaded early if we decide to use it for filtering (instead of bloom filter).
        /// Otherwise, loaded just before the first dictionary-encoded data page (so if we end up
        /// skipping all dictionary-encoded data pages, we never read the dictionary).
        /// Note that older parquet writers may omit dictionary info in file metadata, so we don't
        /// necessarily know in advance whether the column chunk has a dictionary.
        Dictionary dictionary;

        std::vector<std::pair</*start*/ size_t, /*end*/ size_t>> row_ranges_after_column_index;

        PageState page; // TODO [parquet]: deallocate when column chunk is done (and check other fields too)
        /// Offset from the start of `data_pages_prefetch`, if not using offset index (`data_pages` is empty).
        size_t next_page_offset = 0;
        size_t data_pages_idx = 0; // corresponding to `page`
        /// Index in data_pages up to which we checked which pages need to be read, after applying prewhere.
        size_t data_pages_prefetch_idx = 0;

        ReadStage stage;
    };

    struct ColumnSubchunk
    {
        /// Primitive column.
        MutableColumnPtr column;

        MutableColumnPtr null_map;

        /// If this primitive column is inside an array, this is the offsets for `ColumnArray`s at
        /// all nesting levels, from outer to inner. Index is repetition level - 1.
        /// Derived from parquet's repetition/definition levels. See comment on LevelInfo.
        /// ("Arrays offsets" is intentionally grammatically incorrect to emphasize that it's a
        ///  list of lists.)
        std::vector<MutableColumnPtr> arrays_offsets;

        /// Covers `column`, `arrays_offsets`, and also RowSubgroup::output (data can be moved from
        /// the former to the latter).
        MemoryUsageToken column_and_offsets_memory;
    };

    struct RowSubgroup
    {
        /// Subgroup corresponds to range of rows [start_row_idx, start_row_idx + filter.rows_total)
        /// within the row group.
        /// Rows are numbered before any filtering, i.e. these row numbers are comparable to row
        /// numbers in offset index (PageLocation::first_row_index in DataPage).
        /// Subgroup row ranges are decided after inspecting column index but before prewhere.
        /// Subgroups don't necessarily cover all rows.
        size_t start_row_idx = 0;

        /// Initially `filter` is based only on column index, then it's updated after running prewhere.
        RowSet filter;

        std::vector<ColumnSubchunk> columns;
        BlockMissingValues block_missing_values;

        Columns output; // parallel to extended_sample_block

        std::atomic<ReadStage> stage {ReadStage::NotStarted};
        std::atomic<size_t> stage_tasks_remaining {0};
    };

    struct RowGroup
    {
        const parq::RowGroup * meta;

        size_t row_group_idx; // in parquet file
        size_t start_global_row_idx = 0; // total number of rows in preceding row groups in the file

        /// Parallel to Reader::primitive_columns.
        /// NOT parallel to `meta.columns` (it's a subset of parquet columns).
        std::vector<ColumnChunk> columns;

        Hyperrectangle hyperrectangle; // min/max for each column; parallel to extended_sample_block

        std::deque<RowSubgroup> subgroups;

        std::vector<std::pair</*start*/ size_t, /*end*/ size_t>> intersected_row_ranges_after_column_index;


        /// Fields below are used only by ReadManager.

        /// Indexes of the first subgroup that didn't finish
        /// {prewhere, reading main columns, delivering final chunk}.
        /// delivery_ptr <= read_ptr <= prewhere_ptr <= subgroups.size()
        std::atomic<size_t> prewhere_ptr {0};
        std::atomic<size_t> read_ptr {0};
        std::atomic<size_t> delivery_ptr {0};

        std::atomic<ReadStage> stage {ReadStage::NotStarted};
        std::atomic<size_t> stage_tasks_remaining {0};
    };

    struct PrewhereStep
    {
        ExpressionActions actions;
        String result_column_name;
        std::vector<size_t> input_column_idxs {}; // indices in output_columns
        std::optional<size_t> idx_in_output_block = std::nullopt;
        bool need_filter = true;
    };

    ReadOptions options;
    const Block * sample_block;
    FormatFilterInfoPtr format_filter_info;
    Prefetcher prefetcher;

    parq::FileMetaData file_metadata;
    std::deque<RowGroup> row_groups;

    /// Don't get confused in different column numberings (sorry there are so many):
    ///  * In parquet metadata, columns are listed in array `schema`.
    ///    PrimitiveColumnInfo::schema_idx is index in that array.
    ///    This includes compound columns (e.g. arrays or tuples), their constituent primitive columns,
    ///    and various boilerplate (e.g. map key-value tuple, separately from the map itself).
    ///  * In parquet row group metadata, `columns` lists only primitive columns - a subsequence of `schema`.
    ///    PrimitiveColumnInfo::column_idx is index in that array.
    ///  * `extended_sample_block` lists the columns that were requested by the SQL query.
    ///    OutputColumnInfo::idx_in_output_block is index of the column in this block.
    ///    `sample_block`'s list of columns is a prefix of `extended_sample_block`'s list of columns.
    ///    Columns may have compound types, e.g. Tuple.
    ///  * `primitive_columns` is the list of columns we need to physically read, in some order.
    ///    For compound columns, this list has their primitive sub-columns (e.g. elements of Tuple),
    ///    but not the compound columns themselves.
    ///    Arrays RowGroup::columns and RowSubgroup::columns are parallel to `primitive_columns`.
    ///  * `output_columns` contains the instructions for how to go from primitive columns to output
    ///    columns. E.g. for Array(Array(Int64)) there would be 3 elements in output_columns:
    ///    Int64 (pointing to an element of primitive_columns), Array(Int64), and Array(Array(Int64)).

    std::vector<PrimitiveColumnInfo> primitive_columns;
    size_t total_primitive_columns_in_file = 0;
    std::vector<OutputColumnInfo> output_columns;
    /// Maps idx_in_output_block to index in output_columns. I.e.:
    ///     sample_block_to_output_columns_idx[output_columns[i].idx_in_output_block] = i
    /// nullopt if the column is produced by PREWHERE expression:
    ///     prewhere_steps[?].idx_in_output_block == i
    std::vector<std::optional<size_t>> sample_block_to_output_columns_idx;

    /// sample_block with maybe some columns added at the end.
    /// The added columns are used as inputs to prewhere expression, then discarded.
    /// (Why not just add them to sample_block? To avoid unnecessarily applying filter to them.)
    Block extended_sample_block;
    DataTypes extended_sample_block_data_types; // = extended_sample_block.getDataTypes()
    std::vector<PrewhereStep> prewhere_steps;

    std::optional<KeyCondition> bloom_filter_condition;

    /// These methods are listed in the order in which they're used, matching ReadStage order.

    void init(const ReadOptions & options_, const Block & sample_block_, FormatFilterInfoPtr format_filter_info_);

    static parq::FileMetaData readFileMetaData(Prefetcher & prefetcher);
    void prefilterAndInitRowGroups();
    void preparePrewhere();

    /// Deserialize bf header and determine which bf blocks to read.
    void processBloomFilterHeader(ColumnChunk & column, const PrimitiveColumnInfo & column_info);
    /// Returns false if it turned out that `dictionary_page_prefetch` is not actually a dictionary.
    bool decodeDictionaryPage(ColumnChunk & column, const PrimitiveColumnInfo & column_info);

    /// Returns false if the row group was filtered out and should be skipped.
    bool applyBloomAndDictionaryFilters(RowGroup & row_group);

    void applyColumnIndex(ColumnChunk & column, const PrimitiveColumnInfo & column_info, const RowGroup & row_group);
    void intersectColumnIndexResultsAndInitSubgroups(RowGroup & row_group);

    void decodeOffsetIndex(ColumnChunk & column, const RowGroup & row_group);
    /// Call after prewhere is done on row subgroup. Un-requests prefetch for fully filtered out pages,
    /// adds pages that need prefetch to `out`. Must be called in order.
    /// May assign dictionary_page_prefetch.
    void determinePagesToPrefetch(ColumnChunk & column, const RowSubgroup & row_subgroup, const RowGroup & row_group, std::vector<PrefetchHandle *> & out);

    /// Guess how much memory ColumnSubchunk::{column, arrays_offsets} will use, per row.
    double estimateColumnMemoryBytesPerRow(const ColumnChunk & column, const RowGroup & row_group, const PrimitiveColumnInfo & column_info) const;

    void decodePrimitiveColumn(ColumnChunk & column, const PrimitiveColumnInfo & column_info, ColumnSubchunk & subchunk, const RowGroup & row_group, const RowSubgroup & row_subgroup);

    /// Returns mutable column because some of the recursive calls require it,
    /// e.g. ColumnArray::create does assumeMutable() on the nested columns.
    /// Moves the column out of ColumnSubchunk-s, leaving nullptrs in ColumnSubchunk::column.
    /// The caller is responsible for caching the result (in RowSubGroup::output) to make sure this
    /// is not called again for the moved-out columns.
    MutableColumnPtr formOutputColumn(RowSubgroup & row_subgroup, size_t output_column_idx, size_t num_rows);

    void applyPrewhere(RowSubgroup & row_subgroup);

private:
    struct BloomFilterLookup : public KeyCondition::BloomFilter
    {
        Prefetcher & prefetcher;
        ColumnChunk & column;

        BloomFilterLookup(Prefetcher & prefetcher_, ColumnChunk & column_) : prefetcher(prefetcher_), column(column_) {}

        bool findAnyHash(const std::vector<uint64_t> & hashes) override;
    };

    void getHyperrectangleForRowGroup(const parq::RowGroup * meta, Hyperrectangle & hyperrectangle) const;
    void adjustRangeFromIndexIfNeeded(Range & range, const PrimitiveColumnInfo & column_info, bool can_be_null) const;
    void prepareBloomFilterCondition();
    void initializePrefetches();
    double estimateAverageStringLengthPerRow(const ColumnChunk & column, const RowGroup & row_group) const;
    void decodeDictionaryPageImpl(const parq::PageHeader & header, std::span<const char> data, ColumnChunk & column, const PrimitiveColumnInfo & column_info);
    void skipToRow(size_t row_idx, ColumnChunk & column, const PrimitiveColumnInfo & column_info);
    bool initializePage(const char * & data_ptr, const char * data_end, size_t next_row_idx, std::optional<size_t> end_row_idx, size_t target_row_idx, ColumnChunk & column, const PrimitiveColumnInfo & column_info);
    void decompressPageIfCompressed(PageState & page);
    void createPageDecoder(PageState & page, ColumnChunk & column, const PrimitiveColumnInfo & column_info);
    bool skipRowsInPage(size_t target_row_idx, PageState & page, ColumnChunk & column, const PrimitiveColumnInfo & column_info);
    void readRowsInPage(size_t end_row_idx, ColumnSubchunk & subchunk, ColumnChunk & column, const PrimitiveColumnInfo & column_info);
};

}
