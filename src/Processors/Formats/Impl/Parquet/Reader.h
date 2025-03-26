#pragma once

#include <Processors/Formats/Impl/Parquet/ReadCommon.h>
#include <Processors/Formats/Impl/Parquet/Prefetcher.h>

#include <queue>
#include <deque>
#include <mutex>
#include <optional>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
class Block;
struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;
}

namespace DB::Parquet
{

// TODO:
//  * profile
//     - query profiler
//     - perf or samply
//     - add OpenTelemetry::SpanHolder-s and try https://github.com/ClickHouse/ClickHouse/blob/bae91a06d914eac4cda7b90ac65dfef07eef0c82/utils/trace-visualizer/README.md
//  * check fields for false sharing, add cacheline padding as needed
//  * make sure userspace page cache read buffer supports readBigAt
//  * assert that memory usage is zero at the end, the reset()s are easy to miss
//  * make sure to not convert to full column if requested type is LowCardinality
//  * make writer write DataPageV2
//  * make writer write SizeStatistics
//  * try adding [[unlikely]] to all ifs
//  * try adding __restrict to pointers on hot paths
//  * support or deprecate the preserve-order setting
//  * stats (reuse the ones from the other PR?)
//     - number of row groups that were split into chunks
//  * add comments everywhere
//  * test getMissingValues()
//  * progress indication and estimating bytes to read; allow negative total_bytes_to_read?
//  * support newer parquet versions: https://github.com/apache/parquet-format/blob/master/CHANGES.md
//  * cache FileMetaData in something like SchemaCache
//  * TSA
//  * research reading only requested tuple elements automatically (supportsSubcolumns, supportsDynamicSubcolumns, supportsOptimizationToSubcolumns?)
//  * use dictionary page instead of bloom filter when possible
//  * remember to test these:
//     - expression appearing both in prewhere and in select list (presumably remove_prewhere_column = false)
//     - prewhere with all 3 storages (maybe have the main big test pick random storage)
//     - prewhere and where together
//     - prewhere with tuple element access (prepareReadingFromFormat looks like it won't work)
//     - prewhere with expression string equal to some (weird) column name in the file
//     - prewhere with expression that's just a (UInt8) column name, present or not present in select
//     - prewhere condition that uses no columns (e.g. rand()%2=0)
//     - no columns to read outside prewhere
//     - no columns to read, but not trivial count either
//     - ROW POLICY, with and without prewhere, with old and new reader
//     - prewhere with defaults (it probably doesn't fill them correctly, see MergeTreeRangeReader::executeActionsBeforePrewhere)
//     - prewhere on virtual columns
//     - prewhere with weird filter type (LowCardinality(UInt8), Nullable(UInt8), const UInt8)
//     - prewhere involving arrays and tuples
//     - IN with subqueries with and without prewhere
//     - compare performance to MergeTree (full scan, prewhere, skipping granules)
//     - `insert into function file('t.parquet') select number as k, toString(number)||':'||randomPrintableASCII(1000) as v from numbers(1000000) settings engine_file_truncate_on_insert=1; select count(), sum(length(v)) from file('t.parquet')` - new reader is slower than default

/// Components of this parquet reader implementation:
///  * Prefetcher is responsible for coalescing nearby short reads into bigger reads.
///    It needs to know an approximate set of all needed ranges in advance, which we can produce
///    from parquet file metadata.
///  * SharedParsingThreadPool is shared can be shared across multiple parquet file readers belonging
///    to the same query, e.g. when doing `SELECT * FROM url('.../part_{0..999}.parquet')`.
///    Splits the memory and thread count budgets among the readers. Important because we want to
///    use much more memory per reader when reading one file than when reading 100 files in parallel.
///  * SchemaConverter traverses parquet schema tree and figures out which primitive columns to read,
///    how to assemble them into compound columns (e.g. arrays or tuples), and what typecasts are
///    needed.
///  * Reader implements decoding, filtering, and assembling primitive columns into final columns.
///    Doesn't have a clean API, needs to be micromanaged by ReadManager (to minimize boilerplate and
///    distractions in Reader, which is the most complex part).
///    TODO: If it ends up split up, update this comment.
///  * ReadManager drives the Reader. Responsible for scheduling work to threads, thread safety,
///    limiting memory usage, and delivering output.

struct ValueDecoder;

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
    ///  |  2  |  2  |  false   |
    ///  |  3  |  2  |  false   |
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
        bool is_array = false;
    };

    struct PrimitiveColumnInfo
    {
        /// Column index in parquet file. NOT index in primitive_columns array.
        size_t column_idx;
        String name;
        std::unique_ptr<ValueDecoder> decoder;

        DataTypePtr raw_decoded_type; // not Nullable
        DataTypePtr intermediate_type; // maybe Nullable
        DataTypePtr final_type; // castColumn to this type
        bool output_nullable = false;
        /// TODO: Consider also adding output_low_cardinality to allow producing LowCardinality
        ///       column directly from parquet dictionary+indices. This is not straightforward
        ///       because ColumnLowCardinality requires values to be unique and the first value to
        ///       be default. So we'd need to validate uniqueness and possibly adjust indices and
        ///       dictionary to move the default value to the start.
        bool needs_cast = false; // if final_type is different from intermediate_type

        /// How to interpret repetition/definition levels.
        std::vector<LevelInfo> levels;

        /// Which stages involve this column.
        bool use_bloom_filter = false;
        bool use_column_index = false;
        bool use_prewhere = false;
        bool only_for_prewhere = false; // can remove this column after applying prewhere

        PrimitiveColumnInfo() = default;
        PrimitiveColumnInfo(PrimitiveColumnInfo &&) = default;
        ~PrimitiveColumnInfo();
    };

    struct OutputColumnInfo
    {
        String name;
        size_t primitive_start = 0;
        size_t primitive_end = 0;
        DataTypePtr type;
        std::optional<size_t> idx_in_output_block;
        std::vector<size_t> nested_columns;
        bool is_primitive = false;

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
        /// TODO: Consider bitmask for faster range operations. See also: ColumnsCommon.h
        IColumnFilter filter;

        MemoryUsageToken memory;

        void clear(MemoryUsageDiff * diff)
        {
            filter = {};
            memory.reset(diff);
        }
    };

    struct Page
    {
        const parq::PageLocation * meta;

        size_t num_rows = 0;
        bool is_dictionary = false;

        /// Unlike other prefetch requests, this one is created late and using splitAndEnqueueRange,
        /// when scheduling the column read task on the thread pool.
        Prefetcher::RequestHandle prefetch;
    };

    struct ColumnChunk
    {
        const parq::ColumnChunk * meta;

        Prefetcher::RequestHandle bloom_filter_prefetch;
        Prefetcher::RequestHandle offset_index_prefetch;
        Prefetcher::RequestHandle column_index_prefetch;
        Prefetcher::RequestHandle data_prefetch; // covers all pages

        parq::OffsetIndex offset_index;

        /// More fine-grained prefetch, if we decided to skip some pages based on filter.
        /// Empty if we're not skipping pages, and data_prefetch should be used instead.
        /// We preregister data_prefetch in Prefetch before we know page byte ranges
        /// (which come from offset_index_prefetch data), then split the range into smaller ranges
        /// if needed. If the whole data_prefetch is small and very close to other ranges (e.g. if
        /// column data is right next to offset index), Prefetcher may read it incidentally;
        /// then the `pages` prefetch ranges won't do any additional reading and will just point
        /// into the already-read bigger range.
        std::vector<Page> pages;

        /// Result of filtering this column alone. Should be ANDed across all filter columns to
        /// produce RowGroup::filter.
        RowSet partial_filter;

        /// Primitive column.
        /// If `defer_conversion`, LowCardinality of intermediate_type. Otherwise, final_type.
        MutableColumnPtr column;

        /// If this primitive column is inside an array, this is the offsets for `ColumnArray`s at
        /// all nesting levels, from outer to inner. Index is repetition level - 1.
        /// Derived from parquet's repetition/definition levels. See comment on LevelInfo.
        /// ("Arrays offsets" is intentionally grammatically incorrect to emphasize that it's a
        ///  list of lists.)
        std::vector<MutableColumnPtr> arrays_offsets;

        MemoryUsageToken column_and_offsets_memory;

        /// If parquet data is dictionary-encoded, we parse it to a LowCardinality column, then
        /// convert it to full column (unless LowCardinality data type was requested), then cast to
        /// final type if needed.
        /// Normally this conversion happens right after parsing, so ColumnChunk::column is full.
        /// But if the full column would use lots of memory, we don't want to convert it all at once.
        /// In that case, we leave `column` as LowCardinality and set zip_bombness > 1.
        /// We then deliver the row group in smaller chunks (at least `zip_bombness` of them),
        /// doing conversion to full column one chunk at a time.
        ///
        /// This is pretty important as extreme dictionary compression ratios (like 1000x) are
        /// encountered in practice.
        ///
        /// (In contrast, we currently don't split row group if its encoded uncompressed size
        /// is very big, either because it's actually big on disk or because of high compression
        /// ratio. This is probably ok because parquet writers usually have to have the whole
        /// uncompressed row group in memory. If the writer could afford to keep it in memory then
        /// it's probably not crazy big. Not a very solid assumption, maybe we'll have to rethink it.)
        size_t zip_bombness = 1;
        bool defer_conversion = false;
    };

    struct RowGroup
    {
        const parq::RowGroup * meta;

        size_t row_group_idx; // in parquet file

        /// Parallel to Reader::primitive_columns.
        /// NOT parallel to `meta.columns` (it's a subset of parquet colums).
        std::vector<ColumnChunk> columns;

        RowSet filter;

        Columns output; // parallel to extended_sample_block

        /// Fields below are used only by ReadManager.

        size_t rows_per_chunk = 0;
        size_t rows_delivered = 0;

        /// Assigned only in finishRowGroupStage. Read also when delivering chunks.
        std::atomic<ParsingStage> stage {ParsingStage::NotStarted};
        /// When this changes from nonzero to zero, the whole RowGroup experiences a synchronization
        /// point. Whoever makes such change is free to read and mutate any fields here without
        /// locking any mutexes.
        std::atomic<size_t> stage_tasks_remaining {};
    };

    struct PrewhereStep
    {
        ExpressionActions actions;
        String result_column_name;
        std::vector<size_t> input_column_idxs {}; // indices in output_columns
        bool need_filter = true;
        std::optional<size_t> column_idx_in_output_block = std::nullopt;
    };

    ReadOptions options;
    const Block * sample_block;
    std::shared_ptr<const KeyCondition> key_condition;
    Prefetcher prefetcher;

    parq::FileMetaData file_metadata;
    std::deque<RowGroup> row_groups;

    std::vector<PrimitiveColumnInfo> primitive_columns;
    size_t total_primitive_columns_in_file = 0;
    std::vector<OutputColumnInfo> output_columns;

    PrewhereInfoPtr prewhere_info;
    /// sample_block with maybe some columns added at the end.
    /// The added columns are used as inputs to prewhere expression, then discarded.
    /// (Why not just add them to sample_block? To avoid unnecessarily applying filter to them.)
    Block extended_sample_block;
    std::vector<PrewhereStep> prewhere_steps;

    void init(const ReadOptions & options_, const Block & sample_block_, std::shared_ptr<const KeyCondition> key_condition_, PrewhereInfoPtr prewhere_info_);

    static parq::FileMetaData readFileMetaData(Prefetcher & prefetcher);
    void prefilterAndInitRowGroups();
    void preparePrewhere();

    /// Returns false if the row group was filtered out and should be skipped.
    bool applyBloomFilters(RowGroup & row_group);
    RowSet applyPageIndex(ColumnChunk & column, PrimitiveColumnInfo & column_info);
    /// Assigns `pages` if only a subset of pages need to be read.
    void determinePagesToRead(ColumnChunk & column, const RowSet & rows);
    void parsePrimitiveColumn(ColumnChunk & column_chunk, const PrimitiveColumnInfo & column_info, const RowSet & filter);
    /// Returns mutable column because some of the recursive calls require it,
    /// e.g. ColumnArray::create does assumeMutable() on the nested columns.
    /// Two modes:
    ///  * If !whole_column_chunk, copies a range out of the column chunk, leaving
    ///    ColumnChunk::column-s intact. Can be called again later for the same rows.
    ///  * If whole_column_chunk, moves the whole column chunk, leaving nullptrs in
    ///    ColumnChunk::column. The caller is responsible for caching the result
    ///    (in RowGroup::output) to make sure this is not called again for the moved-out columns.
    MutableColumnPtr formOutputColumnImpl(RowGroup & row_group, size_t output_column_idx, size_t start_row, size_t num_rows, bool whole_column_chunk);
    ColumnPtr formOutputColumn(RowGroup & row_group, size_t output_column_idx, size_t start_row, size_t num_rows);
    void applyPrewhere(RowGroup & row_group);
    /// How much memory ColumnChunk::column and arrays_offsets will use.
    size_t estimateColumnMemoryUsage(const ColumnChunk & column) const;

    size_t decideNumRowsPerChunk(RowGroup & row_group);
};

}
