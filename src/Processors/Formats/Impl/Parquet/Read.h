#pragma once

#include <Processors/Formats/Impl/Parquet/ReadCommon.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>
#include <Processors/Formats/Impl/Parquet/Prefetcher.h>

#include <queue>
#include <deque>
#include <mutex>
#include <optional>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{
class Block;
}

namespace DB::Parquet
{

// TODO:
//  * check if the LowCardinality thing runs fast; if not, do partial conversion to full column
//    during read, or even reimplement everything in a fully streaming way
//  * check fields for false sharing, add cacheline padding as needed
//  * make sure userspace page cache read buffer supports readBigAt
//  * assert that memory usage is zero at the end, the reset()s are easy to miss
//  * make sure to not convert to full column if requested type is LowCardinality
//  * make writer write DataPageV2
//  * make writer write SizeStatistics
//  * try adding [[unlikely]] to all ifs
//  * support or deprecate the preserve-order setting
//  * stats (reuse the ones from the other PR?)
//  * add comments everywhere
//  * test getMissingValues()
//  * progress indication and estimating bytes to read; allow negative total_bytes_to_read?

/// Components of this parquet reader implementation:
///  * Prefetcher is responsible for coalescing nearby short reads into bigger reads.
///    It needs to know an approximate set of all needed ranges in advance, which we can produce
///    from parquet file metadata.
///  * SharedParsingThreadPool is shared can be shared across multiple parquet file readers belonging
///    to the same query, e.g. when doing `SELECT * FROM url('.../part_{0..999}.parquet')`.
///    Splits the memory and thread count budgets among the readers. Important because we want to
///    use much more memory per reader when reading one file than when reading 100 files in parallel.
///  * Reader implements parsing, type conversions, and filtering.
///    Doesn't have a clean API, needs to be micromanaged by ReadManager. This is to minimize
///    boilerplate and distractions in Reader.
///    TODO: If it ends up split up, update this comment.
///  * ReadManager drives the Reader. Responsible for scheduling work to threads, thread safety,
///    limiting memory usage, and delivering output.

struct ValueDecoder;

struct Reader
{
    struct PrimitiveColumnInfo
    {
        /// Column index in parquet file. NOT index in primitive_columns array.
        size_t column_idx;
        std::unique_ptr<ValueDecoder> decoder;
        DataTypePtr decoded_type;
        bool output_nullable = false;
        bool output_low_cardinality = false;
        //TODO: assign these and other rep/def level info
        UInt8 max_rep = 0;
        UInt8 max_def = 0;

        /// Which stages involve this column.
        bool use_bloom_filter = false;
        bool use_column_index = false;
        bool use_prewhere = false;
    };

    struct OutputColumnInfo
    {
        size_t primitive_start = 0;
        size_t primitive_end = 0;
        DataTypePtr decoded_type;
        DataTypePtr final_type;
        std::optional<size_t> idx_in_output_block;
        std::vector<size_t> nested_columns;
    };

    struct RowSet
    {
        size_t rows_total = 0;
        size_t rows_pass = 0;
        /// Can be empty if rows_pass is equal to 0 or rows_total.
        IColumnFilter mask; // TODO: consider bitmask for faster range set/check

        MemoryUsageToken memory;

        void clear(MemoryUsageDiff * diff)
        {
            mask = {};
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

        /// Primitive column. ColumnNullable if PrimitiveColumnInfo says is_nullable.
        ColumnPtr column;

        /// If this primitive column is inside an array, this is the offsets for ColumnArray.
        /// If multiple nested arrays (Array(Array(...))), this lists them from inner to outer.
        /// Derived from parquet's repetition/definition levels.
        ///
        /// (In general, parquet file can also have multiple levels of nullables,
        /// e.g. `Nullable(Array(Nullable(Nullable(Array(...)))))`. But clickhouse only supports
        /// Nullable on primitive types. So we get rid of non-innermost nullables when processing
        /// repetition/definition levels. What's left is a maybe-nullable primitive `column`,
        /// maybe nested inside some arrays.)
        std::vector<ColumnPtr> array_offsets;

        MemoryUsageToken column_and_offsets_memory;

        /// If parquet data is dictionary-encoded, we parse it to a LowCardinality column, then
        /// convert it to full column (unless LowCardinality data type was requested).
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
    };

    struct RowGroup
    {
        const parq::RowGroup * meta;

        size_t row_group_idx; // in parquet file

        /// Parallel to Reader::primitive_columns.
        /// NOT parallel to `meta.columns` (it's a subset of parquet colums).
        std::vector<ColumnChunk> columns;

        RowSet filter;


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

    ReadOptions options;
    const Block * sample_block;
    std::shared_ptr<const KeyCondition> key_condition;
    Prefetcher prefetcher;

    parq::FileMetaData file_metadata;
    std::deque<RowGroup> row_groups;

    std::vector<PrimitiveColumnInfo> primitive_columns;
    size_t total_primitive_column_count = 0;
    std::vector<OutputColumnInfo> output_columns;

    void init(const ReadOptions & options_, const Block & sample_block_, std::shared_ptr<const KeyCondition> key_condition_);

    void readFileMetaData();
    void prefilterAndInitRowGroups();

    /// Returns false if the row group was filtered out and should be skipped.
    bool applyBloomFilters(RowGroup & row_group);
    RowSet applyPageIndex(ColumnChunk & column, PrimitiveColumnInfo & column_info);
    /// Assigns `pages` if only a subset of pages need to be read.
    void determinePagesToRead(ColumnChunk & column, const RowSet & rows);
    void parsePrimitiveColumn(ColumnChunk & column_chunk, const PrimitiveColumnInfo & column_info, const RowSet & filter);
    ColumnPtr formOutputColumn(RowGroup & row_group, size_t output_column_idx, size_t start_row, size_t num_rows);
    RowSet applyPrewhere(Block block);
    /// How much memory ColumnChunk::column and array_offsets will use.
    size_t estimateColumnMemoryUsage(const ColumnChunk & column) const;

    size_t decideNumRowsPerChunk(RowGroup & row_group);
};


/// Converting parquet schema to clickhouse schema and decoding information.
/// Used both for schema inference and for reading.
struct SchemaConverter
{
    using PrimitiveColumnInfo = Reader::PrimitiveColumnInfo;
    using OutputColumnInfo = Reader::OutputColumnInfo;

    const parq::FileMetaData & file_metadata;
    const ReadOptions & options;
    const Block * sample_block;
    std::vector<PrimitiveColumnInfo> primitive_columns;
    std::vector<OutputColumnInfo> output_columns;

    size_t schema_idx = 1;
    size_t primitive_column_idx = 0;

    SchemaConverter(const parq::FileMetaData &, const ReadOptions &, const Block *);

    void checkHasColumns();

    std::optional<size_t> processSchemaElement(String name, bool requested, DataTypePtr type_hint);
    void processPrimitiveColumn(
        const parq::SchemaElement & element, const IDataType * type_hint,
        std::unique_ptr<ValueDecoder> & out_decoder, DataTypePtr & out_decoded_type,
        DataTypePtr & out_inferred_type);
};


// I'd like to speak to the manager.
class ReadManager
{
public:
    Reader reader;

    /// To initialize ReadManager:
    ///  1. call manager.reader.prefetcher.init
    ///  2. call manager.reader.init
    ///  3. call manager.init
    /// (I'm trying this style because the usual pattern of passing-through lots of arguments through
    /// layers of constructors seems bad. This seems better but still not great, hopefully there's an
    /// even better way.)
    void init(SharedParsingThreadPoolPtr thread_pool_);

    ~ReadManager();

    /// Not thread safe.
    Chunk read();

private:
    using RowGroup = Reader::RowGroup;
    using ColumnChunk = Reader::ColumnChunk;
    using PrimitiveColumnInfo = Reader::PrimitiveColumnInfo;

    struct Task
    {
        ParsingStage stage;
        size_t row_group_idx;
        /// Some stages have a Task per column, others have a Task per row group.
        /// The stages with Task per column may also do some per-row-group work after all per-column
        /// tasks complete, in finishRowGroupStage.
        size_t column_idx = UINT64_MAX;
    };

    struct Stage
    {
        std::atomic<size_t> memory_usage {0};
        double memory_target_fraction = 1;

        std::mutex mutex;
        /// Tasks not scheduled on thread pool yet.
        /// For Delivering stage, these tasks are picked up by read() instead of going to thread pool.
        std::queue<Task> tasks_to_schedule;
    };

    SharedParsingThreadPoolPtr thread_pool;

    std::shared_ptr<ShutdownHelper> shutdown = std::make_shared<ShutdownHelper>();

    std::array<Stage, size_t(ParsingStage::COUNT)> stages;

    std::mutex delivery_mutex;
    std::condition_variable delivery_cv;
    std::deque<size_t> deliverable_row_groups;
    size_t first_incomplete_row_group = 0;
    std::exception_ptr exception;

    void scheduleTask(Task task, MemoryUsageDiff * diff);
    void runTask(Task task);
    void scheduleTasksIfNeeded(ParsingStage stage_idx, std::unique_lock<std::mutex> &);
    void finishRowGroupStage(size_t row_group_idx, MemoryUsageDiff && diff);
    void flushMemoryUsageDiff(MemoryUsageDiff && diff);
};

//TODO:
// * BIT_PACKED for rep/def levels
// * PLAIN BOOLEAN
// * RLE BOOLEAN
// * DELTA_BINARY_PACKED (INT32, INT64)
// * DELTA_LENGTH_BYTE_ARRAY
// * DELTA_BYTE_ARRAY
// * BYTE_STREAM_SPLIT
struct ValueDecoder
{
    /// If canReadDirectlyIntoColumn returns true, decodePage is not called.
    virtual bool canReadDirectlyIntoColumn(parq::Encoding::type, size_t /*num_values*/, IColumn &, char ** /*out_ptr*/, size_t * /*out_bytes*/) const { return false; }
    /// Caller ensures that `data` and `filter` are padded, i.e. have at least PADDING_FOR_SIMD bytes
    /// of readable memory before start and after end.
    /// `num_values_out` is the number of ones in filter[0..num_values_in]. If filter is nullptr,
    /// num_values_out == num_values_in.
    virtual void decodePage(parq::Encoding::type, const char * /*data*/, size_t /*bytes*/, size_t /*num_values_in*/, size_t /*num_values_out*/, IColumn &, const UInt8 * /*filter*/) const { chassert(false); }

    virtual ~ValueDecoder() = default;
};

}
