#pragma once
#include <Core/Block.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

template <typename T>
class ColumnVector;
using ColumnUInt8 = ColumnVector<UInt8>;

class IMergeTreeReader;
class MergeTreeIndexGranularity;
struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

struct PrewhereExprStep
{
    enum Type
    {
        Filter,
        Expression,
    };

    Type type = Type::Filter;
    ExpressionActionsPtr actions;
    String filter_column_name;

    bool remove_filter_column = false;
    bool need_filter = false;

    /// Some PREWHERE steps should be executed without conversions (e.g. early mutation steps)
    /// A step without alter conversion cannot be executed after step with alter conversions.
    bool perform_alter_conversions = false;
};

using PrewhereExprStepPtr = std::shared_ptr<PrewhereExprStep>;
using PrewhereExprSteps = std::vector<PrewhereExprStepPtr>;

/// The same as PrewhereInfo, but with ExpressionActions instead of ActionsDAG
struct PrewhereExprInfo
{
    PrewhereExprSteps steps;

    std::string dump() const;
    std::string dumpConditions() const;
};

struct ReadStepPerformanceCounters
{
    std::atomic<UInt64> rows_read = 0;
};

using ReadStepPerformanceCountersPtr = std::shared_ptr<ReadStepPerformanceCounters>;

class ReadStepsPerformanceCounters final
{
public:
    ReadStepPerformanceCountersPtr getCountersForStep(size_t step)
    {
        if (step >= performance_counters.size())
            performance_counters.resize(step + 1);
        if (!performance_counters[step])
            performance_counters[step] = std::make_shared<ReadStepPerformanceCounters>();
        return performance_counters[step];
    }

    const std::vector<ReadStepPerformanceCountersPtr> & getCounters() const { return performance_counters; }

private:
    std::vector<ReadStepPerformanceCountersPtr> performance_counters;
};

class FilterWithCachedCount
{
    ConstantFilterDescription const_description;  /// TODO: ConstantFilterDescription only checks always true/false for const columns
                                                  /// think how to handle when the column in not const but has all 0s or all 1s
    ColumnPtr column = nullptr;
    const IColumn::Filter * data = nullptr;
    mutable size_t cached_count_bytes = -1;

public:
    explicit FilterWithCachedCount() = default;

    explicit FilterWithCachedCount(const ColumnPtr & column_)
        : const_description(*column_)
    {
        ColumnPtr col = column_->convertToFullIfNeeded();
        FilterDescription desc(*col);
        column = desc.data_holder ? desc.data_holder : col;
        data = desc.data;
    }

    bool present() const { return !!column; }

    bool alwaysTrue() const { return const_description.always_true; }
    bool alwaysFalse() const { return const_description.always_false; }

    ColumnPtr getColumn() const { return column; }

    const IColumn::Filter & getData() const { return *data; }

    size_t size() const { return column->size(); }

    size_t countBytesInFilter() const
    {
        if (cached_count_bytes == size_t(-1))
            cached_count_bytes = DB::countBytesInFilter(*data);
        return cached_count_bytes;
    }
};

/// MergeTreeReader iterator which allows sequential reading for arbitrary number of rows between pairs of marks in the same part.
/// Stores reading state, which can be inside granule. Can skip rows in current granule and start reading from next mark.
/// Used generally for reading number of rows less than index granularity to decrease cache misses for fat blocks.
class MergeTreeRangeReader
{
public:
    MergeTreeRangeReader(
        IMergeTreeReader * merge_tree_reader_,
        Block prev_reader_header_,
        const PrewhereExprStep * prewhere_info_,
        ReadStepPerformanceCountersPtr performance_counters_,
        bool main_reader_);

    MergeTreeRangeReader() = default;

    size_t numReadRowsInCurrentGranule() const;
    size_t numPendingRowsInCurrentGranule() const;
    size_t numRowsInCurrentGranule() const;
    size_t currentMark() const;

    bool isCurrentRangeFinished() const;

    /// Names of virtual columns that are filled in RangeReader.
    static const NameSet virtuals_to_fill;

private:
    /// Accumulates sequential read() requests to perform a large read instead of multiple small reads
    class DelayedStream
    {
    public:
        DelayedStream() = default;
        DelayedStream(size_t from_mark, size_t current_task_last_mark_, IMergeTreeReader * merge_tree_reader);

        /// Read @num_rows rows from @from_mark starting from @offset row
        /// Returns the number of rows added to block.
        /// NOTE: have to return number of rows because block has broken invariant:
        ///       some columns may have different size (for example, default columns may be zero size).
        size_t read(Columns & columns, size_t from_mark, size_t offset, size_t num_rows);

        /// Skip extra rows to current_offset and perform actual reading
        size_t finalize(Columns & columns);

        bool isFinished() const { return is_finished; }

    private:
        size_t current_mark = 0;
        /// Offset from current mark in rows
        size_t current_offset = 0;
        /// Num of rows we have to read
        size_t num_delayed_rows = 0;
        /// Last mark from all ranges of current task.
        size_t current_task_last_mark = 0;

        /// Actual reader of data from disk
        IMergeTreeReader * merge_tree_reader = nullptr;
        const MergeTreeIndexGranularity * index_granularity = nullptr;
        bool continue_reading = false;
        bool is_finished = true;

        /// Current position from the beginning of file in rows
        size_t position() const;
        size_t readRows(Columns & columns, size_t num_rows);
    };

    /// Very thin wrapper for DelayedStream
    /// Check bounds of read ranges and make steps between marks
    class Stream
    {
    public:
        Stream() = default;
        Stream(size_t from_mark, size_t to_mark, size_t current_task_last_mark, IMergeTreeReader * merge_tree_reader);

        /// Returns the number of rows added to block.
        size_t read(Columns & columns, size_t num_rows, bool skip_remaining_rows_in_current_granule);
        size_t finalize(Columns & columns);
        void skip(size_t num_rows);

        void finish() { current_mark = last_mark; }
        bool isFinished() const { return current_mark >= last_mark; }

        size_t numReadRowsInCurrentGranule() const { return offset_after_current_mark; }
        size_t numPendingRowsInCurrentGranule() const
        {
            return current_mark_index_granularity - numReadRowsInCurrentGranule();
        }
        size_t numPendingGranules() const { return last_mark - current_mark; }
        size_t numPendingRows() const;
        size_t currentMark() const { return current_mark; }
        UInt64 currentPartOffset() const;
        UInt64 lastPartOffset() const;

        IMergeTreeReader * merge_tree_reader = nullptr;
        const MergeTreeIndexGranularity * index_granularity = nullptr;
        DelayedStream stream;
        size_t current_mark = 0;
        size_t current_mark_index_granularity = 0;

        /// Invariant: offset_after_current_mark <= current_mark_index_granularity
        size_t offset_after_current_mark = 0;

        /// Last mark in current range.
        size_t last_mark = 0;

        void checkNotFinished() const;
        void checkEnoughSpaceInCurrentGranule(size_t num_rows) const;
        size_t readRows(Columns & columns, size_t num_rows);
        void toNextMark();
        size_t ceilRowsToCompleteGranules(size_t rows_num) const;
    };

public:
    /// Statistics after next reading step.
    class ReadResult
    {
    public:
        Columns columns;
        size_t num_rows = 0;

        /// All read marks.
        MarkRanges read_mark_ranges;

        /// The number of rows were added to block as a result of reading chain.
        size_t numReadRows() const { return num_read_rows; }
        /// The number of bytes read from disk.
        size_t numBytesRead() const { return num_bytes_read; }

    private:
        friend class MergeTreeRangeReader;
        friend class MergeTreeReadersChain;

        using NumRows = std::vector<size_t>;

        struct GranuleOffset
        {
            UInt64 starting_offset;  /// Granule starting offset within the part
            UInt64 granule_offset;   /// Granule index within the part
        };
        using GranuleOffsets = std::vector<GranuleOffset>;

        struct RangeInfo
        {
            size_t num_granules_read_before_start;
            MarkRange range;
        };

        using RangesInfo = std::vector<RangeInfo>;

        explicit ReadResult(LoggerPtr log_) : log(log_) {}

        static size_t getLastMark(const MergeTreeRangeReader::ReadResult::RangesInfo & ranges);

        /// Populate @rows_per_granule and @granule_offsets. See comments below.
        void addGranule(size_t num_rows_, GranuleOffset granule_offset);
        void adjustLastGranule();

        void addRows(size_t rows) { num_read_rows += rows; }

        /// Populate @started_ranges. See comments below.
        void addRange(const MarkRange & range) { started_ranges.push_back({rows_per_granule.size(), range}); }

        /// For query condition cache.
        void addReadRange(MarkRange mark_range) { read_mark_ranges.push_back(std::move(mark_range)); }

        /// Add current step filter to the result and then for each granule calculate the number of filtered rows at the end.
        /// Remove them and update filter.
        /// Apply the filter to the columns and update num_rows if required
        void optimize(const FilterWithCachedCount & current_filter, bool can_read_incomplete_granules);

        /// Remove all rows from granules.
        void clear();

        void setFilterConstTrue();

        void addNumBytesRead(size_t count) { num_bytes_read += count; }

        /// Shrinks columns according to the diff between current and previous rows_per_granule.
        void shrink(Columns & old_columns, const NumRows & rows_per_granule_previous) const;

        /// Applies the filter to the columns and updates num_rows.
        void applyFilter(const FilterWithCachedCount & filter);

        /// Verifies that columns and filter sizes match.
        /// The checks might be non-trivial so it make sense to have the only in debug builds.
        void checkInternalConsistency() const;

        std::string dumpInfo() const;

        /// Contains columns that are not included into result but might be needed for default values calculation.
        Block additional_columns;

        /// Track newly initiated granule ranges during startReadingChain. Does not contain the range started in previous read.
        /// Used to compute _part_offset and align continueReadingChain streams accordingly.
        RangesInfo started_ranges;

        /// Number of rows intended to be read per granule during the reading chain.
        ///
        /// Filled in `startReadingChain` based on initial granule layout and expected row counts.
        /// May be further filtered in `optimize` when `PREWHERE` filters prune rows early.
        /// Used by `continueReadingChain` to guide how many rows to fetch from each granule.
        ///
        /// Example:
        ///                 startReadingChain         optimize   continueReadingChain   optimize       ...
        ///
        /// Granule i       8192                      4000       4000                   4000
        /// Granule i+1     8192                      3000       3000                   0 (filtered)
        /// Granule i+2     8192                      3000       3000                   1000
        /// Granule i+3     1000 (last incomplete)    1000       1000                   1000
        NumRows rows_per_granule;
        /// Stores starting row offset and granule index for each granule.
        /// Used to generate _part_offset and _part_granule_offset columns.
        GranuleOffsets granule_offsets;
        /// Sum(rows_per_granule)
        size_t total_rows_per_granule = 0;
        /// The number of rows was read at first step. May be zero if no read columns present in part.
        size_t num_read_rows = 0;
        /// The number of rows was removed from last granule after clear or optimize.
        size_t num_rows_to_skip_in_last_granule = 0;
        /// Without any filtration.
        size_t num_bytes_read = 0;

        /// This filter has the size of total_rows_per_granule. This means that it can be applied to newly read columns.
        /// The result of applying this filter is that only rows that pass all previous filtering steps will remain.
        FilterWithCachedCount final_filter;

        /// This flag is true when prewhere column can be returned without filtering.
        /// It's true when it contains 0s from all filtering steps (not just the step when it was calculated).
        /// NOTE: If we accumulated the final_filter for several steps without applying it then prewhere column calculated at the last step
        /// will not contain 0s from all previous steps.
        bool can_return_prewhere_column_without_filtering = true;

        /// Checks if result columns have current final_filter applied.
        bool filterWasApplied() const { return !final_filter.present() || final_filter.countBytesInFilter() == num_rows; }

        /// Builds updated filter by cutting zeros in granules tails
        void collapseZeroTails(const IColumn::Filter & filter, const NumRows & rows_per_granule_previous, IColumn::Filter & new_filter) const;
        size_t countZeroTails(const IColumn::Filter & filter, NumRows & zero_tails, bool can_read_incomplete_granules) const;
        static size_t numZerosInTail(const UInt8 * begin, const UInt8 * end);

        LoggerPtr log;
    };

    ReadResult startReadingChain(size_t max_rows, MarkRanges & ranges);
    Columns continueReadingChain(ReadResult & result, size_t & num_rows);

    const Block & getSampleBlock() const { return result_sample_block; }
    const Block & getReadSampleBlock() const { return read_sample_block; }

    /// Executes actions required before PREWHERE, such as alter conversions and filling defaults.
    void executeActionsBeforePrewhere(ReadResult & result, Columns & read_columns, const Block & previous_header, size_t num_read_rows) const;
    void executePrewhereActionsAndFilterColumns(ReadResult & result, const Block & previous_header, bool is_last_reader) const;

    IMergeTreeReader * getReader() const { return merge_tree_reader; }

private:
    void fillVirtualColumns(Columns & columns, ReadResult & result);
    ColumnPtr createPartOffsetColumn(ReadResult & result);
    ColumnPtr createPartGranuleOffsetColumn(ReadResult & result);

    void updatePerformanceCounters(size_t num_rows_read);

    IMergeTreeReader * merge_tree_reader = nullptr;
    const MergeTreeIndexGranularity * index_granularity = nullptr;
    const PrewhereExprStep * prewhere_info;

    Stream stream;

    Block read_sample_block;    /// Block with columns that are actually read from disk + non-const virtual columns that are filled at this step.
    Block result_sample_block;  /// Block with columns that are returned by this step.

    ReadStepPerformanceCountersPtr performance_counters;
    bool main_reader = false; /// Whether it is the main reader or one of the readers for prewhere steps

    LoggerPtr log = getLogger("MergeTreeRangeReader");
};

}
