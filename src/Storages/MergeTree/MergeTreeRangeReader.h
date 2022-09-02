#pragma once
#include <Core/Block.h>
#include <Common/logger_useful.h>
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
    ExpressionActionsPtr actions;
    String column_name;
    bool remove_column = false;
    bool need_filter = false;
};

/// The same as PrewhereInfo, but with ExpressionActions instead of ActionsDAG
struct PrewhereExprInfo
{
    std::vector<PrewhereExprStep> steps;

    std::string dump() const;
};

/// MergeTreeReader iterator which allows sequential reading for arbitrary number of rows between pairs of marks in the same part.
/// Stores reading state, which can be inside granule. Can skip rows in current granule and start reading from next mark.
/// Used generally for reading number of rows less than index granularity to decrease cache misses for fat blocks.
class MergeTreeRangeReader
{
public:
    MergeTreeRangeReader(
        IMergeTreeReader * merge_tree_reader_,
        MergeTreeRangeReader * prev_reader_,
        const PrewhereExprStep * prewhere_info_,
        bool last_reader_in_chain_,
        const Names & non_const_virtual_column_names);

    MergeTreeRangeReader() = default;

    bool isReadingFinished() const;

    size_t numReadRowsInCurrentGranule() const;
    size_t numPendingRowsInCurrentGranule() const;
    size_t numRowsInCurrentGranule() const;
    size_t currentMark() const;

    bool isCurrentRangeFinished() const;
    bool isInitialized() const { return is_initialized; }

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

        /// Current position from the begging of file in rows
        size_t position() const;
        size_t readRows(Columns & columns, size_t num_rows);
    };

    /// Very thin wrapper for DelayedStream
    /// Check bounds of read ranges and make steps between marks
    class Stream
    {
    public:
        Stream() = default;
        Stream(size_t from_mark, size_t to_mark,
               size_t current_task_last_mark, IMergeTreeReader * merge_tree_reader);

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

        size_t current_mark = 0;
        /// Invariant: offset_after_current_mark + skipped_rows_after_offset < index_granularity
        size_t offset_after_current_mark = 0;

        /// Last mark in current range.
        size_t last_mark = 0;

        IMergeTreeReader * merge_tree_reader = nullptr;
        const MergeTreeIndexGranularity * index_granularity = nullptr;

        size_t current_mark_index_granularity = 0;

        DelayedStream stream;

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

        /// The number of rows were added to block as a result of reading chain.
        size_t numReadRows() const { return num_read_rows; }
        /// The number of bytes read from disk.
        size_t numBytesRead() const { return num_bytes_read; }

    private:
        /// Only MergeTreeRangeReader is supposed to access ReadResult internals.
        friend class MergeTreeRangeReader;

        using NumRows = std::vector<size_t>;

        struct RangeInfo
        {
            size_t num_granules_read_before_start;
            MarkRange range;
        };

        using RangesInfo = std::vector<RangeInfo>;

        const RangesInfo & startedRanges() const { return started_ranges; }
        const NumRows & rowsPerGranule() const { return rows_per_granule; }

        static size_t getLastMark(const MergeTreeRangeReader::ReadResult::RangesInfo & ranges);

        /// The number of rows were read at LAST iteration in chain. <= num_added_rows + num_filtered_rows.
        size_t totalRowsPerGranule() const { return total_rows_per_granule; }
        size_t numRowsToSkipInLastGranule() const { return num_rows_to_skip_in_last_granule; }
        /// Filter you need to apply to newly-read columns in order to add them to block.
        const ColumnUInt8 * getFilterOriginal() const { return filter_original ? filter_original : filter; }
        const ColumnUInt8 * getFilter() const { return filter; }
        ColumnPtr & getFilterHolder() { return filter_holder; }

        void addGranule(size_t num_rows_);
        void adjustLastGranule();
        void addRows(size_t rows) { num_read_rows += rows; }
        void addRange(const MarkRange & range) { started_ranges.push_back({rows_per_granule.size(), range}); }

        /// Set filter or replace old one. Filter must have more zeroes than previous.
        void setFilter(const ColumnPtr & new_filter);
        /// For each granule calculate the number of filtered rows at the end. Remove them and update filter.
        void optimize(bool can_read_incomplete_granules, bool allow_filter_columns);
        /// Remove all rows from granules.
        void clear();

        void clearFilter() { filter = nullptr; }
        void setFilterConstTrue();
        void setFilterConstFalse();

        void addNumBytesRead(size_t count) { num_bytes_read += count; }

        void shrink(Columns & old_columns);

        size_t countBytesInResultFilter(const IColumn::Filter & filter);

        /// If this flag is false than filtering form PREWHERE can be delayed and done in WHERE
        /// to reduce memory copies and applying heavy filters multiple times
        bool need_filter = false;

        Block block_before_prewhere;

        RangesInfo started_ranges;
        /// The number of rows read from each granule.
        /// Granule here is not number of rows between two marks
        /// It's amount of rows per single reading act
        NumRows rows_per_granule;
        NumRows rows_per_granule_original;
        /// Sum(rows_per_granule)
        size_t total_rows_per_granule = 0;
        /// The number of rows was read at first step. May be zero if no read columns present in part.
        size_t num_read_rows = 0;
        /// The number of rows was removed from last granule after clear or optimize.
        size_t num_rows_to_skip_in_last_granule = 0;
        /// Without any filtration.
        size_t num_bytes_read = 0;
        /// nullptr if prev reader hasn't prewhere_actions. Otherwise filter.size() >= total_rows_per_granule.
        ColumnPtr filter_holder;
        ColumnPtr filter_holder_original;
        const ColumnUInt8 * filter = nullptr;
        const ColumnUInt8 * filter_original = nullptr;

        void collapseZeroTails(const IColumn::Filter & filter, IColumn::Filter & new_filter);
        size_t countZeroTails(const IColumn::Filter & filter, NumRows & zero_tails, bool can_read_incomplete_granules) const;
        static size_t numZerosInTail(const UInt8 * begin, const UInt8 * end);

        std::map<const IColumn::Filter *, size_t> filter_bytes_map;
    };

    ReadResult read(size_t max_rows, MarkRanges & ranges);

    const Block & getSampleBlock() const { return sample_block; }

private:
    ReadResult startReadingChain(size_t max_rows, MarkRanges & ranges);
    Columns continueReadingChain(const ReadResult & result, size_t & num_rows);
    void executePrewhereActionsAndFilterColumns(ReadResult & result);
    void fillPartOffsetColumn(ReadResult & result, UInt64 leading_begin_part_offset, UInt64 leading_end_part_offset);

    IMergeTreeReader * merge_tree_reader = nullptr;
    const MergeTreeIndexGranularity * index_granularity = nullptr;
    MergeTreeRangeReader * prev_reader = nullptr; /// If not nullptr, read from prev_reader firstly.
    const PrewhereExprStep * prewhere_info;

    Stream stream;

    Block sample_block;

    bool last_reader_in_chain = false;
    bool is_initialized = false;
    Names non_const_virtual_column_names;
};

}
