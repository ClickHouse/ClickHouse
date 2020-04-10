#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/RowRef.h>
#include <Processors/Merges/MergedData.h>
#include <Processors/Merges/Graphite.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/AlignedBuffer.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

#include <common/logger_useful.h>

namespace DB
{
/** Merges several sorted ports into one.
  *
  * For each group of consecutive identical values of the `path` column,
  *  and the same `time` values, rounded to some precision
  *  (where rounding accuracy depends on the template set for `path`
  *   and the amount of time elapsed from `time` to the specified time),
  * keeps one line,
  *  performing the rounding of time,
  *  merge `value` values using the specified aggregate functions,
  *  as well as keeping the maximum value of the `version` column.
  */
class GraphiteRollupSortedTransform : public IMergingTransform
{
public:
    GraphiteRollupSortedTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_, size_t max_block_size,
        Graphite::Params params_, time_t time_of_merge_);

    String getName() const override { return "GraphiteRollupSortedTransform"; }
    void work() override;

    struct ColumnsDefinition
    {
        size_t path_column_num;
        size_t time_column_num;
        size_t value_column_num;
        size_t version_column_num;

        /// All columns other than 'time', 'value', 'version'. They are unmodified during rollup.
        ColumnNumbers unmodified_column_numbers;
    };

    using RowRef = detail::RowRefWithOwnedChunk;

    /// Specialization for SummingSortedTransform.
    class GraphiteRollupMergedData : public MergedData
    {
    public:
        using MergedData::MergedData;
        ~GraphiteRollupMergedData();

        void startNextGroup(const ColumnRawPtrs & raw_columns, size_t row,
                            Graphite::RollupRule next_rule, ColumnsDefinition & def);
        void insertRow(time_t time, RowRef & row, ColumnsDefinition & def);
        void accumulateRow(RowRef & row, ColumnsDefinition & def);
        bool wasGroupStarted() const { return was_group_started; }

        const Graphite::RollupRule & currentRule() const { return current_rule; }
        void allocMemForAggregates(size_t size, size_t alignment) { place_for_aggregate_state.reset(size, alignment); }

    private:
        Graphite::RollupRule current_rule = {nullptr, nullptr};
        AlignedBuffer place_for_aggregate_state;
        bool aggregate_state_created = false; /// Invariant: if true then current_rule is not NULL.
        bool was_group_started = false;
    };

protected:
    void initializeInputs() override;
    void consume(Chunk chunk, size_t input_number) override;

private:
    Logger * log = &Logger::get("GraphiteRollupSortedBlockInputStream");

    GraphiteRollupMergedData merged_data;
    SortDescription description;

    /// Allocator must be destroyed after all RowRefs.
    detail::SharedChunkAllocator chunk_allocator;

    /// Chunks currently being merged.
    using SourceChunks = std::vector<detail::SharedChunkPtr>;
    SourceChunks source_chunks;
    SortCursorImpls cursors;

    SortingHeap<SortCursor> queue;
    bool is_queue_initialized = false;

    const Graphite::Params params;
    ColumnsDefinition columns_definition;

    time_t time_of_merge;

    /// No data has been read.
    bool is_first = true;

    /* | path | time | rounded_time | version | value | unmodified |
     * -----------------------------------------------------------------------------------
     * | A    | 11   | 10           | 1       | 1     | a          |                     |
     * | A    | 11   | 10           | 3       | 2     | b          |> subgroup(A, 11)    |
     * | A    | 11   | 10           | 2       | 3     | c          |                     |> group(A, 10)
     * ----------------------------------------------------------------------------------|>
     * | A    | 12   | 10           | 0       | 4     | d          |                     |> Outputs (A, 10, avg(2, 5), a)
     * | A    | 12   | 10           | 1       | 5     | e          |> subgroup(A, 12)    |
     * -----------------------------------------------------------------------------------
     * | A    | 21   | 20           | 1       | 6     | f          |
     * | B    | 11   | 10           | 1       | 7     | g          |
     * ...
     */

    /// Path name of current bucket
    StringRef current_group_path;

    static constexpr size_t max_row_refs = 2; /// current_subgroup_newest_row, current_row.
    /// Last row with maximum version for current primary key (time bucket).
    RowRef current_subgroup_newest_row;

    /// Time of last read row
    time_t current_time = 0;
    time_t current_time_rounded = 0;

    const Graphite::Pattern undef_pattern =
    { /// temporary empty pattern for selectPatternForPath
        .regexp = nullptr,
        .regexp_str = "",
        .function = nullptr,
        .retentions = DB::Graphite::Retentions(),
        .type = undef_pattern.TypeUndef,
    };

    Graphite::RollupRule selectPatternForPath(StringRef path) const;
    UInt32 selectPrecision(const Graphite::Retentions & retentions, time_t time) const;

    /// Insert the values into the resulting columns, which will not be changed in the future.
    void startNextGroup(SortCursor & cursor, Graphite::RollupRule next_rule);

    /// Insert the calculated `time`, `value`, `version` values into the resulting columns by the last group of rows.
    void finishCurrentGroup();

    /// Update the state of the aggregate function with the new `value`.
    void accumulateRow(RowRef & row);

    void merge();
    void updateCursor(Chunk chunk, size_t source_num);
};

}
