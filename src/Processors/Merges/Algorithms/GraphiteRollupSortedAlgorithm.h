#pragma once
#include <Processors/Merges/Algorithms/IMergingAlgorithmWithSharedChunks.h>
#include <Processors/Merges/Algorithms/Graphite.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Common/AlignedBuffer.h>

namespace DB
{

/** Merges several sorted inputs into one.
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
class GraphiteRollupSortedAlgorithm final : public IMergingAlgorithmWithSharedChunks
{
public:
    GraphiteRollupSortedAlgorithm(
        const Block & header, size_t num_inputs,
        SortDescription description_, size_t max_block_size,
        Graphite::Params params_, time_t time_of_merge_);

    Status merge() override;

    struct ColumnsDefinition
    {
        size_t path_column_num;
        size_t time_column_num;
        size_t value_column_num;
        size_t version_column_num;

        DataTypePtr time_column_type;

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

private:
    GraphiteRollupMergedData merged_data;

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
    std::string_view current_group_path;

    static constexpr size_t max_row_refs = 2; /// current_subgroup_newest_row, current_row.
    /// Last row with maximum version for current primary key (time bucket).
    RowRef current_subgroup_newest_row;

    /// Time of last read row
    time_t current_time = 0;
    time_t current_time_rounded = 0;

    UInt32 selectPrecision(const Graphite::Retentions & retentions, time_t time) const;

    /// Insert the values into the resulting columns, which will not be changed in the future.
    void startNextGroup(SortCursor & cursor, Graphite::RollupRule next_rule);

    /// Insert the calculated `time`, `value`, `version` values into the resulting columns by the last group of rows.
    void finishCurrentGroup();

    /// Update the state of the aggregate function with the new `value`.
    void accumulateRow(RowRef & row);
};

}
