#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/RowRef.h>
#include <Processors/Merges/MergedData.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/AlignedBuffer.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

#include <common/logger_useful.h>

namespace DB
{
/** Intended for implementation of "rollup" - aggregation (rounding) of older data
  *  for a table with Graphite data (Graphite is the system for time series monitoring).
  *
  * Table with graphite data has at least the following columns (accurate to the name):
  * Path, Time, Value, Version
  *
  * Path - name of metric (sensor);
  * Time - time of measurement;
  * Value - value of measurement;
  * Version - a number, that for equal pairs of Path and Time, need to leave only record with maximum version.
  *
  * Each row in a table correspond to one value of one sensor.
  *
  * Pattern should contain function, retention scheme, or both of them. The order of patterns does mean as well:
  *   * Aggregation OR retention patterns should be first
  *   * Then aggregation AND retention full patterns have to be placed
  *   * default pattern without regexp must be the last
  *
  * Rollup rules are specified in the following way:
  *
  * pattern
  *     regexp
  *     function
  * pattern
  *     regexp
  *     age -> precision
  *     age -> precision
  *     ...
  * pattern
  *     regexp
  *     function
  *     age -> precision
  *     age -> precision
  *     ...
  * pattern
  *     ...
  * default
  *     function
  *        age -> precision
  *     ...
  *
  * regexp - pattern for sensor name
  * default - if no pattern has matched
  *
  * age - minimal data age (in seconds), to start rounding with specified precision.
  * precision - rounding precision (in seconds)
  *
  * function - name of aggregate function to be applied for values, that time was rounded to same.
  *
  * Example:
  *
  * <graphite_rollup>
  *     <pattern>
  *         <regexp>\.max$</regexp>
  *         <function>max</function>
  *     </pattern>
  *     <pattern>
  *         <regexp>click_cost</regexp>
  *         <function>any</function>
  *         <retention>
  *             <age>0</age>
  *             <precision>5</precision>
  *         </retention>
  *         <retention>
  *             <age>86400</age>
  *             <precision>60</precision>
  *         </retention>
  *     </pattern>
  *     <default>
  *         <function>max</function>
  *         <retention>
  *             <age>0</age>
  *             <precision>60</precision>
  *         </retention>
  *         <retention>
  *             <age>3600</age>
  *             <precision>300</precision>
  *         </retention>
  *         <retention>
  *             <age>86400</age>
  *             <precision>3600</precision>
  *         </retention>
  *     </default>
  * </graphite_rollup>
  */
namespace Graphite
{
    struct Retention
    {
        UInt32 age;
        UInt32 precision;
    };

    using Retentions = std::vector<Retention>;

    struct Pattern
    {
        std::shared_ptr<OptimizedRegularExpression> regexp;
        std::string regexp_str;
        AggregateFunctionPtr function;
        Retentions retentions;    /// Must be ordered by 'age' descending.
        enum { TypeUndef, TypeRetention, TypeAggregation, TypeAll } type = TypeAll; /// The type of defined pattern, filled automatically
    };

    using Patterns = std::vector<Pattern>;
    using RetentionPattern = Pattern;
    using AggregationPattern = Pattern;

    struct Params
    {
        String config_name;
        String path_column_name;
        String time_column_name;
        String value_column_name;
        String version_column_name;
        Graphite::Patterns patterns;
    };

    using RollupRule = std::pair<const RetentionPattern *, const AggregationPattern *>;
}

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
        size_t num_inputs, const Block & header,
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

    detail::SharedChunkAllocator chunk_allocator;

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
