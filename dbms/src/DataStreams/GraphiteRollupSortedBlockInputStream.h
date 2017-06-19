#pragma once

#include <common/logger_useful.h>

#include <Core/Row.h>
#include <Core/ColumnNumbers.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/OptimizedRegularExpression.h>


namespace DB
{

/** Intended for implementation of "rollup" - aggregation (rounding) of older data
  *  for a table with Graphite data (Graphite is the system for time series monitoring).
  *
  * Table with graphite data has at least the folowing columns (accurate to the name):
  * Path, Time, Value, Version
  *
  * Path - name of metric (sensor);
  * Time - time of measurement;
  * Value - value of measurement;
  * Version - a number, that for equal pairs of Path and Time, need to leave only record with maximum version.
  *
  * Each row in a table correspond to one value of one sensor.
  *
  * Rollup rules are specified in the following way:
  *
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
        AggregateFunctionPtr function;
        Retentions retentions;    /// Must be ordered by 'age' descending.
    };

    using Patterns = std::vector<Pattern>;

    struct Params
    {
        String path_column_name;
        String time_column_name;
        String value_column_name;
        String version_column_name;
        Graphite::Patterns patterns;
    };
}

/** Merges several sorted streams into one.
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
class GraphiteRollupSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    GraphiteRollupSortedBlockInputStream(
        BlockInputStreams inputs_, const SortDescription & description_, size_t max_block_size_,
        const Graphite::Params & params, time_t time_of_merge)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_),
        params(params), time_of_merge(time_of_merge)
    {
    }

    String getName() const override { return "GraphiteRollupSorted"; }

    String getID() const override
    {
        std::stringstream res;
        res << "GraphiteRollupSorted(inputs";

        for (size_t i = 0; i < children.size(); ++i)
            res << ", " << children[i]->getID();

        res << ", description";

        for (size_t i = 0; i < description.size(); ++i)
            res << ", " << description[i].getID();

        res << ")";
        return res.str();
    }

    ~GraphiteRollupSortedBlockInputStream()
    {
        if (aggregate_state_created)
            current_pattern->function->destroy(place_for_aggregate_state.data());
    }

protected:
    Block readImpl() override;

private:
    Logger * log = &Logger::get("GraphiteRollupSortedBlockInputStream");

    const Graphite::Params params;

    size_t path_column_num;
    size_t time_column_num;
    size_t value_column_num;
    size_t version_column_num;

    /// All columns other than 'time', 'value', 'version'. They are unmodified during rollup.
    ColumnNumbers unmodified_column_numbers;

    time_t time_of_merge;

    /// All data has been read.
    bool finished = false;

    RowRef current_selected_row;        /// Last row with maximum version for current primary key.
    UInt64 current_max_version = 0;

    bool is_first = true;
    StringRef current_path;
    time_t current_time = 0;
    time_t current_time_rounded = 0;

    const Graphite::Pattern * current_pattern = nullptr;
    std::vector<char> place_for_aggregate_state;
    bool aggregate_state_created = false; /// Invariant: if true then current_pattern is not NULL.

    const Graphite::Pattern * selectPatternForPath(StringRef path) const;
    UInt32 selectPrecision(const Graphite::Retentions & retentions, time_t time) const;


    template <typename TSortCursor>
    void merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);

    /// Insert the values into the resulting columns, which will not be changed in the future.
    template <class TSortCursor>
    void startNextRow(ColumnPlainPtrs & merged_columns, TSortCursor & cursor, const Graphite::Pattern * next_pattern);

    /// Insert the calculated `time`, `value`, `version` values into the resulting columns by the last group of rows.
    void finishCurrentRow(ColumnPlainPtrs & merged_columns);

    /// Update the state of the aggregate function with the new `value`.
    void accumulateRow(RowRef & row);
};

}
