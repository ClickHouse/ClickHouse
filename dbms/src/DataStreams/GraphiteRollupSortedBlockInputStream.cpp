#include <DataStreams/GraphiteRollupSortedBlockInputStream.h>
#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int LOGICAL_ERROR;
}


GraphiteRollupSortedBlockInputStream::GraphiteRollupSortedBlockInputStream(
    const BlockInputStreams & inputs_, const SortDescription & description_, size_t max_block_size_,
    const Graphite::Params & params, time_t time_of_merge)
    : MergingSortedBlockInputStream(inputs_, description_, max_block_size_),
    params(params), time_of_merge(time_of_merge)
{
    size_t max_size_of_aggregate_state = 0;
    size_t max_alignment_of_aggregate_state = 1;

    for (const auto & pattern : params.patterns)
    {
        if (pattern.function)
        {
            max_size_of_aggregate_state = std::max(max_size_of_aggregate_state, pattern.function->sizeOfData());
            max_alignment_of_aggregate_state = std::max(max_alignment_of_aggregate_state, pattern.function->alignOfData());
        }
    }

    place_for_aggregate_state.reset(max_size_of_aggregate_state, max_alignment_of_aggregate_state);

    /// Memoize column numbers in block.
    path_column_num = header.getPositionByName(params.path_column_name);
    time_column_num = header.getPositionByName(params.time_column_name);
    value_column_num = header.getPositionByName(params.value_column_name);
    version_column_num = header.getPositionByName(params.version_column_name);

    for (size_t i = 0; i < num_columns; ++i)
        if (i != time_column_num && i != value_column_num && i != version_column_num)
            unmodified_column_numbers.push_back(i);
}


Graphite::RollupRule GraphiteRollupSortedBlockInputStream::selectPatternForPath(StringRef path) const
{
    const Graphite::Pattern * first_match = &undef_pattern;

    for (const auto & pattern : params.patterns)
    {
        if (!pattern.regexp)
        {
            /// Default pattern
            if (first_match->type == first_match->TypeUndef && pattern.type == pattern.TypeAll)
            {
                /// There is only default pattern for both retention and aggregation
                return std::pair(&pattern, &pattern);
            }
            if (pattern.type != first_match->type)
            {
                if (first_match->type == first_match->TypeRetention)
                {
                    return std::pair(first_match, &pattern);
                }
                if (first_match->type == first_match->TypeAggregation)
                {
                    return std::pair(&pattern, first_match);
                }
            }
        }
        else if (pattern.regexp->match(path.data, path.size))
        {
            /// General pattern with matched path
            if (pattern.type == pattern.TypeAll)
            {
               /// Only for not default patterns with both function and retention parameters
               return std::pair(&pattern, &pattern);
            }
            if (first_match->type == first_match->TypeUndef)
            {
                first_match = &pattern;
                continue;
            }
            if (pattern.type != first_match->type)
            {
                if (first_match->type == first_match->TypeRetention)
                {
                    return std::pair(first_match, &pattern);
                }
                if (first_match->type == first_match->TypeAggregation)
                {
                    return std::pair(&pattern, first_match);
                }
            }
        }
    }

    return {nullptr, nullptr};
}


UInt32 GraphiteRollupSortedBlockInputStream::selectPrecision(const Graphite::Retentions & retentions, time_t time) const
{
    static_assert(std::is_signed_v<time_t>, "time_t must be signed type");

    for (const auto & retention : retentions)
    {
        if (time_of_merge - time >= static_cast<time_t>(retention.age))
            return retention.precision;
    }

    /// No rounding.
    return 1;
}


/** Round the unix timestamp to seconds precision.
  * In this case, the date should not change. The date is calculated using the local time zone.
  *
  * If the rounding value is less than an hour,
  *  then, assuming that time zones that differ from UTC by a non-integer number of hours are not supported,
  *  just simply round the unix timestamp down to a multiple of 3600.
  * And if the rounding value is greater,
  *  then we will round down the number of seconds from the beginning of the day in the local time zone.
  *
  * Rounding to more than a day is not supported.
  */
static time_t roundTimeToPrecision(const DateLUTImpl & date_lut, time_t time, UInt32 precision)
{
    if (precision <= 3600)
    {
        return time / precision * precision;
    }
    else
    {
        time_t date = date_lut.toDate(time);
        time_t remainder = time - date;
        return date + remainder / precision * precision;
    }
}


Block GraphiteRollupSortedBlockInputStream::readImpl()
{
    if (finished)
        return Block();

    MutableColumns merged_columns;
    init(merged_columns);

    if (has_collation)
        throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

    if (merged_columns.empty())
        return Block();

    merge(merged_columns, queue_without_collation);
    return header.cloneWithColumns(std::move(merged_columns));
}


void GraphiteRollupSortedBlockInputStream::merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue)
{
    const DateLUTImpl & date_lut = DateLUT::instance();

    size_t started_rows = 0; /// Number of times startNextRow() has been called.

    /// Take rows in needed order and put them into `merged_columns` until we get `max_block_size` rows.
    ///
    /// Variables starting with current_* refer to the rows previously popped from the queue that will
    /// contribute towards current output row.
    /// Variables starting with next_* refer to the row at the top of the queue.

    while (!queue.empty())
    {
        SortCursor next_cursor = queue.top();

        StringRef next_path = next_cursor->all_columns[path_column_num]->getDataAt(next_cursor->pos);
        bool new_path = is_first || next_path != current_group_path;

        is_first = false;

        time_t next_row_time = next_cursor->all_columns[time_column_num]->getUInt(next_cursor->pos);
        /// Is new key before rounding.
        bool is_new_key = new_path || next_row_time != current_time;

        if (is_new_key)
        {
            /// Accumulate the row that has maximum version in the previous group of rows with the same key:
            if (started_rows)
                accumulateRow(current_subgroup_newest_row);

            Graphite::RollupRule next_rule = current_rule;
            if (new_path)
                next_rule = selectPatternForPath(next_path);

            const Graphite::RetentionPattern * retention_pattern = std::get<0>(next_rule);
            time_t next_time_rounded;
            if (retention_pattern)
            {
                UInt32 precision = selectPrecision(retention_pattern->retentions, next_row_time);
                next_time_rounded = roundTimeToPrecision(date_lut, next_row_time, precision);
            }
            else
            {
                /// If no pattern has matched - take the value as-is.
                next_time_rounded = next_row_time;
            }

            /// Key will be new after rounding. It means new result row.
            bool will_be_new_key = new_path || next_time_rounded != current_time_rounded;

            if (will_be_new_key)
            {
                if (started_rows)
                {
                    finishCurrentGroup(merged_columns);

                    /// We have enough rows - return, but don't advance the loop. At the beginning of the
                    /// next call to merge() the same next_cursor will be processed once more and
                    /// the next output row will be created from it.
                    if (started_rows >= max_block_size)
                        return;
                }

                /// At this point previous row has been fully processed, so we can advance the loop
                /// (substitute current_* values for next_*, advance the cursor).

                startNextGroup(merged_columns, next_cursor, next_rule);
                ++started_rows;

                current_time_rounded = next_time_rounded;
            }

            current_time = next_row_time;
        }

        /// Within all rows with same key, we should leave only one row with maximum version;
        /// and for rows with same maximum version - only last row.
        if (is_new_key
            || next_cursor->all_columns[version_column_num]->compareAt(
                next_cursor->pos, current_subgroup_newest_row.row_num,
                *(*current_subgroup_newest_row.columns)[version_column_num],
                /* nan_direction_hint = */ 1) >= 0)
        {
            setRowRef(current_subgroup_newest_row, next_cursor);

            /// Small hack: group and subgroups have the same path, so we can set current_group_path here instead of startNextGroup
            /// But since we keep in memory current_subgroup_newest_row's block, we could use StringRef for current_group_path and don't
            ///  make deep copy of the path.
            current_group_path = next_path;
        }

        queue.pop();

        if (!next_cursor->isLast())
        {
            next_cursor->next();
            queue.push(next_cursor);
        }
        else
        {
            /// We get the next block from the appropriate source, if there is one.
            fetchNextBlock(next_cursor, queue);
        }
    }

    /// Write result row for the last group.
    if (started_rows)
    {
        accumulateRow(current_subgroup_newest_row);
        finishCurrentGroup(merged_columns);
    }

    finished = true;
}


template <typename TSortCursor>
void GraphiteRollupSortedBlockInputStream::startNextGroup(MutableColumns & merged_columns, TSortCursor & cursor,
                                                          Graphite::RollupRule next_rule)
{
    const Graphite::AggregationPattern * aggregation_pattern = std::get<1>(next_rule);

    /// Copy unmodified column values (including path column).
    for (size_t i = 0, size = unmodified_column_numbers.size(); i < size; ++i)
    {
        size_t j = unmodified_column_numbers[i];
        merged_columns[j]->insertFrom(*cursor->all_columns[j], cursor->pos);
    }

    if (aggregation_pattern)
    {
        aggregation_pattern->function->create(place_for_aggregate_state.data());
        aggregate_state_created = true;
    }

    current_rule = next_rule;
}


void GraphiteRollupSortedBlockInputStream::finishCurrentGroup(MutableColumns & merged_columns)
{
    /// Insert calculated values of the columns `time`, `value`, `version`.
    merged_columns[time_column_num]->insert(current_time_rounded);
    merged_columns[version_column_num]->insertFrom(
        *(*current_subgroup_newest_row.columns)[version_column_num], current_subgroup_newest_row.row_num);

    const Graphite::AggregationPattern * aggregation_pattern = std::get<1>(current_rule);
    if (aggregate_state_created)
    {
        aggregation_pattern->function->insertResultInto(place_for_aggregate_state.data(), *merged_columns[value_column_num]);
        aggregation_pattern->function->destroy(place_for_aggregate_state.data());
        aggregate_state_created = false;
    }
    else
        merged_columns[value_column_num]->insertFrom(
            *(*current_subgroup_newest_row.columns)[value_column_num], current_subgroup_newest_row.row_num);
}


void GraphiteRollupSortedBlockInputStream::accumulateRow(RowRef & row)
{
    const Graphite::AggregationPattern * aggregation_pattern = std::get<1>(current_rule);
    if (aggregate_state_created)
        aggregation_pattern->function->add(place_for_aggregate_state.data(), &(*row.columns)[value_column_num], row.row_num, nullptr);
}

}
