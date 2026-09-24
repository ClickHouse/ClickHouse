#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/WindowFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/WindowTransform.h>
#include <WindowFunctions/helpers.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct NtileState
{
    UInt64 buckets = 0;
    RowNumber start_row;
    UInt64 current_partition_rows = 0;
    UInt64 current_partition_inserted_row = 0;

    void windowInsertResultInto(
        const WindowTransform * transform,
        size_t function_index,
        const DataTypes & argument_types)
    {
        if (!buckets) [[unlikely]]
        {
            const auto & current_block = transform->blockAt(transform->current_row);
            const auto & workspace = transform->workspaces[function_index];
            const auto & arg_col = *current_block.original_input_columns[workspace.argument_column_indices[0]];
            if (!isColumnConst(arg_col))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument of 'ntile' function must be a constant");
            auto type_id = argument_types[0]->getTypeId();
            if (type_id == TypeIndex::UInt8)
                buckets = arg_col[transform->current_row.row].safeGet<UInt8>();
            else if (type_id == TypeIndex::UInt16)
                buckets = arg_col[transform->current_row.row].safeGet<UInt16>();
            else if (type_id == TypeIndex::UInt32)
                buckets = arg_col[transform->current_row.row].safeGet<UInt32>();
            else if (type_id == TypeIndex::UInt64)
                buckets = arg_col[transform->current_row.row].safeGet<UInt64>();

            if (!buckets)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument of 'ntile' function must be greater than zero");
            }
        }
        // new partition
        if (WindowRowAccess::isPartitionFirstRow(transform)) [[unlikely]]
        {
            current_partition_rows = 0;
            current_partition_inserted_row = 0;
            start_row = transform->current_row;
        }
        current_partition_rows++;

        // Only do the action when we meet the last row in this partition.
        if (!WindowRowAccess::isPartitionLastRow(transform))
            return;

        auto bucket_capacity = current_partition_rows / buckets;
        auto capacity_diff = current_partition_rows - bucket_capacity * buckets;

        // bucket number starts from 1.
        UInt64 bucket_num = 1;
        while (current_partition_inserted_row < current_partition_rows)
        {
            auto current_bucket_capacity = bucket_capacity;
            if (capacity_diff > 0)
            {
                current_bucket_capacity += 1;
                capacity_diff--;
            }
            auto left_rows = current_bucket_capacity;
            while (left_rows)
            {
                auto available_block_rows = transform->blockRowsNumber(start_row) - start_row.row;
                IColumn & to = *transform->blockAt(start_row).output_columns[function_index];
                auto & pod_array = assert_cast<ColumnUInt64 &>(to).getData();
                if (left_rows < available_block_rows)
                {
                    pod_array.resize_fill(pod_array.size() + left_rows, bucket_num);
                    start_row.row += left_rows;
                    left_rows = 0;
                }
                else
                {
                    pod_array.resize_fill(pod_array.size() + available_block_rows, bucket_num);
                    left_rows -= available_block_rows;
                    start_row.block++;
                    start_row.row = 0;
                }
            }
            current_partition_inserted_row += current_bucket_capacity;
            bucket_num += 1;
        }
    }
};

// Usage: ntile(n). n is the number of buckets.
struct WindowFunctionNtile final : public StatefulWindowFunction<NtileState>
{
    WindowFunctionNtile(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : StatefulWindowFunction<NtileState>(name_, argument_types_, parameters_, std::make_shared<DataTypeUInt64>())
    {
        if (argument_types.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} takes exactly one argument", name_);

        auto type_id = argument_types[0]->getTypeId();
        if (type_id != TypeIndex::UInt8 && type_id != TypeIndex::UInt16 && type_id != TypeIndex::UInt32 && type_id != TypeIndex::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' argument type must be an unsigned integer (not larger than 64-bit), got {}", name_, argument_types[0]->getName());
    }

    bool allocatesMemoryInArena() const override { return false; }

    bool checkWindowFrameType(const WindowTransform * transform) const override
    {
        if (transform->order_by_indices.empty())
        {
            LOG_ERROR(getLogger("WindowFunctionNtile"), "Window frame for 'ntile' function must have ORDER BY clause");
            return false;
        }

        // We must wait all for the partition end and get the total rows number in this
        // partition. So before the end of this partition, there is no any block could be
        // dropped out.
        bool is_frame_supported = transform->window_description.frame.begin_type == WindowFrame::BoundaryType::Unbounded
            && transform->window_description.frame.end_type == WindowFrame::BoundaryType::Unbounded;
        if (!is_frame_supported)
        {
            LOG_ERROR(
                getLogger("WindowFunctionNtile"),
                "Window frame for function 'ntile' should be 'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING'");
            return false;
        }
        return true;
    }

    std::optional<WindowFrame> getDefaultFrame() const override
    {
        WindowFrame frame;
        frame.type = WindowFrame::FrameType::ROWS;
        frame.end_type = WindowFrame::BoundaryType::Unbounded;
        return frame;
    }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        const auto & workspace = transform->workspaces[function_index];
        auto & state = getState(workspace);
        state.windowInsertResultInto(transform, function_index, argument_types);
    }
};

struct PercentRankState
{
    RowNumber start_row;
    UInt64 current_partition_rows = 0;
};

struct WindowFunctionPercentRank final : public StatefulWindowFunction<PercentRankState>
{
    WindowFunctionPercentRank(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : StatefulWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
    {}

    bool allocatesMemoryInArena() const override { return false; }

    bool checkWindowFrameType(const WindowTransform * transform) const override
    {
        auto default_window_frame = getDefaultFrame();
        if (transform->window_description.frame != default_window_frame)
        {
            LOG_ERROR(
                getLogger("WindowFunctionPercentRank"),
                "Window frame for function 'percent_rank' should be '{}'", default_window_frame->toString());
            return false;
        }
        return true;
    }

    std::optional<WindowFrame> getDefaultFrame() const override
    {
        WindowFrame frame;
        frame.type = WindowFrame::FrameType::RANGE;
        frame.begin_type = WindowFrame::BoundaryType::Unbounded;
        frame.end_type = WindowFrame::BoundaryType::Unbounded;
        return frame;
    }

    void windowInsertResultInto(const WindowTransform * transform, size_t function_index) const override
    {
        auto & state = getWorkspaceState(transform, function_index);
        if (WindowRowAccess::isPartitionFirstRow(transform))
        {
            state.current_partition_rows = 0;
            state.start_row = transform->current_row;
        }

        insertRankIntoColumn(transform, function_index);
        state.current_partition_rows++;

        if (!WindowRowAccess::isPartitionLastRow(transform))
        {
            return;
        }

        UInt64 remaining_rows = state.current_partition_rows;
        Float64 percent_rank_denominator = remaining_rows == 1 ? 1 : static_cast<Float64>(remaining_rows - 1);

        while (remaining_rows > 0)
        {
            auto block_rows_number = transform->blockRowsNumber(state.start_row);
            auto available_block_rows = block_rows_number - state.start_row.row;
            if (available_block_rows <= remaining_rows)
            {
                /// This partition involves multiple blocks. Finish current block and move on to the
                /// next block.
                auto & to_column = *transform->blockAt(state.start_row).output_columns[function_index];
                auto & data = assert_cast<ColumnFloat64 &>(to_column).getData();
                for (size_t i = state.start_row.row; i < block_rows_number; ++i)
                    data[i] = (data[i] - 1) / percent_rank_denominator;

                state.start_row.block++;
                state.start_row.row = 0;
                remaining_rows -= available_block_rows;
            }
            else
            {
                /// The partition ends in current block.s
                auto & to_column = *transform->blockAt(state.start_row).output_columns[function_index];
                auto & data = assert_cast<ColumnFloat64 &>(to_column).getData();
                for (size_t i = state.start_row.row, n = state.start_row.row + remaining_rows; i < n; ++i)
                {
                    data[i] = (data[i] - 1) / percent_rank_denominator;
                }
                state.start_row.row += remaining_rows;
                remaining_rows = 0;
            }
        }
    }


    inline PercentRankState & getWorkspaceState(const WindowTransform * transform, size_t function_index) const
    {
        const auto & workspace = transform->workspaces[function_index];
        return getState(workspace);
    }

    inline void insertRankIntoColumn(const WindowTransform * transform, size_t function_index) const
    {
        auto & to_column = *transform->blockAt(transform->current_row).output_columns[function_index];
        assert_cast<ColumnFloat64 &>(to_column).getData().push_back(static_cast<Float64>(transform->peer_group_start_row_number));
    }
};

struct CumeDistState
{
    RowNumber start_row;
    UInt64 current_partition_rows = 0;

    // The peer-group-end row number is identical for every row in a peer group, so we compute it
    // once (by scanning forward to the last peer) when entering a new peer group and reuse it for
    // the rest of the group. Without this the per-row scan is O(k^2) for a peer group of size k.
    // 0 for not cached is safe because the first row in a partition is always peer group 1.
    UInt64 cached_peer_group_number = 0;
    UInt64 cached_peer_group_end_row_number = 0;
};

struct WindowFunctionCumeDist final : public StatefulWindowFunction<CumeDistState>
{
    WindowFunctionCumeDist(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : StatefulWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
    {}

    bool allocatesMemoryInArena() const override { return false; }

    bool checkWindowFrameType(const WindowTransform * transform) const override
    {
        auto default_window_frame = getDefaultFrame();
        if (transform->window_description.frame != default_window_frame)
        {
            LOG_ERROR(
                getLogger("WindowFunctionCumeDist"),
                "Window frame for function 'cume_dist' should be '{}'", default_window_frame->toString());
            return false;
        }
        return true;
    }

    std::optional<WindowFrame> getDefaultFrame() const override
    {
        WindowFrame frame;
        frame.type = WindowFrame::FrameType::RANGE;
        frame.begin_type = WindowFrame::BoundaryType::Unbounded;
        frame.end_type = WindowFrame::BoundaryType::Unbounded;
        return frame;
    }

    void windowInsertResultInto(const WindowTransform * transform, size_t function_index) const override
    {
        auto & state = getWorkspaceState(transform, function_index);
        if (WindowRowAccess::isPartitionFirstRow(transform))
        {
            state.current_partition_rows = 0;
            state.start_row = transform->current_row;
            state.cached_peer_group_number = 0;
        }

        insertPeerGroupEndRowNumberIntoColumn(transform, function_index, state);
        state.current_partition_rows++;

        if (!WindowRowAccess::isPartitionLastRow(transform))
        {
            return;
        }

        UInt64 remaining_rows = state.current_partition_rows;
        Float64 cume_dist_denominator = static_cast<Float64>(remaining_rows);

        while (remaining_rows > 0)
        {
            auto block_rows_number = transform->blockRowsNumber(state.start_row);
            auto available_block_rows = block_rows_number - state.start_row.row;
            if (available_block_rows <= remaining_rows)
            {
                auto & to_column = *transform->blockAt(state.start_row).output_columns[function_index];
                auto & data = assert_cast<ColumnFloat64 &>(to_column).getData();
                for (size_t i = state.start_row.row; i < block_rows_number; ++i)
                    data[i] = data[i] / cume_dist_denominator;

                state.start_row.block++;
                state.start_row.row = 0;
                remaining_rows -= available_block_rows;
            }
            else
            {
                auto & to_column = *transform->blockAt(state.start_row).output_columns[function_index];
                auto & data = assert_cast<ColumnFloat64 &>(to_column).getData();
                for (size_t i = state.start_row.row, n = state.start_row.row + remaining_rows; i < n; ++i)
                {
                    data[i] = data[i] / cume_dist_denominator;
                }
                state.start_row.row += remaining_rows;
                remaining_rows = 0;
            }
        }
    }


    inline CumeDistState & getWorkspaceState(const WindowTransform * transform, size_t function_index) const
    {
        const auto & workspace = transform->workspaces[function_index];
        return getState(workspace);
    }

    inline void insertPeerGroupEndRowNumberIntoColumn(const WindowTransform * transform, size_t function_index, CumeDistState & state) const
    {
        // The peer-group-end row number is the same for every row in a peer group. Recompute it (by
        // scanning forward to the last peer) only when we enter a new peer group; otherwise reuse the
        // cached value. This turns the per-peer-group cost from O(k^2) into O(k).
        if (state.cached_peer_group_number != transform->peer_group_number)
        {
            UInt64 peer_group_end_row_number = transform->current_row_number;
            RowNumber check_row = transform->current_row;

            // Advance through all rows that are peers with the current row
            while (true)
            {
                RowNumber next = transform->nextRowNumber(check_row);
                if (next >= transform->partition_end || !transform->arePeers(transform->current_row, next))
                    break;
                check_row = next;
                peer_group_end_row_number++;
            }

            state.cached_peer_group_number = transform->peer_group_number;
            state.cached_peer_group_end_row_number = peer_group_end_row_number;
        }

        auto & to_column = *transform->blockAt(transform->current_row).output_columns[function_index];
        assert_cast<ColumnFloat64 &>(to_column).getData().push_back(static_cast<Float64>(state.cached_peer_group_end_row_number));
    }
};

}

void registerWindowFunctionsDistribution(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsDistribution(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
{
    factory.registerFunction("percentRank", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionPercentRank>(name, argument_types,
                parameters);
        }, {.description = R"DOCS_MD(
returns the relative rank (i.e. percentile) of rows within a window partition.

**Syntax**

Alias: `percentRank` (case-sensitive)

```sql
percent_rank ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

The default and required window frame definition is `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

For more detail on window function syntax see: [Window Functions - Syntax](/reference/functions/window-functions/index#syntax).

**Example**

```sql title="Query"
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT Values
    ('Port Elizabeth Barbarians', 'Gary Chen', 195000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 150000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 150000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql title="Query"
SELECT player, salary,
       percent_rank() OVER (ORDER BY salary DESC) AS percent_rank
FROM salaries;
```

```response title="Response"

   ┌─player──────────┬─salary─┬───────percent_rank─┐
1. │ Gary Chen       │ 195000 │                  0 │
2. │ Robert George   │ 195000 │                  0 │
3. │ Charles Juarez  │ 190000 │ 0.3333333333333333 │
4. │ Michael Stanley │ 150000 │                0.5 │
5. │ Scott Harrison  │ 150000 │                0.5 │
6. │ Douglas Benson  │ 150000 │                0.5 │
7. │ James Henderson │ 140000 │                  1 │
   └─────────────────┴────────┴────────────────────┘

```
)DOCS_MD", .category = FunctionDocumentation::Category::AggregateFunction}, properties});

    factory.registerAlias("percent_rank", "percentRank", AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction("cume_dist", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionCumeDist>(name, argument_types,
                parameters);
        }, {.description = R"DOCS_MD(
Computes the cumulative distribution of a value within a group of values, i.e., the percentage of rows with values less than or equal to the current row's value. Can be used to determine relative standing of a value within a partition.

**Syntax**

```sql
cume_dist ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

The default and required window frame definition is `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

For more detail on window function syntax see: [Window Functions - Syntax](/reference/functions/window-functions/index#syntax).

**Returned value**

- The relative rank of the current row. The return type is Float64 in the range [0, 1]. [Float64](/reference/data-types/float).

**Example**

The following example calculates the cumulative distribution of salaries within a team:

```sql title="Query"
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT Values
    ('Port Elizabeth Barbarians', 'Gary Chen', 195000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 150000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 150000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql title="Query"
SELECT player, salary,
       cume_dist() OVER (ORDER BY salary DESC) AS cume_dist
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬───────────cume_dist─┐
1. │ Robert George   │ 195000 │  0.2857142857142857 │
2. │ Gary Chen       │ 195000 │  0.2857142857142857 │
3. │ Charles Juarez  │ 190000 │ 0.42857142857142855 │
4. │ Douglas Benson  │ 150000 │  0.8571428571428571 │
5. │ Michael Stanley │ 150000 │  0.8571428571428571 │
6. │ Scott Harrison  │ 150000 │  0.8571428571428571 │
7. │ James Henderson │ 140000 │                   1 │
   └─────────────────┴────────┴─────────────────────┘
```

**Implementation Details**

The `cume_dist()` function calculates the relative position using the following formula:

```text
cume_dist = (number of rows ≤ current row value) / (total number of rows in partition)
```

Rows with equal values (peers) receive the same cumulative distribution value, which corresponds to the highest position of the peer group.
)DOCS_MD", .category = FunctionDocumentation::Category::AggregateFunction}, properties});

    factory.registerFunction("ntile", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionNtile>(name, argument_types,
                parameters);
        }, {.description = R"DOCS_MD(
Divides the ordered rows within a partition into a specified number of buckets (groups) of as equal a size as possible, and returns the bucket number that the current row belongs to. Buckets are numbered starting from 1. For each partition, the rows are assigned to buckets in order: if the number of rows is not divisible by the number of buckets, the earlier buckets receive one more row than the later ones.

**Syntax**

```sql
ntile (buckets)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

The argument `buckets` must be a constant positive integer.

An `ORDER BY` clause is required. The window frame must be the whole partition (`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`), which is also the default frame used when none is specified explicitly.

For more detail on window function syntax see: [Window Functions - Syntax](/reference/functions/window-functions/index#syntax).

**Returned value**

- The bucket number of the current row within its partition. [UInt64](/reference/data-types/int-uint).

**Example**

The following example divides the players into four buckets ordered by descending salary.

```sql title="Query"
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT Values
    ('Port Elizabeth Barbarians', 'Gary Chen', 195000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 150000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 150000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql title="Query"
SELECT player, salary,
       ntile(4) OVER (ORDER BY salary DESC, player ASC) AS bucket
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─bucket─┐
1. │ Gary Chen       │ 195000 │      1 │
2. │ Robert George   │ 195000 │      1 │
3. │ Charles Juarez  │ 190000 │      2 │
4. │ Douglas Benson  │ 150000 │      2 │
5. │ Michael Stanley │ 150000 │      3 │
6. │ Scott Harrison  │ 150000 │      3 │
7. │ James Henderson │ 140000 │      4 │
   └─────────────────┴────────┴────────┘
```

Here there are seven rows and four buckets, so the first three buckets contain two rows each and the last bucket contains a single row.
)DOCS_MD", .category = FunctionDocumentation::Category::AggregateFunction}, properties}, AggregateFunctionFactory::Case::Insensitive);
}

}
