#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <WindowFunctions/IWindowFunction.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/WindowTransform.h>
#include <WindowFunctions/helpers.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace
{

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

void registerWindowFunctionCumeDist(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionCumeDist(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
{
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
}

}
