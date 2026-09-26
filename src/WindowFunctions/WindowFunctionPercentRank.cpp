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

}

void registerWindowFunctionPercentRank(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionPercentRank(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
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
}

}
