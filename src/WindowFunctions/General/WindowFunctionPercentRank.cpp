#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <WindowFunctions/Common/WindowBounds.h>
#include <WindowFunctions/IGeneralWindowFunction.h>
#include <WindowFunctions/WindowFunction.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

WindowFrame wholePartitionRangeFrame()
{
    WindowFrame frame;
    frame.type = WindowFrame::FrameType::RANGE;
    frame.begin_type = WindowFrame::BoundaryType::Unbounded;
    frame.end_type = WindowFrame::BoundaryType::Unbounded;
    return frame;
}

class WindowFunctionPercentRank final : public IGeneralWindowFunction
{
public:
    WindowFunctionPercentRank(const String & name_, const DataTypes & argument_types_, const Array & parameters_)
        : IGeneralWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
    {
    }

    WindowFunctionRequirements getRequirements(const WindowDescription &) const override
    {
        return WindowFunctionRequirements{.peers = true, .partition = true};
    }

    std::optional<WindowFrame> getDefaultFrame() const override
    {
        return wholePartitionRangeFrame();
    }

    void checkWindowFrameType(const WindowDescription & window) const override
    {
        if (window.frame != wholePartitionRangeFrame())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window frame for function '{}' should be '{}', '{}' given", getName(), wholePartitionRangeFrame().toString(), window.frame.toString());
    }

    void evaluate(char *, const WindowBounds & bounds, const WindowArguments &, IColumn & result) const override
    {
        auto & data = assert_cast<ColumnFloat64 &>(result).getData();
        for (size_t i = 0; i < bounds.rows; ++i)
        {
            const Int64 partition_rows = bounds.partition_end[i];
            const Float64 denominator = partition_rows == 1 ? 1 : static_cast<Float64>(partition_rows - 1);
            data.push_back(static_cast<Float64>(bounds.peers_start[i]) / denominator);
        }
    }
};

}

void registerWindowFunctionPercentRank(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionPercentRank(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
{
    factory.registerFunction("percentRank", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunction>(std::make_shared<WindowFunctionPercentRank>(name, argument_types, parameters));
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
