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

class WindowFunctionCumeDist final : public IGeneralWindowFunction
{
public:
    WindowFunctionCumeDist(const String & name_, const DataTypes & argument_types_, const Array & parameters_)
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
            data.push_back(static_cast<Float64>(bounds.peers_end[i]) / static_cast<Float64>(bounds.partition_end[i]));
    }
};

}

void registerWindowFunctionCumeDist(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionCumeDist(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
{
    factory.registerFunction("cume_dist", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunction>(std::make_shared<WindowFunctionCumeDist>(name, argument_types, parameters));
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
