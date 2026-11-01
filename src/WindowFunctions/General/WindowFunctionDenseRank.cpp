#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <WindowFunctions/Common/WindowBounds.h>
#include <WindowFunctions/IGeneralWindowFunction.h>
#include <WindowFunctions/WindowFunction.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_rank_dense_rank_arguments;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class WindowFunctionDenseRank final : public IGeneralWindowFunction
{
public:
    WindowFunctionDenseRank(const String & name_, const DataTypes & argument_types_, const Array & parameters_)
        : IGeneralWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeUInt64>())
    {
    }

    WindowFunctionRequirements getRequirements(const WindowDescription &) const override
    {
        return WindowFunctionRequirements{.peers = true};
    }

    void evaluate(char *, const WindowBounds & bounds, const WindowArguments &, IColumn & result) const override
    {
        auto & data = assert_cast<ColumnUInt64 &>(result).getData();
        for (size_t i = 0; i < bounds.rows; ++i)
            data.push_back(bounds.peer_group_index[i] + 1);
    }
};

}

void registerWindowFunctionDenseRank(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionDenseRank(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
{
    factory.registerFunction("denseRank", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings * settings)
        {
            // The `DENSE_RANK` window function takes no arguments per SQL standard.
            // See `rank` registration above for the rationale.
            if (!argument_types.empty() && (settings == nullptr || !(*settings)[Setting::allow_rank_dense_rank_arguments]))
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for window function {} doesn't match: passed {}, should be 0. "
                    "Set `allow_rank_dense_rank_arguments = 1` to restore the legacy behavior of silently ignoring arguments.",
                    name, argument_types.size());
            return std::make_shared<WindowFunction>(std::make_shared<WindowFunctionDenseRank>(name, argument_types, parameters));
        }, {.description = R"DOCS_MD(
Ranks the current row within its partition without gaps. In other words, if the value of any new row encountered is equal to the value of one of the previous rows then it will receive the next successive rank without any gaps in ranking.

The [rank](/reference/functions/window-functions/rank) function provides the same behaviour, but with gaps in ranking.

**Syntax**

Alias: `denseRank` (case-sensitive)

```sql
dense_rank ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS, RANGE, or GROUPS expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

For more detail on window function syntax see: [Window Functions - Syntax](/reference/functions/window-functions/index#syntax).

**Returned value**

- A number for the current row within its partition, without gaps in ranking. [UInt64](/reference/data-types/int-uint).

**Example**

The following example is based on the example provided in the video instructional [Ranking window functions in ClickHouse](https://youtu.be/Yku9mmBYm_4?si=XIMu1jpYucCQEoXA).

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
       dense_rank() OVER (ORDER BY salary DESC) AS dense_rank
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─dense_rank─┐
1. │ Gary Chen       │ 195000 │          1 │
2. │ Robert George   │ 195000 │          1 │
3. │ Charles Juarez  │ 190000 │          2 │
4. │ Michael Stanley │ 150000 │          3 │
5. │ Douglas Benson  │ 150000 │          3 │
6. │ Scott Harrison  │ 150000 │          3 │
7. │ James Henderson │ 140000 │          4 │
   └─────────────────┴────────┴────────────┘
```
)DOCS_MD", .category = FunctionDocumentation::Category::AggregateFunction}, properties});

    factory.registerAlias("dense_rank", "denseRank", AggregateFunctionFactory::Case::Insensitive);
}

}
