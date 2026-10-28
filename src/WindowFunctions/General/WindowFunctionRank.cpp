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

class WindowFunctionRank final : public IGeneralWindowFunction
{
public:
    WindowFunctionRank(const String & name_, const DataTypes & argument_types_, const Array & parameters_)
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
            data.push_back(bounds.peers_start[i] + 1);
    }
};

}

void registerWindowFunctionRank(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionRank(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
{
    factory.registerFunction("rank", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings * settings)
        {
            // The `RANK` window function takes no arguments per SQL standard.
            // ClickHouse historically accepted and silently ignored arbitrary arguments,
            // which caused user confusion (issue #49526). Reject them by default; the
            // legacy permissive behavior is gated behind `allow_rank_dense_rank_arguments`.
            if (!argument_types.empty() && (settings == nullptr || !(*settings)[Setting::allow_rank_dense_rank_arguments]))
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for window function {} doesn't match: passed {}, should be 0. "
                    "Set `allow_rank_dense_rank_arguments = 1` to restore the legacy behavior of silently ignoring arguments.",
                    name, argument_types.size());
            return std::make_shared<WindowFunction>(std::make_shared<WindowFunctionRank>(name, argument_types, parameters));
        }, {.description = R"DOCS_MD(
Ranks the current row within its partition with gaps. In other words, if the value of any row it encounters is equal to the value of a previous row then it will receive the same rank as that previous row.
The rank of the next row is then equal to the rank of the previous row plus a gap equal to the number of times the previous rank was given.

The [dense_rank](/reference/functions/window-functions/dense_rank) function provides the same behaviour but without gaps in ranking.

**Syntax**

```sql
rank ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS, RANGE, or GROUPS expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

For more detail on window function syntax see: [Window Functions - Syntax](/reference/functions/window-functions/index#syntax).

**Returned value**

- A number for the current row within its partition, including gaps. [UInt64](/reference/data-types/int-uint).

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
       rank() OVER (ORDER BY salary DESC) AS rank
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─rank─┐
1. │ Gary Chen       │ 195000 │    1 │
2. │ Robert George   │ 195000 │    1 │
3. │ Charles Juarez  │ 190000 │    3 │
4. │ Douglas Benson  │ 150000 │    4 │
5. │ Michael Stanley │ 150000 │    4 │
6. │ Scott Harrison  │ 150000 │    4 │
7. │ James Henderson │ 140000 │    7 │
   └─────────────────┴────────┴──────┘
```
)DOCS_MD", .category = FunctionDocumentation::Category::AggregateFunction}, properties}, AggregateFunctionFactory::Case::Insensitive);
}

}
