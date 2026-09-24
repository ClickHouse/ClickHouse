#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <WindowFunctions/IWindowFunction.h>
#include <Processors/Transforms/WindowTransform.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct WindowFunctionNthValue final : public StatelessWindowFunction
{
    WindowFunctionNthValue(const std::string & name_, const DataTypes & argument_types_, const Array & parameters_)
        : StatelessWindowFunction(name_, argument_types_, parameters_, createResultType(name_, argument_types_))
    {
        if (!parameters.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {} cannot be parameterized", name_);
        }

        if (!isInt64OrUInt64FieldType(argument_types[1]->getDefault().getType()))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Offset must be an integer, '{}' given",
                argument_types[1]->getName());
        }
    }

    static DataTypePtr createResultType(const std::string & name_, const DataTypes & argument_types_)
    {
        if (argument_types_.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly two arguments", name_);
        }

        return argument_types_[0];
    }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        const auto & current_block = transform->blockAt(transform->current_row);
        IColumn & to = *current_block.output_columns[function_index];
        const auto & workspace = transform->workspaces[function_index];

        Int64 offset = (*current_block.input_columns[
                workspace.argument_column_indices[1]])[
            transform->current_row.row].safeGet<Int64>();

        /// Either overflow or really negative value, both is not acceptable.
        if (offset <= 0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The offset for function {} must be in (1, {}], {} given",
                getName(), INT64_MAX, offset);
        }

        --offset;
        const auto [target_row, offset_left] = transform->moveRowNumber(transform->frame_start, offset);
        if (offset_left != 0
            || target_row < transform->frame_start
            || transform->frame_end <= target_row)
        {
            // Offset is outside the frame.
            to.insertDefault();
        }
        else
        {
            // Offset is inside the frame.
            to.insertFrom(*transform->blockAt(target_row).input_columns[
                    workspace.argument_column_indices[0]],
               target_row.row);
        }
    }
};

}

void registerWindowFunctionsNthValue(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsNthValue(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
{
    factory.registerFunction("nth_value", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionNthValue>(
                name, argument_types, parameters);
        }, {.description = R"DOCS_MD(
Returns the first non-NULL value evaluated against the nth row (offset) in its ordered frame.

**Syntax**

```sql
nth_value (x, offset)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS, RANGE, or GROUPS expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

For more detail on window function syntax see: [Window Functions - Syntax](/reference/functions/window-functions/index#syntax).

**Parameters**

- `x` — Column name.
- `offset` — nth row to evaluate current row against.

**Returned value**

- The first non-NULL value evaluated against the nth row (offset) in its ordered frame.

**Example**

In this example the `nth-value` function is used to find the third-highest salary from a fictional dataset of salaries of Premier League football players.

```sql title="Query"
DROP TABLE IF EXISTS salaries;
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
    ('Port Elizabeth Barbarians', 'Michael Stanley', 100000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 180000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql title="Query"
SELECT player, salary, nth_value(player,3) OVER(ORDER BY salary DESC) AS third_highest_salary FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─third_highest_salary─┐
1. │ Gary Chen       │ 195000 │                      │
2. │ Robert George   │ 195000 │                      │
3. │ Charles Juarez  │ 190000 │ Charles Juarez       │
4. │ Scott Harrison  │ 180000 │ Charles Juarez       │
5. │ Douglas Benson  │ 150000 │ Charles Juarez       │
6. │ James Henderson │ 140000 │ Charles Juarez       │
7. │ Michael Stanley │ 100000 │ Charles Juarez       │
   └─────────────────┴────────┴──────────────────────┘
```
)DOCS_MD", .category = FunctionDocumentation::Category::AggregateFunction}, properties}, AggregateFunctionFactory::Case::Insensitive);
}

}
