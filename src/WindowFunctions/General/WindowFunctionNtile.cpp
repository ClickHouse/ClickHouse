#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <WindowFunctions/Common/WindowArguments.h>
#include <WindowFunctions/Common/WindowBounds.h>
#include <WindowFunctions/IGeneralWindowFunction.h>
#include <WindowFunctions/WindowFunction.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <limits>

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
    Int64 buckets = 0;
};

Int64 checkedBuckets(UInt64 value)
{
    if (value == 0 || value > std::numeric_limits<Int64>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument of 'ntile' function must be in [1, {}], {} given", std::numeric_limits<Int64>::max(), value);

    return value;
}

class WindowFunctionNtile final : public IGeneralWindowFunction
{
public:
    WindowFunctionNtile(const String & name_, const DataTypes & argument_types_, const Array & parameters_)
        : IGeneralWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeUInt64>())
    {
        if (argument_types.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} takes exactly one argument", name_);

        auto type_id = argument_types[0]->getTypeId();
        if (type_id != TypeIndex::UInt8 && type_id != TypeIndex::UInt16 && type_id != TypeIndex::UInt32 && type_id != TypeIndex::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' argument type must be an unsigned integer (not larger than 64-bit), got {}", name_, argument_types[0]->getName());
    }

    WindowFunctionRequirements getRequirements(const WindowDescription &) const override
    {
        return WindowFunctionRequirements{.frame = false, .partition = true, .order_by = true};
    }

    size_t sizeOfState() const override { return sizeof(NtileState); }
    size_t alignOfState() const override { return alignof(NtileState); }
    void createState(char * place) const override { new (place) NtileState(); }
    void resetPartition(char * place) const override { new (place) NtileState(); }

    std::optional<WindowFrame> getDefaultFrame() const override
    {
        WindowFrame frame;
        frame.type = WindowFrame::FrameType::ROWS;
        frame.end_type = WindowFrame::BoundaryType::Unbounded;
        return frame;
    }

    void checkWindowFrameType(const WindowDescription & window) const override
    {
        if (window.frame.begin_type != WindowFrame::BoundaryType::Unbounded || window.frame.end_type != WindowFrame::BoundaryType::Unbounded)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window frame for function '{}' should be 'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING', '{}' given", getName(), window.frame.toString());
    }

    void evaluate(char * place, const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result) const override
    {
        NtileState & state = *reinterpret_cast<NtileState *>(place);
        if (state.buckets == 0)
            state.buckets = checkedBuckets(arguments.at(0, bounds.row[0]).getUInt());

        const Int64 partition_rows = bounds.partition_end[0];
        const Int64 small_bucket_rows = partition_rows / state.buckets;
        const Int64 large_buckets = partition_rows - small_bucket_rows * state.buckets;
        const Int64 large_bucket_rows = small_bucket_rows + 1;
        const Int64 rows_in_large_buckets = large_buckets * large_bucket_rows;
        auto & data = assert_cast<ColumnUInt64 &>(result).getData();
        for (size_t i = 0; i < bounds.rows; ++i)
        {
            const Int64 row = bounds.row[i];
            const Int64 bucket = row < rows_in_large_buckets ? row / large_bucket_rows : large_buckets + (row - rows_in_large_buckets) / small_bucket_rows;
            data.push_back(bucket + 1);
        }
    }
};

}

void registerWindowFunctionNtile(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionNtile(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
{
    factory.registerFunction("ntile", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunction>(std::make_shared<WindowFunctionNtile>(name, argument_types, parameters));
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
