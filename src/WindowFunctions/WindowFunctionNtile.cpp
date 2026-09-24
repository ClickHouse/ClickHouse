#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <WindowFunctions/IWindowFunction.h>
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

}

void registerWindowFunctionNtile(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionNtile(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
{
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
