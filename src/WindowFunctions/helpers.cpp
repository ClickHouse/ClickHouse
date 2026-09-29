#include <WindowFunctions/helpers.h>

#include <Columns/ColumnsNumber.h>
#include <Processors/Transforms/WindowTransform.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace WindowRowAccess
{

Float64 getArgumentFloat64(const WindowTransform * transform, size_t function_index, size_t argument_index, RowNumber row)
{
    const auto & workspace = transform->workspaces[function_index];
    const auto & column = transform->blockAt(row.block).input_columns[workspace.argument_column_indices[argument_index]];
    return column->getFloat64(row.row);
}

void insertResultFloat64(const WindowTransform * transform, size_t function_index, Float64 value)
{
    IColumn & to = *transform->blockAt(transform->current_row).output_columns[function_index];
    assert_cast<ColumnFloat64 &>(to).getData().push_back(value);
}

bool isPartitionFirstRow(const WindowTransform * transform)
{
    return transform->current_row_number == 1;
}

bool isPartitionLastRow(const WindowTransform * transform)
{
    /// This is for fast check.
    if (!transform->partition_ended)
        return false;

    auto current_row = transform->current_row;
    /// isPartitionLastRow is called on each row, also move on current_row.row here.
    current_row.row++;
    const auto & partition_end_row = transform->partition_end;

    /// The partition end is reached, when following is true
    /// - current row is the partition end row,
    /// - or current row is the last row of all input.
    if (current_row != partition_end_row)
    {
        /// when current row is not the partition end row, we need to check whether it's the last
        /// input row.
        if (current_row.row < transform->blockRowsNumber(current_row))
            return false;
        if (partition_end_row.block != current_row.block + 1 || partition_end_row.row)
            return false;
    }
    return true;
}

}

}
