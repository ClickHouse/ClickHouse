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
    const auto & column = transform->blocks.blockAt(row.block).materialized_columns[workspace.argument_column_indices[argument_index]];
    return column->getFloat64(row.row);
}

void insertResultFloat64(const WindowTransform * transform, size_t function_index, Float64 value)
{
    IColumn & to = *transform->blocks.blockAt(transform->current.location.block).result_columns[function_index];
    assert_cast<ColumnFloat64 &>(to).getData().push_back(value);
}

bool isPartitionFirstRow(const WindowTransform * transform)
{
    return transform->current.row_index_in_partition == 0;
}

bool isPartitionLastRow(const WindowTransform * transform)
{
    if (!transform->partition.bounds().fully_visible)
        return false;

    return transform->blocks.next(transform->current.location) == transform->partition.bounds().end;
}

}

}
