#include <WindowFunctions/Common/WindowArguments.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <base/defines.h>

#include <algorithm>

namespace DB
{

namespace
{

size_t blockIndexAt(const DevectorWithMemoryTracking<WindowArguments::Block> & blocks, size_t & last_block, Int64 position)
{
    if (last_block < blocks.size() && blocks[last_block].begin_position <= position && position < blocks[last_block].end_position)
        return last_block;

    const auto * const after = std::upper_bound(blocks.begin(), blocks.end(), position, [](Int64 value, const WindowArguments::Block & block) { return value < block.begin_position; });
    last_block = after - blocks.begin() - 1;
    return last_block;
}

}

ArgumentValue::ArgumentValue(const IColumn & column_, size_t row_)
    : column(&column_)
    , row(row_)
{
}

bool ArgumentValue::isNull() const
{
    return column->isNullAt(row);
}

UInt64 ArgumentValue::getUInt() const
{
    return column->getUInt(row);
}

Int64 ArgumentValue::getInt() const
{
    return column->getInt(row);
}

Float64 ArgumentValue::getFloat64() const
{
    return column->getFloat64(row);
}

Field ArgumentValue::getField() const
{
    return (*column)[row];
}

void ArgumentValue::appendTo(IColumn & result) const
{
    result.insertFrom(*column, row);
}

WindowArguments::WindowArguments(const DevectorWithMemoryTracking<Block> & blocks_, Int64 first_row_position_, Int64 num_arguments_)
    : blocks(blocks_)
    , first_row_position(first_row_position_)
    , num_arguments(num_arguments_)
    , last_used_block(num_arguments_, 0)
{
}

Int64 WindowArguments::size() const
{
    return num_arguments;
}

ArgumentValue WindowArguments::at(size_t argument, Int64 row) const
{
    const Int64 position = first_row_position + row;
    const Block & block = blocks[blockIndexAt(blocks, last_used_block[argument], position)];
    return ArgumentValue(*block.columns[argument], position - block.begin_position);
}

}
