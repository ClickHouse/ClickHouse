#include <WindowFunctions/Common/WindowArguments.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <base/defines.h>

#include <algorithm>

namespace DB
{

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

WindowArguments::WindowArguments(std::vector<size_t> column_indices_)
    : column_indices(std::move(column_indices_))
    , last_used_block_for_arg(column_indices.size(), 0)
{
}

void WindowArguments::setBlocks(const std::vector<Block> & blocks_)
{
    blocks = blocks_;
    std::ranges::fill(last_used_block_for_arg, 0);
}

size_t WindowArguments::findBlock(size_t argument, Int64 position) const
{
    size_t & last_block = last_used_block_for_arg[argument];
    if (last_block < blocks.size() && blocks[last_block].first_position <= position && position < blocks[last_block].end_position)
        return last_block;

    const auto after = std::upper_bound(blocks.begin(), blocks.end(), position, [](Int64 value, const Block & block) { return value < block.first_position; });
    last_block = after - blocks.begin() - 1;
    return last_block;
}

size_t WindowArguments::size() const
{
    return column_indices.size();
}

ArgumentValue WindowArguments::at(size_t argument, Int64 position) const
{
    const Block & block = blocks[findBlock(argument, position)];
    return ArgumentValue(*block.columns[column_indices[argument]], position - block.first_position);
}

}
