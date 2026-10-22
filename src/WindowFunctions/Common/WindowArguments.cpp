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

ArgumentSegments::Iterator::Iterator(const ArgumentSegments & segments_, size_t block_index_)
    : segments(&segments_)
    , block_index(block_index_)
{
}

ArgumentSegment ArgumentSegments::Iterator::operator*() const
{
    const ArgumentBlock & block = segments->blocks[block_index];
    return ArgumentSegment{
        .columns = segments->columns.subspan(block_index * segments->arguments_count, segments->arguments_count),
        .row_begin = static_cast<size_t>(std::max(segments->begin_row, block.first_row) - block.first_row),
        .row_end = static_cast<size_t>(std::min(segments->end_row, block.end_row) - block.first_row),
    };
}

ArgumentSegments::Iterator & ArgumentSegments::Iterator::operator++()
{
    ++block_index;
    return *this;
}

bool ArgumentSegments::Iterator::operator!=(const Iterator & other) const
{
    return block_index != other.block_index;
}

ArgumentSegments::ArgumentSegments(std::span<const ArgumentBlock> blocks_, std::span<const IColumn *> columns_, size_t arguments_count_, Int64 begin_row_, Int64 end_row_)
    : blocks(blocks_)
    , columns(columns_)
    , arguments_count(arguments_count_)
    , begin_row(begin_row_)
    , end_row(end_row_)
{
}

ArgumentSegments::Iterator ArgumentSegments::begin() const
{
    return Iterator(*this, 0);
}

ArgumentSegments::Iterator ArgumentSegments::end() const
{
    return Iterator(*this, blocks.size());
}

WindowArguments::WindowArguments(std::vector<size_t> argument_column_indices_)
    : argument_column_indices(std::move(argument_column_indices_))
{
}

void WindowArguments::rebuild(std::span<const ArgumentBlock> blocks_)
{
    blocks = blocks_;
    columns.clear();
    for (const ArgumentBlock & block : blocks)
        for (const size_t index : argument_column_indices)
            columns.push_back(block.columns[index].get());
    resolved = 0;
}

size_t WindowArguments::blockOf(Int64 position) const
{
    if (resolved < blocks.size() && blocks[resolved].first_row <= position && position < blocks[resolved].end_row)
        return resolved;

    const auto after = std::upper_bound(blocks.begin(), blocks.end(), position, [](Int64 value, const ArgumentBlock & block) { return value < block.first_row; });
    chassert(after != blocks.begin());
    resolved = after - blocks.begin() - 1;
    chassert(position < blocks[resolved].end_row);
    return resolved;
}

size_t WindowArguments::size() const
{
    return argument_column_indices.size();
}

ArgumentValue WindowArguments::at(size_t argument, Int64 position) const
{
    const size_t block = blockOf(position);
    return ArgumentValue(*columns[block * argument_column_indices.size() + argument], position - blocks[block].first_row);
}

ArgumentSegments WindowArguments::segments(Int64 begin, Int64 end) const
{
    const size_t arguments_count = argument_column_indices.size();
    if (begin >= end)
        return ArgumentSegments({}, {}, arguments_count, begin, end);

    const size_t first_block = blockOf(begin);
    const size_t blocks_count = blockOf(end - 1) + 1 - first_block;
    const std::span<const IColumn *> cache(const_cast<const IColumn **>(columns.data()), columns.size());
    return ArgumentSegments(blocks.subspan(first_block, blocks_count), cache.subspan(first_block * arguments_count, blocks_count * arguments_count), arguments_count, begin, end);
}

}
