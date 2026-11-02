#include <Processors/Transforms/Window/SlidingArguments.h>

#include <Columns/IColumn.h>

#include <Core/Block.h>

#include <ranges>

namespace DB
{

SlidingArguments::SlidingArguments(const Block & input_header, const std::vector<WindowFunctionDescription> & functions)
    : argument_positions(functions.size())
    , function_blocks(functions.size())
{
    for (auto && [function, positions] : std::views::zip(functions, argument_positions))
        for (const String & argument_name : function.argument_names)
            positions.push_back(input_header.getPositionByName(argument_name));
}

void SlidingArguments::add(const Columns & materialized_columns, Int64 rows_count)
{
    for (auto && [positions, blocks] : std::views::zip(argument_positions, function_blocks))
        blocks.push_back(WindowArguments::Block{
            .columns = positions | std::views::transform([&](size_t position) { return materialized_columns[position].get(); }) | std::ranges::to<ColumnRawPtrs>(),
            .begin_position = global_end_position,
            .end_position = global_end_position + rows_count,
        });

    global_end_position += rows_count;
}

void SlidingArguments::pop()
{
    for (auto & blocks : function_blocks)
        blocks.pop_front();
}

void SlidingArguments::startPartition(Int64 closed_partition_length)
{
    partition_begin_position += closed_partition_length;
}

WindowArguments SlidingArguments::view(size_t function) const
{
    return WindowArguments(function_blocks[function], partition_begin_position, argument_positions[function].size());
}

}
