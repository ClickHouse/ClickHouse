#include <Processors/Transforms/Window/SlidingArguments.h>

#include <Columns/IColumn.h>

#include <Core/Block.h>

#include <ranges>

namespace DB
{

SlidingArguments::SlidingArguments(const Block & input_header, const VectorWithMemoryTracking<WindowFunctionDescription> & functions)
    : argument_positions(functions.size())
    , function_blocks(functions.size())
{
    for (auto && [function, positions] : std::views::zip(functions, argument_positions))
        for (const String & argument_name : function.argument_names)
            positions.push_back(input_header.getPositionByName(argument_name));

    for (const auto & positions : argument_positions)
        views.emplace_back(positions.size());
}

void SlidingArguments::add(const Columns & materialized_columns, Int64 rows_count)
{
    for (auto && [positions, blocks] : std::views::zip(argument_positions, function_blocks))
        blocks.push_back(WindowArguments::Block{
            .columns = positions | std::views::transform([&](size_t position) { return materialized_columns[position].get(); }) | std::ranges::to<ColumnRawPtrs>(),
            .begin_position = end_position,
            .end_position = end_position + rows_count,
        });

    end_position += rows_count;
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

const WindowArguments & SlidingArguments::view(size_t function)
{
    WindowArguments & view = views[function];
    view.setBlocks({function_blocks[function].data(), function_blocks[function].size()}, partition_begin_position);
    return view;
}

}
