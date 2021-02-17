#include <Processors/Transforms/WindowTransform.h>

#include <Interpreters/ExpressionActions.h>

#include <Common/Arena.h>

namespace DB
{

WindowTransform::WindowTransform(const Block & input_header_,
        const Block & output_header_,
        const WindowDescription & window_description_,
        const std::vector<WindowFunctionDescription> & window_function_descriptions
        )
    : ISimpleTransform(input_header_, output_header_,
        false /* skip_empty_chunks */)
    , input_header(input_header_)
    , window_description(window_description_)
{
    workspaces.reserve(window_function_descriptions.size());
    for (const auto & f : window_function_descriptions)
    {
        WindowFunctionWorkspace workspace;
        workspace.window_function = f;

        const auto & aggregate_function
            = workspace.window_function.aggregate_function;
        if (!arena && aggregate_function->allocatesMemoryInArena())
        {
            arena = std::make_unique<Arena>();
        }

        workspace.argument_column_indices.reserve(
            workspace.window_function.argument_names.size());
        workspace.argument_columns.reserve(
            workspace.window_function.argument_names.size());
        for (const auto & argument_name : workspace.window_function.argument_names)
        {
            workspace.argument_column_indices.push_back(
                input_header.getPositionByName(argument_name));
        }

        workspace.aggregate_function_state.reset(aggregate_function->sizeOfData(),
            aggregate_function->alignOfData());
        aggregate_function->create(workspace.aggregate_function_state.data());

        workspaces.push_back(std::move(workspace));
    }

    partition_by_indices.reserve(window_description.partition_by.size());
    for (const auto & column : window_description.partition_by)
    {
        partition_by_indices.push_back(
            input_header.getPositionByName(column.column_name));
    }
    partition_start_columns.resize(partition_by_indices.size(), nullptr);
    partition_start_row = 0;
}

WindowTransform::~WindowTransform()
{
    // Some states may be not created yet if the creation failed.
    for (auto & ws : workspaces)
    {
        ws.window_function.aggregate_function->destroy(
            ws.aggregate_function_state.data());
    }
}

void WindowTransform::transform(Chunk & chunk)
{
    const size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (auto & ws : workspaces)
    {
        ws.argument_columns.clear();
        for (const auto column_index : ws.argument_column_indices)
        {
            // Aggregate functions can't work with constant columns, so we have to
            // materialize them like the Aggregator does.
            columns[column_index]
                = std::move(columns[column_index])->convertToFullColumnIfConst();

            ws.argument_columns.push_back(columns[column_index].get());
        }

        ws.result_column = ws.window_function.aggregate_function->getReturnType()
            ->createColumn();
    }

    // We loop for all window functions for each row. Switching the loops might
    // be more efficient, because we would run less code and access less data in
    // the inner loop. If you change this, don't forget to fix the calculation of
    // partition boundaries. Probably it has to be precalculated and stored as
    // an array of offsets. An interesting optimization would be to pass it as
    // an extra column from the previous sorting step -- that step might need to
    // make similar comparison anyway, if it's sorting only by the PARTITION BY
    // columns.
    for (size_t row = 0; row < num_rows; row++)
    {
        // Check whether the new partition has started. We have to reset the
        // aggregate functions when the new partition starts.
        assert(partition_start_columns.size() == partition_by_indices.size());
        bool new_partition = false;
        if (partition_start_columns.empty())
        {
            // No PARTITION BY at all, do nothing.
        }
        else if (partition_start_columns[0] == nullptr)
        {
            // This is the first partition.
            new_partition = true;
            partition_start_columns.clear();
            for (const auto i : partition_by_indices)
            {
                partition_start_columns.push_back(columns[i]);
            }
            partition_start_row = row;
        }
        else
        {
            // Check whether the new partition started, by comparing all the
            // PARTITION BY columns.
            size_t first_inequal_column = 0;
            for (; first_inequal_column < partition_start_columns.size();
                  ++first_inequal_column)
            {
                const auto * current_column = columns[
                    partition_by_indices[first_inequal_column]].get();

                if (current_column->compareAt(row, partition_start_row,
                    *partition_start_columns[first_inequal_column],
                    1 /* nan_direction_hint */) != 0)
                {
                    break;
                }
            }

            if (first_inequal_column < partition_start_columns.size())
            {
                // The new partition has started. Remember where.
                new_partition = true;
                partition_start_columns.clear();
                for (const auto i : partition_by_indices)
                {
                    partition_start_columns.push_back(columns[i]);
                }
                partition_start_row = row;
            }
        }

        for (auto & ws : workspaces)
        {
            const auto & f = ws.window_function;
            const auto * a = f.aggregate_function.get();
            auto * buf = ws.aggregate_function_state.data();

            if (new_partition)
            {
                // Reset the aggregate function states.
                a->destroy(buf);
                a->create(buf);
            }

            // Update the aggregate function state and save the result.
            a->add(buf,
                ws.argument_columns.data(),
                row,
                arena.get());

            a->insertResultInto(buf,
                *ws.result_column,
                arena.get());
        }
    }

    // We have to release the mutable reference to the result column before we
    // return this block, or else extra copying may occur when the subsequent
    // processors modify the block. Workspaces live longer than individual blocks.
    for (auto & ws : workspaces)
    {
        columns.push_back(std::move(ws.result_column));
    }

    chunk.setColumns(std::move(columns), num_rows);
}

}
