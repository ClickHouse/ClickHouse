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
    for (size_t i = 0; i < window_function_descriptions.size(); ++i)
    {
        WindowFunctionWorkspace workspace;
        workspace.window_function = window_function_descriptions[i];

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
    for (size_t i = 0; i < workspaces.size(); i++)
    {
        workspaces[i].window_function.aggregate_function->destroy(
            workspaces[i].aggregate_function_state.data());
    }
}

void WindowTransform::transform(Chunk & chunk)
{
    const size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    for (auto & workspace : workspaces)
    {
        workspace.argument_columns.clear();
        for (const auto column_index : workspace.argument_column_indices)
        {
            workspace.argument_columns.push_back(
                block.getColumns()[column_index].get());
        }
    }

    for (auto & ws : workspaces)
    {
        const auto & f = ws.window_function;
        const auto * a = f.aggregate_function.get();

        // Create the resulting column.
        ColumnWithTypeAndName column_with_type;
        column_with_type.name = f.column_name;
        column_with_type.type = a->getReturnType();
        auto c = column_with_type.type->createColumn();
        column_with_type.column.reset(c.get());

        for (size_t row = 0; row < num_rows; row++)
        {
            // THIS IS BROKEN when we have multiple window functions.
            // Have to switch the loops or track partition per-function.
            //
            // Check whether the new partition has started and reinitialize the
            // aggregate function states.
            assert(partition_start_columns.size() == partition_by_indices.size());
            if (partition_start_columns.size() == 0)
            {
                // No PARTITION BY at all, do nothing.
            }
            else if (partition_start_columns[0] == nullptr)
            {
                // This is the first partition.
                partition_start_columns.clear();
                for (const auto i : partition_by_indices)
                {
                    partition_start_columns.push_back(block.getColumns()[i]);
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
                    const auto * current_column = block.getColumns()[
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
                    // The new partition has started.
                    // 1) Remember it.
                    partition_start_columns.clear();
                    for (const auto i : partition_by_indices)
                    {
                        partition_start_columns.push_back(block.getColumns()[i]);
                    }
                    partition_start_row = row;

                    // 2) Reset the aggregate function states.
                    ws.window_function.aggregate_function->destroy(
                        ws.aggregate_function_state.data());
                    ws.window_function.aggregate_function->create(
                        ws.aggregate_function_state.data());
                }
            }

            // Update the aggregate function state and save the result.
            a->add(ws.aggregate_function_state.data(),
                ws.argument_columns.data(),
                row,
                arena.get());

            a->insertResultInto(ws.aggregate_function_state.data(),
                *c,
                arena.get());
        }

        block.insert(column_with_type);
    }

    chunk.setColumns(block.getColumns(), num_rows);
}

}
