#pragma once
#include <Processors/ISimpleTransform.h>

#include <Interpreters/AggregateDescription.h>

#include <Common/AlignedBuffer.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class Arena;

// Runtime data for computing one window function
struct WindowFunctionWorkspace
{
    WindowFunctionDescription window_function;
    AlignedBuffer aggregate_function_state;
    std::vector<size_t> argument_column_indices;

    // Argument and result columns. Be careful, they are per-chunk.
    std::vector<const IColumn *> argument_columns;
    MutableColumnPtr result_column;
};

/*
 * Computes several window functions that share the same window. The input must
 * be sorted correctly for this window (PARTITION BY, then ORDER BY).
 */
class WindowTransform : public ISimpleTransform
{
public:
    WindowTransform(
            const Block & input_header_,
            const Block & output_header_,
            const WindowDescription & window_description_,
            const std::vector<WindowFunctionDescription> &
                window_function_descriptions);

    ~WindowTransform() override;

    String getName() const override
    {
        return "WindowTransform";
    }

    static Block transformHeader(Block header, const ExpressionActionsPtr & expression);

    void transform(Chunk & chunk) override;

public:
    Block input_header;

    WindowDescription window_description;

    // Indices of the PARTITION BY columns in block.
    std::vector<size_t> partition_by_indices;

    // The columns for PARTITION BY and the row in these columns where the
    // current partition started. They might be in some of the previous blocks,
    // so we have to keep the shared ownership of the columns. We don't keep the
    // entire block to save memory, only the needed columns, in the same order
    // as the partition_by_indices array.
    // Can be empty if there is no PARTITION BY.
    // Columns are nullptr when it is the first partition.
    std::vector<ColumnPtr> partition_start_columns;
    size_t partition_start_row = 0;

    // Data for computing the window functions.
    std::vector<WindowFunctionWorkspace> workspaces;

    std::unique_ptr<Arena> arena;
};

}
