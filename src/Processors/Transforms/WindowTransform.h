#pragma once
#include <Processors/ISimpleTransform.h>

#include <Interpreters/AggregateDescription.h>

#include <Common/AlignedBuffer.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class Arena;

struct WindowFunctionWorkspace
{
    WindowFunctionDescription window_function;
    AlignedBuffer aggregate_function_state;
    std::vector<size_t> argument_column_indices;
    // Be careful, this is per-chunk.
    std::vector<const IColumn *> argument_columns;
};

/** Executes a certain expression over the block.
  * The expression consists of column identifiers from the block, constants, common functions.
  * For example: hits * 2 + 3, url LIKE '%yandex%'
  * The expression processes each row independently of the others.
  */
class WindowTransform : public ISimpleTransform
{
public:
    WindowTransform(
            const Block & input_header,
            const Block & output_header,
            const WindowDescription & window_description,
            const std::vector<WindowFunctionDescription> & window_functions);

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

    std::vector<WindowFunctionWorkspace> workspaces;

    std::unique_ptr<Arena> arena;
};

}
