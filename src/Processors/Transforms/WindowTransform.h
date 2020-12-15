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
    std::vector<size_t> partition_by_indices;

    std::vector<WindowFunctionWorkspace> workspaces;

    std::unique_ptr<Arena> arena;
    std::vector<AlignedBuffer> aggregate_function_data;
};

}
