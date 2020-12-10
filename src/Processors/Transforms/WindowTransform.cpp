#include <Processors/Transforms/WindowTransform.h>

#include <Interpreters/ExpressionActions.h>

namespace DB
{

Block WindowTransform::transformHeader(Block header, const ExpressionActionsPtr & expression)
{
    size_t num_rows = header.rows();
    expression->execute(header, num_rows, true);
    return header;
}


WindowTransform::WindowTransform(const Block & header_,
        ExpressionActionsPtr expression_)
    : ISimpleTransform(header_, transformHeader(header_, expression_), false)
    , expression(std::move(expression_))
{
}

void WindowTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);
}

}
