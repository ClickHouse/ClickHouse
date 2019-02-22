#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB
{

static Block transformHeader(Block header, const ExpressionActionsPtr & expression)
{
    expression->execute(header, true);
    return header;
}


ExpressionTransform::ExpressionTransform(const Block & header, ExpressionActionsPtr expression)
    : ISimpleTransform(header, transformHeader(header, expression), false)
    , expression(std::move(expression))
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    auto num_rows = chunk.getNumRows();
    expression->execute(block);
    chunk.setColumns(block.getColumns(), num_rows);
}

}
