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
    auto num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    expression->execute(block);
    num_rows = block.rows();
    chunk.setColumns(block.getColumns(), num_rows);
}

}
