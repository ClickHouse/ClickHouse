#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <Core/Block.h>
#include <memory>


namespace DB
{

Block ExpressionTransform::transformHeader(const Block & header, const ActionsDAG & expression)
{
    return expression.updateHeader(header);
}

ExpressionTransform::ExpressionTransform(SharedHeader header_, ExpressionActionsPtr expression_)
    : ISimpleTransform(header_, std::make_shared<const Block>(transformHeader(*header_, expression_->getActionsDAG())), false)
    , expression(std::move(expression_))
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);
}

ConvertingTransform::ConvertingTransform(SharedHeader header_, ExpressionActionsPtr expression_)
    : ExceptionKeepingTransform(header_, std::make_shared<const Block>(ExpressionTransform::transformHeader(*header_, expression_->getActionsDAG())))
    , expression(std::move(expression_))
{
}

void ConvertingTransform::onConsume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);
    cur_chunk = std::move(chunk);
}

}
