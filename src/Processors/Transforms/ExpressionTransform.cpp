#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
namespace DB
{

Block ExpressionTransform::transformHeader(Block header, const ActionsDAG & expression)
{
    return expression.updateHeader(std::move(header));
}


ExpressionTransform::ExpressionTransform(const Block & header_, ExpressionActionsPtr expression_)
    : ISimpleTransform(header_, transformHeader(header_, expression_->getActionsDAG()), false)
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

ProcessorPtr ExpressionTransform::getPartialResultProcessor(const ProcessorPtr & /*current_processor*/, UInt64 /*partial_result_limit*/, UInt64 /*partial_result_duration_ms*/)
{
    const auto & header = getInputPort().getHeader();
    auto result = std::make_shared<ExpressionTransform>(header, expression);
    result->setDescription("(Partial result)");
    return result;
}

ConvertingTransform::ConvertingTransform(const Block & header_, ExpressionActionsPtr expression_)
    : ExceptionKeepingTransform(header_, ExpressionTransform::transformHeader(header_, expression_->getActionsDAG()))
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
