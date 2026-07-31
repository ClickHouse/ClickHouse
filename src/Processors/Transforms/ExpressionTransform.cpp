#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <Core/Block.h>
#include <Functions/IFunction.h>
#include <memory>

#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>


namespace DB
{

Block ExpressionTransform::transformHeader(const Block & header, const ActionsDAG & expression)
{
    return expression.updateHeader(header);
}

ExpressionTransform::ExpressionTransform(
    SharedHeader header_, ExpressionActionsPtr expression_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : ISimpleTransform(header_, std::make_shared<const Block>(transformHeader(*header_, expression_->getActionsDAG())), false)
    , expression(std::move(expression_))
    , input_positions(expression->getInputPositions(*header_))
    , updater(std::move(updater_))
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();

    /// The statistics updater needs the full output Block, so fall back to the block-based path when it is set.
    if (updater)
    {
        auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
        expression->execute(block, num_rows, false, false, [this]() { return isCancelled(); });
        chunk.setColumns(block.getColumns(), num_rows);
        updater->recordOutputChunk(chunk, block);
        return;
    }

    /// Fast path: run positionally against the fixed input header, avoiding per-chunk Block name-index work.
    auto columns = expression->executeOnColumns(
        chunk.detachColumns(), getInputPort().getHeader(), input_positions, num_rows, false, [this]() { return isCancelled(); });

    chunk.setColumns(std::move(columns), num_rows);
}

void ExpressionTransform::onCancel() noexcept
{
    ISimpleTransform::onCancel();
    const auto & nodes = expression->getNodes();
    for (const auto & node : nodes)
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION && node.function)
            node.function->cancelExecution();
    }
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

    expression->execute(block, num_rows, false, false, [this]() { return isCancelled(); });

    chunk.setColumns(block.getColumns(), num_rows);
    cur_chunk = std::move(chunk);
}

}
