#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <Core/Block.h>
#include <Functions/IFunction.h>
#include <Processors/ChunkSortDescription.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
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
    , updater(std::move(updater_))
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows, false, false, [this]() { return isCancelled(); });

    chunk.setColumns(block.getColumns(), num_rows);

    /// Update ChunkSortDescription column names to reflect any renames
    /// performed by this expression (e.g. "x" → "__table1.x").
    if (auto sort_info = chunk.getChunkInfos().extract<ChunkSortDescription>())
    {
        auto updated = sort_info->sort_description;
        applyActionsToSortDescription(updated, expression->getActionsDAG());
        if (!updated.empty())
            chunk.getChunkInfos().add(std::make_shared<ChunkSortDescription>(std::move(updated)));
    }

    if (updater)
        updater->recordOutputChunk(chunk, block);
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
