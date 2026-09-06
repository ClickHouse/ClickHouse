#include <Processors/Transforms/ExpressionTransform.h>
#include <Common/FailPoint.h>
#include <Interpreters/ExpressionActions.h>
#include <Core/Block.h>
#include <Functions/IFunction.h>
#include <memory>

#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>


namespace DB
{

namespace FailPoints
{
    extern const char expression_transform_before_expression_pause[];
    extern const char expression_transform_pause[];
    extern const char converting_transform_before_expression_pause[];
    extern const char converting_transform_pause[];
}

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

ExpressionTransform::ExpressionTransform(
    SharedHeader input_header_,
    SharedHeader transformed_header_,
    ExpressionActionsPtr expression_,
    RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : ISimpleTransform(input_header_, std::move(transformed_header_), false)
    , expression(std::move(expression_))
    , input_positions(expression->getInputPositions(*input_header_))
    , updater(std::move(updater_))
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();

    FailPointInjection::pauseFailPoint(FailPoints::expression_transform_before_expression_pause);

    if (isCancelled())
    {
        chunk.setColumns(getOutputPort().getHeader().cloneEmptyColumns(), 0);
        return;
    }

    /// The statistics updater needs the full output Block, so fall back to the block-based path when it is set.
    if (updater)
    {
        auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
        expression->execute(block, num_rows, false, false, &getCancellationFlag());
        FailPointInjection::pauseFailPoint(FailPoints::expression_transform_pause);
        if (isCancelled())
        {
            block = getOutputPort().getHeader().cloneWithColumns(getOutputPort().getHeader().cloneEmptyColumns());
            num_rows = 0;
        }
        chunk.setColumns(block.getColumns(), num_rows);
        updater->recordOutputChunk(chunk, block);
        return;
    }

    /// Fast path: run positionally against the fixed input header, avoiding per-chunk Block name-index work.
    auto columns = expression->executeOnColumns(
        chunk.detachColumns(), getInputPort().getHeader(), input_positions, num_rows, false, &getCancellationFlag());

    FailPointInjection::pauseFailPoint(FailPoints::expression_transform_pause);

    if (isCancelled())
    {
        columns = getOutputPort().getHeader().cloneWithColumns(getOutputPort().getHeader().cloneEmptyColumns()).getColumns();
        num_rows = 0;
    }
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

void ConvertingTransform::onCancel() noexcept
{
    ExceptionKeepingTransform::onCancel();
    const auto & nodes = expression->getNodes();
    for (const auto & node : nodes)
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION && node.function)
            node.function->cancelExecution();
    }
}

void ConvertingTransform::onConsume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();

    FailPointInjection::pauseFailPoint(FailPoints::converting_transform_before_expression_pause);

    if (isCancelled())
    {
        chunk.setColumns(getOutputPort().getHeader().cloneEmptyColumns(), 0);
        cur_chunk = std::move(chunk);
        return;
    }

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows, false, false, &getCancellationFlag());

    FailPointInjection::pauseFailPoint(FailPoints::converting_transform_pause);

    if (isCancelled())
    {
        block = getOutputPort().getHeader().cloneWithColumns(getOutputPort().getHeader().cloneEmptyColumns());
        num_rows = 0;
    }
    chunk.setColumns(block.getColumns(), num_rows);
    cur_chunk = std::move(chunk);
}

}
