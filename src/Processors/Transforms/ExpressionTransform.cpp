#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <Columns/IColumn.h>
#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Core/Defines.h>
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
    , updater(std::move(updater_))
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    LOG_TEST(getLogger("ExpressionTransform"), "Make expression transform");

    size_t total_rows = chunk.getNumRows();
    constexpr size_t batch_size = DEFAULT_BLOCK_SIZE;

    if (total_rows == 0)
        return;

    if (total_rows <= batch_size)
    {
        auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
        expression->execute(block, total_rows, false, false, [this]() { return isCancelled(); });
        chunk.setColumns(block.getColumns(), total_rows);

        if (updater)
            updater->recordOutputChunk(chunk, getOutputPort().getHeader());
        return;
    }

    const auto & header = getInputPort().getHeader();
    MutableColumns output_columns;

    for (size_t i = 0; i < header.columns(); ++i)
        output_columns.push_back(header.getByPosition(i).type->createColumn());

    Columns input_columns = chunk.getColumns();
    size_t processed = 0;
    while (processed < total_rows && !isCancelled())
    {
        size_t current_batch_size = std::min(batch_size, total_rows - processed);

        MutableColumns batch_columns;
        for (const auto & col : input_columns)
            batch_columns.push_back(IColumn::mutate(col->cut(processed, current_batch_size)));

        auto batch_block = header.cloneWithColumns(std::move(batch_columns));
        size_t batch_rows = current_batch_size;
        expression->execute(batch_block, batch_rows, false, false, [this]() { return isCancelled(); });

        batch_rows = batch_block.rows();

        for (size_t i = 0; i < batch_block.columns(); ++i)
            output_columns[i]->insertRangeFrom(*batch_block.getByPosition(i).column, 0, batch_rows);

        processed += batch_rows;
    }

    chunk.setColumns(std::move(output_columns), processed);

    if (updater)
        updater->recordOutputChunk(chunk, getOutputPort().getHeader());
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
    /// TODO: batching could be implemented in similar way as in ExpressionTransform

    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows, false, false, [this]() { return isCancelled(); });

    chunk.setColumns(block.getColumns(), num_rows);
    cur_chunk = std::move(chunk);
}

}
