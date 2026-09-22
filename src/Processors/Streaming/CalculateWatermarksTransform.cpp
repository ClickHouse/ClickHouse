#include <Processors/Streaming/CalculateWatermarksTransform.h>
#include <Processors/Streaming/Markers.h>
#include <Processors/Port.h>

#include <Columns/IColumn.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>

#include <Core/Block.h>
#include <Core/Field.h>

#include <base/defines.h>

namespace DB
{

static Field calculateWatermark(const ExpressionActionsPtr & actions, Block data)
{
    actions->execute(data, data.rows());
    const auto watermark_column = data.getByPosition(0).column;

    Field min_value;
    Field max_value;
    watermark_column->getExtremes(min_value, max_value, 0, data.rows());

    return max_value;
}

CalculateWatermarksTransform::CalculateWatermarksTransform(
    SharedHeader header_,
    ActionsDAG watermark_expression_,
    ContextPtr context_)
    : IInflatingTransform(header_, header_)
    , watermark_expression(std::make_shared<ExpressionActions>(std::move(watermark_expression_), ExpressionActionsSettings(context_)))
{
}

void CalculateWatermarksTransform::consume(Chunk chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
    {
        pending_chunks.push(std::move(chunk));
        return;
    }

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
    Field watermark = calculateWatermark(watermark_expression, std::move(block));

    pending_chunks.push(std::move(chunk));
    pending_chunks.push(WatermarkMarker::create(getOutputPort().getHeader(), std::move(watermark)));
}

bool CalculateWatermarksTransform::canGenerate()
{
    return !pending_chunks.empty();
}

Chunk CalculateWatermarksTransform::generate()
{
    chassert(!pending_chunks.empty());
    auto chunk = std::move(pending_chunks.front());
    pending_chunks.pop();
    return chunk;
}

}
