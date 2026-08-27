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

static ColumnPtr calculateWatermarkColumn(const ExpressionActionsPtr & actions, Block data)
{
    actions->execute(data, data.rows());
    return data.getByPosition(0).column->convertToFullColumnIfConst();
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

    Field min_value;
    Field max_value;
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
    calculateWatermarkColumn(watermark_expression, std::move(block))->getExtremes(min_value, max_value, 0, num_rows);

    pending_chunks.push(std::move(chunk));
    pending_chunks.push(WatermarkMarker::create(getOutputPort().getHeader(), std::move(max_value)));
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
