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
    SharedHeader input_header_,
    SharedHeader output_header_,
    std::string event_time_column_,
    ActionsDAG watermark_expression_,
    ContextPtr context_)
    : IInflatingTransform(std::move(input_header_), std::move(output_header_))
    , event_time_column(std::move(event_time_column_))
    , watermark_expression(std::make_shared<ExpressionActions>(std::move(watermark_expression_), ExpressionActionsSettings(context_)))
{
}

void CalculateWatermarksTransform::consume(Chunk chunk)
{
    const auto & input_header = getInputPort().getHeader();
    const size_t num_rows = chunk.getNumRows();

    auto block = input_header.cloneWithColumns(chunk.getColumns());
    const auto event_time_col = block.getByName(event_time_column).column->convertToFullColumnIfConst();
    const auto watermark_col = calculateWatermarkColumn(watermark_expression, std::move(block));

    auto columns = chunk.detachColumns();
    columns.emplace_back(event_time_col);
    columns.emplace_back(watermark_col);
    chunk.setColumns(std::move(columns), num_rows);
    pending_chunks.push(std::move(chunk));

    if (num_rows == 0)
        return;

    Field min_value;
    Field max_value;
    watermark_col->getExtremes(min_value, max_value, 0, num_rows);

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
