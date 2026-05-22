#include <Processors/Streaming/WatermarkCalculatorTransform.h>
#include <Processors/Streaming/MarkerWatermark.h>
#include <Processors/Port.h>

#include <Columns/IColumn.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>

#include <Core/Block.h>

namespace DB
{

namespace
{

ColumnPtr calculateWatermarkColumn(const ExpressionActionsPtr & actions, Block data)
{
    actions->execute(data, data.rows());
    return data.getByPosition(0).column->convertToFullColumnIfConst();
}

/// TODO: Make type dispatch here.
std::pair<Chunk, std::optional<Field>> executeWatermarkFilter(
    Columns columns,
    Chunk::ChunkInfoCollection chunk_infos,
    std::optional<Field> emitted_watermark,
    const ColumnPtr & event_time_col,
    const ColumnPtr & watermark_col)
{
    const size_t num_rows = watermark_col->size();
    IColumn::Filter mask(num_rows, 1);
    for (size_t i = 0; i < num_rows; ++i)
    {
        Field c;
        watermark_col->get(i, c);
        if (!emitted_watermark.has_value() || c < emitted_watermark.value())
            emitted_watermark = c;

        Field e;
        event_time_col->get(i, e);
        if (e < emitted_watermark.value())
            mask[i] = 0;
    }

    Columns output_columns;
    output_columns.reserve(columns.size());
    for (const auto & col : columns)
        output_columns.emplace_back(col->filter(mask, 0));

    const size_t num_output_rows = output_columns.empty() ? 0 : output_columns.front()->size();
    Chunk data_chunk(std::move(output_columns), num_output_rows);
    data_chunk.setChunkInfos(std::move(chunk_infos));
    return std::make_pair(std::move(data_chunk), std::move(emitted_watermark));
}

}

WatermarkCalculatorTransform::WatermarkCalculatorTransform(SharedHeader header_, std::string event_time_column_, ActionsDAG watermark_expression_, ContextPtr context_)
    : IInflatingTransform(header_, header_)
    , event_time_column(std::move(event_time_column_))
    , watermark_expression(std::make_shared<ExpressionActions>(std::move(watermark_expression_), ExpressionActionsSettings(context_)))
{
}

void WatermarkCalculatorTransform::consume(Chunk chunk)
{
    /// Empty chunks - pass through.
    if (chunk.getNumRows() == 0)
    {
        pending_chunks.push(std::move(chunk));
        return;
    }

    const auto & input_header = getInputPort().getHeader();
    auto chunk_infos = chunk.getChunkInfos();
    auto columns = chunk.detachColumns();

    auto block = input_header.cloneWithColumns(columns);
    const auto event_time_col = block.getByName(event_time_column).column->convertToFullColumnIfConst();
    const auto watermark_col = calculateWatermarkColumn(watermark_expression, std::move(block));

    auto [new_chunk, new_watermark] = executeWatermarkFilter(std::move(columns), std::move(chunk_infos), emitted_watermark, event_time_col, watermark_col);
    pending_chunks.push(std::move(new_chunk));

    if (new_watermark.has_value())
        pending_chunks.push(makeWatermarkMarkerChunk(input_header, new_watermark.value()));

    emitted_watermark = std::move(new_watermark);
}

bool WatermarkCalculatorTransform::canGenerate()
{
    return !pending_chunks.empty();
}

Chunk WatermarkCalculatorTransform::generate()
{
    chassert(!pending_chunks.empty());
    auto chunk = std::move(pending_chunks.front());
    pending_chunks.pop();
    return chunk;
}

}
