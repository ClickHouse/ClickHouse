#include <Processors/Streaming/WatermarkCalculatorTransform.h>
#include <Processors/Streaming/MarkerWatermark.h>
#include <Processors/Port.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/IColumn.h>

#include <DataTypes/IDataType.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>

#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>

#include <Core/Block.h>
#include <Core/Field.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

ColumnPtr calculateWatermarkColumn(const ExpressionActionsPtr & actions, Block data)
{
    actions->execute(data, data.rows());
    return data.getByPosition(0).column->convertToFullColumnIfConst();
}

template <typename ColumnT>
std::pair<Columns, std::optional<Field>> executeWatermarkFilter(
    Columns source_columns,
    std::optional<Field> initial_watermark,
    const ColumnT & event_time_col,
    const ColumnT & watermark_col)
{
    using ValueType = typename ColumnT::ValueType;

    const auto & event_time_data = event_time_col.getData();
    const auto & watermark_data = watermark_col.getData();
    const size_t num_rows = watermark_data.size();

    std::optional<ValueType> active_watermark = std::nullopt;
    if (initial_watermark)
    {
        if constexpr (is_decimal<ValueType>)
            active_watermark = initial_watermark->template safeGet<NearestFieldType<ValueType>>().getValue();
        else
            active_watermark = static_cast<ValueType>(initial_watermark->template safeGet<NearestFieldType<ValueType>>());
    }

    IColumn::Filter mask(num_rows, 1);
    size_t num_output_rows = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        const auto candidate = watermark_data[i];
        if (!active_watermark || candidate > active_watermark.value())
            active_watermark = candidate;

        if (event_time_data[i] < active_watermark.value())
            mask[i] = 0;
        else
            num_output_rows += 1;
    }

    Columns output_columns;
    output_columns.reserve(source_columns.size() + 2);
    for (const auto & source : source_columns)
        output_columns.emplace_back(source->filter(mask, num_output_rows));

    output_columns.emplace_back(event_time_col.filter(mask, num_output_rows));
    output_columns.emplace_back(watermark_col.filter(mask, num_output_rows));

    if (active_watermark)
    {
        if constexpr (is_decimal<ValueType>)
            return {std::move(output_columns), Field(NearestFieldType<ValueType>(active_watermark.value(), watermark_col.getScale()))};
        else
            return {std::move(output_columns), Field(active_watermark.value())};
    }

    return {std::move(output_columns), std::nullopt};
}

std::pair<Chunk, std::optional<Field>> calculateWatermark(
    Chunk chunk,
    std::optional<Field> watermark,
    const DataTypePtr & watermark_type,
    const ColumnPtr & event_time_col,
    const ColumnPtr & watermark_col)
{
    auto columns = chunk.detachColumns();
    auto infos = std::move(chunk.getChunkInfos());

    const WhichDataType which(watermark_type);
    if (which.isDate())
        std::tie(columns, watermark) = executeWatermarkFilter<ColumnDate>(
            std::move(columns), watermark, assert_cast<const ColumnDate &>(*event_time_col), assert_cast<const ColumnDate &>(*watermark_col));
    else if (which.isDate32())
        std::tie(columns, watermark) = executeWatermarkFilter<ColumnDate32>(
            std::move(columns), watermark, assert_cast<const ColumnDate32 &>(*event_time_col), assert_cast<const ColumnDate32 &>(*watermark_col));
    else if (which.isDateTime())
        std::tie(columns, watermark) = executeWatermarkFilter<ColumnDateTime>(
            std::move(columns), watermark, assert_cast<const ColumnDateTime &>(*event_time_col), assert_cast<const ColumnDateTime &>(*watermark_col));
    else if (which.isDateTime64())
        std::tie(columns, watermark) = executeWatermarkFilter<ColumnDateTime64>(
            std::move(columns), watermark, assert_cast<const ColumnDateTime64 &>(*event_time_col), assert_cast<const ColumnDateTime64 &>(*watermark_col));
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected watermark column type: {}", watermark_type->getName());

    size_t num_rows = columns.front()->size();
    Chunk processed_chunk(std::move(columns), num_rows);
    processed_chunk.setChunkInfos(std::move(infos));

    return {std::move(processed_chunk), std::move(watermark)};
}

}

WatermarkCalculatorTransform::WatermarkCalculatorTransform(
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

void WatermarkCalculatorTransform::consume(Chunk chunk)
{
    const auto & output_header = getOutputPort().getHeader();
    const auto & input_header = getInputPort().getHeader();

    if (chunk.getNumRows() == 0)
    {
        Chunk reshaped(output_header.cloneEmptyColumns(), 0);
        reshaped.setChunkInfos(std::move(chunk.getChunkInfos()));
        pending_chunks.push(std::move(reshaped));
        return;
    }

    auto block = input_header.cloneWithColumns(chunk.getColumns());
    const auto & event_time_column_with_type = block.getByName(event_time_column);
    const auto event_time_col = event_time_column_with_type.column->convertToFullColumnIfConst();
    const auto watermark_type = event_time_column_with_type.type;
    const auto watermark_col = calculateWatermarkColumn(watermark_expression, std::move(block));

    std::tie(chunk, emitted_watermark) = calculateWatermark(std::move(chunk), emitted_watermark, watermark_type, event_time_col, watermark_col);
    pending_chunks.push(std::move(chunk));

    if (emitted_watermark)
        pending_chunks.push(makeWatermarkMarkerChunk(output_header, emitted_watermark.value()));
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
