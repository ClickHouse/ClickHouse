#include <Processors/Streaming/CalculateWatermarksTransform.h>
#include <Processors/Streaming/Markers.h>
#include <Processors/Port.h>

#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>

#include <Core/Block.h>
#include <Core/Field.h>

#include <DataTypes/IDataType.h>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <base/defines.h>

#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

ColumnWithTypeAndName calculateWatermarkColumn(const ExpressionActionsPtr & actions, Block data)
{
    actions->execute(data, data.rows());

    auto result = data.getByPosition(0);
    result.column = result.column->convertToFullColumnIfConst();

    return result;
}

template <typename ColumnType>
ColumnPtr calculatePrefixMaxColumnTyped(const IColumn & column, const Field & previous)
{
    using ValueType = typename ColumnType::ValueType;

    std::optional<ValueType> running;
    if (!previous.isNull())
    {
        if constexpr (is_decimal<ValueType>)
            running = previous.safeGet<DecimalField<ValueType>>().getValue();
        else
            running = static_cast<ValueType>(previous.safeGet<NearestFieldType<ValueType>>());
    }

    const auto & data = assert_cast<const ColumnType &>(column).getData();
    auto result_column = column.cloneEmpty();
    auto & result = assert_cast<ColumnType &>(*result_column).getData();
    result.resize(data.size());

    for (size_t i = 0; i < data.size(); ++i)
    {
        if (!running.has_value() || *running < data[i])
            running = data[i];

        result[i] = *running;
    }

    return result_column;
}

ColumnPtr calculatePrefixMaxColumn(TypeIndex type_index, const ColumnPtr & column, const Field & previous)
{
    switch (type_index)
    {
        case TypeIndex::Date:
            return calculatePrefixMaxColumnTyped<ColumnUInt16>(*column, previous);
        case TypeIndex::Date32:
            return calculatePrefixMaxColumnTyped<ColumnInt32>(*column, previous);
        case TypeIndex::DateTime:
            return calculatePrefixMaxColumnTyped<ColumnUInt32>(*column, previous);
        case TypeIndex::DateTime64:
            return calculatePrefixMaxColumnTyped<ColumnDecimal<DateTime64>>(*column, previous);
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected watermark column type {}", column->getName());
    }
}

}

CalculateWatermarksTransform::CalculateWatermarksTransform(
    SharedHeader input_header_,
    SharedHeader output_header_,
    std::string event_time_column_,
    ActionsDAG watermark_expression_,
    Field initial_watermark_,
    ContextPtr context_)
    : IInflatingTransform(std::move(input_header_), std::move(output_header_))
    , event_time_column(std::move(event_time_column_))
    , watermark_expression(std::make_shared<ExpressionActions>(std::move(watermark_expression_), ExpressionActionsSettings(context_)))
    , watermark(std::move(initial_watermark_))
{
}

void CalculateWatermarksTransform::consume(Chunk chunk)
{
    const auto & input_header = getInputPort().getHeader();
    const size_t num_rows = chunk.getNumRows();

    auto block = input_header.cloneWithColumns(chunk.getColumns());
    const auto event_time_col = block.getByName(event_time_column).column->convertToFullColumnIfConst();
    const auto expression_result = calculateWatermarkColumn(watermark_expression, std::move(block));
    const auto watermark_col = calculatePrefixMaxColumn(expression_result.type->getTypeId(), expression_result.column, watermark);

    if (num_rows > 0)
        watermark_col->get(num_rows - 1, watermark);

    auto columns = chunk.detachColumns();
    columns.emplace_back(event_time_col);
    columns.emplace_back(watermark_col);
    chunk.setColumns(std::move(columns), num_rows);
    pending_chunks.push(std::move(chunk));

    if (num_rows == 0)
        return;

    pending_chunks.push(WatermarkMarker::create(getOutputPort().getHeader(), watermark));
}

Chunk CalculateWatermarksTransform::getRemaining()
{
    if (watermark.isNull())
        return {};

    return WatermarkMarker::create(getOutputPort().getHeader(), watermark);
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
