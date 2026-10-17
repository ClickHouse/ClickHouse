#include <Processors/Streaming/CalculateWatermarksTransform.h>
#include <Processors/Port.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>

#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>

#include <DataTypes/IDataType.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActionsSettings.h>

#include <algorithm>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename ColumnType>
ColumnPtr calculateWatermarkColumnTyped(const IColumn & column, const Field & previous_watermark)
{
    using ValueType = typename ColumnType::ValueType;

    const auto & data = assert_cast<const ColumnType &>(column).getData();
    auto result_column = column.cloneEmpty();
    auto & result = assert_cast<ColumnType &>(*result_column).getData();
    result.resize(data.size());

    ValueType running = data[0];
    if (!previous_watermark.isNull())
    {
        if constexpr (is_decimal<ValueType>)
            running = previous_watermark.safeGet<DecimalField<ValueType>>().getValue();
        else
            running = static_cast<ValueType>(previous_watermark.safeGet<NearestFieldType<ValueType>>());
    }

    for (size_t i = 0; i < data.size(); ++i)
    {
        running = std::max(running, data[i]);
        result[i] = running;
    }

    return result_column;
}

ColumnPtr calculateWatermarkColumn(const IDataType & type, const IColumn & column, const Field & previous_watermark)
{
    switch (type.getTypeId())
    {
        case TypeIndex::Date:
            return calculateWatermarkColumnTyped<ColumnUInt16>(column, previous_watermark);
        case TypeIndex::Date32:
            return calculateWatermarkColumnTyped<ColumnInt32>(column, previous_watermark);
        case TypeIndex::DateTime:
            return calculateWatermarkColumnTyped<ColumnUInt32>(column, previous_watermark);
        case TypeIndex::DateTime64:
            return calculateWatermarkColumnTyped<ColumnDecimal<DateTime64>>(column, previous_watermark);
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected watermark type {}", type.getName());
    }
}

}

CalculateWatermarksTransform::CalculateWatermarksTransform(
    SharedHeader input_header_,
    SharedHeader output_header_,
    ActionsDAG watermark_expression_,
    Field initial_watermark_,
    ContextPtr context_)
    : ISimpleTransform(input_header_, output_header_, /*skip_empty_chunks=*/false)
    , result_name(watermark_expression_.getOutputs().front()->result_name)
    , watermark_expression(ExpressionActions::create(std::move(watermark_expression_), ExpressionActionsSettings(context_)))
    , watermark(std::move(initial_watermark_))
{
}

void CalculateWatermarksTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
    {
        chunk.addColumn(getOutputPort().getHeader().getByName(WatermarkColumn::name).column->cloneEmpty());
        return;
    }

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
    watermark_expression->execute(block, num_rows);

    const auto & result = block.getByName(result_name);
    auto watermark_column = calculateWatermarkColumn(*result.type, *result.column->convertToFullColumnIfConst()->convertToFullColumnIfSparse(), watermark);
    watermark_column->get(num_rows - 1, watermark);

    chunk.addColumn(std::move(watermark_column));
}

}
