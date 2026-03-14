#include <Processors/LimitRangeTransform.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Chunk.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

static bool canUseTypeForCondition(const DataTypePtr & type)
{
    return type->canBeUsedInBooleanContext();
}

LimitRangeTransform::LimitRangeTransform(
    SharedHeader header_,
    ExpressionActionsPtr start_expression_,
    const String & start_column_name_,
    ExpressionActionsPtr end_expression_,
    const String & end_column_name_,
    std::optional<UInt64> limit_)
    : ISimpleTransform(header_, header_, true)
    , start_expression(std::move(start_expression_))
    , start_column_name(start_column_name_)
    , end_expression(std::move(end_expression_))
    , end_column_name(end_column_name_)
    , limit(limit_)
{
    if (limit && *limit == 0)
        stopReading();

    if (start_expression)
    {
        Block block = getInputPort().getHeader().cloneEmpty();
        start_expression->execute(block);
        const auto & col = block.getByName(start_column_name);
        if (!canUseTypeForCondition(col.type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "LIMIT AFTER expression must be boolean, got {}", col.type->getName());
        start_column_position = block.getPositionByName(start_column_name);
    }
    if (end_expression)
    {
        Block block = getInputPort().getHeader().cloneEmpty();
        end_expression->execute(block);
        const auto & col = block.getByName(end_column_name);
        if (!canUseTypeForCondition(col.type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "LIMIT UNTIL expression must be boolean, got {}", col.type->getName());
        end_column_position = block.getPositionByName(end_column_name);
    }
}

size_t LimitRangeTransform::findFirstTrue(const ColumnPtr & column, size_t num_rows)
{
    if (!column || num_rows == 0)
        return num_rows;

    const IColumn * col = column.get();
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(col))
        col = &nullable->getNestedColumn();

    if (const auto * uint8 = typeid_cast<const ColumnUInt8 *>(col))
    {
        const auto & data = uint8->getData();
        for (size_t i = 0; i < num_rows; ++i)
        {
            if (data[i] != 0)
                return i;
        }
        return num_rows;
    }

    for (size_t i = 0; i < num_rows; ++i)
    {
        if (column->getBool(i))
            return i;
    }
    return num_rows;
}

void LimitRangeTransform::sliceChunk(Chunk & chunk, size_t start_row, size_t end_row)
{
    if (start_row >= end_row)
    {
        chunk.clear();
        return;
    }
    size_t length = end_row - start_row;
    auto columns = chunk.detachColumns();
    for (auto & col : columns)
        col = col->cut(start_row, length);
    chunk.setColumns(std::move(columns), length);
}

void LimitRangeTransform::transform(Chunk & chunk)
{
    if (chunk.empty())
        return;

    if (limit && rows_output >= *limit)
    {
        stopReading();
        chunk.clear();
        return;
    }

    const size_t num_rows = chunk.getNumRows();
    size_t output_start = 0;
    size_t output_end = num_rows;

    ColumnPtr start_col;
    if (!started && start_expression)
    {
        Block block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        start_expression->execute(block, block.rows());
        start_col = block.getByPosition(start_column_position).column;
    }

    ColumnPtr end_col;
    if (end_expression)
    {
        Block block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        end_expression->execute(block, block.rows());
        end_col = block.getByPosition(end_column_position).column;
    }

    if (!started)
    {
        if (start_col)
        {
            const size_t first_start = findFirstTrue(start_col, num_rows);
            const size_t first_end = end_col ? findFirstTrue(end_col, num_rows) : num_rows;

            if (first_end <= first_start)
            {
                if (first_end < num_rows)
                    stopReading();
                chunk.clear();
                return;
            }

            if (first_start >= num_rows)
            {
                chunk.clear();
                return;
            }

            started = true;
            output_start = first_start;
        }
        else
        {
            started = true;
        }
    }

    size_t num_rows_cur = chunk.getNumRows();
    if (output_start >= num_rows_cur)
    {
        chunk.clear();
        return;
    }

    if (end_col && output_end > output_start)
    {
        size_t first_end = findFirstTrue(end_col, num_rows);
        if (first_end < num_rows)
            output_end = first_end;
    }

    if (output_end <= output_start)
    {
        if (end_col && output_end < num_rows)
            stopReading();

        chunk.clear();
        return;
    }

    if (limit)
    {
        UInt64 remaining = *limit - rows_output;
        if (remaining == 0)
        {
            stopReading();
            chunk.clear();
            return;
        }

        size_t take = output_end - output_start;
        if (take > remaining)
            output_end = output_start + remaining;
    }

    sliceChunk(chunk, output_start, output_end);
    rows_output += chunk.getNumRows();

    if (limit && rows_output >= *limit)
        stopReading();
    else if (end_col && output_end < num_rows_cur)
        stopReading();
}

}
