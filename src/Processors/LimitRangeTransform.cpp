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
    size_t limit_)
    : ISimpleTransform(header_, header_, true)
    , start_expression(std::move(start_expression_))
    , start_column_name(start_column_name_)
    , end_expression(std::move(end_expression_))
    , end_column_name(end_column_name_)
    , limit(limit_)
{
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

IProcessor::Status LimitRangeTransform::prepare()
{
    if (finished)
    {
        input.close();
        return Status::Finished;
    }
    return ISimpleTransform::prepare();
}

void LimitRangeTransform::transform(Chunk & chunk)
{
    if (chunk.empty() || finished)
    {
        if (!chunk.empty())
            chunk.clear();
        return;
    }

    const size_t num_rows = chunk.getNumRows();
    size_t output_start = 0;
    size_t output_end = num_rows;

    if (!started)
    {
        if (start_expression)
        {
            Block block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
            start_expression->execute(block, block.rows());
            ColumnPtr start_col = block.getByPosition(start_column_position).column;
            size_t first = findFirstTrue(start_col, block.rows());
            if (first >= block.rows())
            {
                chunk.clear();
                return;
            }
            started = true;
            output_start = first;
            Columns cols = block.getColumns();
            cols.erase(cols.begin() + start_column_position);
            chunk.setColumns(std::move(cols), block.rows());
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

    if (end_expression && output_end > output_start)
    {
        Block block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        end_expression->execute(block, block.rows());
        ColumnPtr end_col = block.getByPosition(end_column_position).column;
        size_t first_end = findFirstTrue(end_col, block.rows());
        if (first_end < block.rows())
            output_end = first_end;
    }

    if (limit > 0)
    {
        size_t remaining = limit - rows_output;
        if (remaining == 0)
        {
            finished = true;
            chunk.clear();
            return;
        }
        size_t take = output_end - output_start;
        if (take > remaining)
        {
            output_end = output_start + remaining;
        }
    }

    sliceChunk(chunk, output_start, output_end);
    rows_output += chunk.getNumRows();

    if (limit > 0 && rows_output >= limit)
        finished = true;
    else if (end_expression && output_end < num_rows_cur)
        finished = true;
}

}
