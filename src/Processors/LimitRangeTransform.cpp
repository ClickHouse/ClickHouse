#include <Processors/LimitRangeTransform.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Chunk.h>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

static UInt64 saturatingAdd(UInt64 lhs, UInt64 rhs)
{
    if (lhs > std::numeric_limits<UInt64>::max() - rhs)
        return std::numeric_limits<UInt64>::max();
    return lhs + rhs;
}

LimitRangeTransform::LimitRangeTransform(
    SharedHeader header_,
    ExpressionActionsPtr start_expression_,
    const String & start_column_name_,
    ExpressionActionsPtr end_expression_,
    const String & end_column_name_,
    bool start_all_,
    std::optional<UInt64> limit_,
    bool always_read_till_end_)
    : ISimpleTransform(header_, header_, true)
    , start_expression(std::move(start_expression_))
    , start_column_name(start_column_name_)
    , end_expression(std::move(end_expression_))
    , end_column_name(end_column_name_)
    , start_all(start_all_)
    , limit(limit_)
    , always_read_till_end(always_read_till_end_)
{
    if (limit && *limit == 0)
    {
        setDone();
        return;
    }

    if (start_expression)
    {
        Block block = getInputPort().getHeader().cloneEmpty();
        start_expression->execute(block, /*dry_run=*/true);
        const auto & col = block.getByName(start_column_name);
        if (!col.type->canBeUsedInBooleanContext())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "LIMIT AFTER expression must be boolean, got {}", col.type->getName());
        start_column_position = block.getPositionByName(start_column_name);
    }
    if (end_expression)
    {
        Block block = getInputPort().getHeader().cloneEmpty();
        end_expression->execute(block, /*dry_run=*/true);
        const auto & col = block.getByName(end_column_name);
        if (!col.type->canBeUsedInBooleanContext())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "LIMIT UNTIL expression must be boolean, got {}", col.type->getName());
        end_column_position = block.getPositionByName(end_column_name);
    }
}

bool LimitRangeTransform::isTrueAt(const ColumnPtr & column, size_t row_num)
{
    if (!column)
        return false;

    const IColumn * col = column.get();
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(col))
    {
        if (nullable->isNullAt(row_num))
            return false;

        col = &nullable->getNestedColumn();
    }

    if (const auto * uint8 = typeid_cast<const ColumnUInt8 *>(col))
        return uint8->getData()[row_num] != 0;

    return col->getBool(row_num);
}

size_t LimitRangeTransform::findFirstTrue(const ColumnPtr & column, size_t num_rows)
{
    if (!column || num_rows == 0)
        return num_rows;

    for (size_t i = 0; i < num_rows; ++i)
    {
        if (isTrueAt(column, i))
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

void LimitRangeTransform::filterChunk(Chunk & chunk, const IColumn::Filter & filter, size_t filtered_rows)
{
    auto columns = chunk.detachColumns();
    for (auto & col : columns)
    {
        if (isColumnConst(*col))
            col = col->cut(0, filtered_rows);
        else
            col = col->filter(filter, filtered_rows);
    }

    chunk.setColumns(std::move(columns), filtered_rows);
}

void LimitRangeTransform::transformAll(Chunk & chunk, const ColumnPtr & start_col, const ColumnPtr & end_col)
{
    const size_t num_rows = chunk.getNumRows();
    IColumn::Filter filter(num_rows, 0);
    size_t filtered_rows = 0;

    for (size_t row = 0; row < num_rows; ++row)
    {
        const UInt64 current_row = rows_read + row;
        const bool end_match = end_col && isTrueAt(end_col, row);
        if (end_match)
        {
            has_repeated_unbounded_window = false;
            repeated_window_end = current_row;
        }

        const bool start_match = start_col && isTrueAt(start_col, row);
        if (start_match && !end_match)
        {
            if (limit)
                repeated_window_end = std::max(repeated_window_end, saturatingAdd(current_row, *limit));
            else
                has_repeated_unbounded_window = true;
        }

        const bool should_emit = has_repeated_unbounded_window || (limit && current_row < repeated_window_end);
        if (should_emit)
        {
            filter[row] = 1;
            ++filtered_rows;
        }
    }

    rows_read += num_rows;

    if (filtered_rows == 0)
    {
        chunk.clear();
        return;
    }

    if (filtered_rows < num_rows)
        filterChunk(chunk, filter, filtered_rows);
}

void LimitRangeTransform::setDone()
{
    if (always_read_till_end)
        done_outputting = true;
    else
        stopReading();
}

void LimitRangeTransform::transform(Chunk & chunk)
{
    if (chunk.empty())
        return;

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(chunk.getNumRows());

    if (done_outputting)
    {
        chunk.clear();
        return;
    }

    if (limit && rows_output >= *limit)
    {
        setDone();
        chunk.clear();
        return;
    }

    const size_t num_rows = chunk.getNumRows();
    size_t output_start = 0;
    size_t output_end = num_rows;

    ColumnPtr start_col;
    if (start_expression && (start_all || !started))
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

    if (start_all)
    {
        transformAll(chunk, start_col, end_col);
        return;
    }

    /// end_in_chunk caches the result of findFirstTrue(end_col) when it is computed
    /// inside the !started block, to avoid a redundant second scan below.
    size_t end_in_chunk = num_rows;
    bool end_searched = false;

    if (!started)
    {
        if (start_col)
        {
            const size_t first_start = findFirstTrue(start_col, num_rows);

            if (end_col)
            {
                end_in_chunk = findFirstTrue(end_col, num_rows);
                end_searched = true;
                /// UNTIL fired at or before AFTER (covers: AFTER not found, UNTIL at same row,
                /// UNTIL precedes AFTER).  The window is permanently closed.
                if (end_in_chunk <= first_start)
                {
                    if (end_in_chunk < num_rows)
                        setDone();
                    chunk.clear();
                    return;
                }
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

    if (end_col && output_end > output_start)
    {
        const size_t first_end = end_searched ? end_in_chunk : findFirstTrue(end_col, num_rows);
        if (first_end < num_rows)
            output_end = first_end;
    }

    if (output_end <= output_start)
    {
        if (end_col && output_end < num_rows)
            setDone();

        chunk.clear();
        return;
    }

    if (limit)
    {
        UInt64 remaining = *limit - rows_output;
        if (remaining == 0)
        {
            setDone();
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
        setDone();
    else if (end_col && output_end < num_rows)
        setDone();
}

}
