#include <cstddef>
#include <vector>
#include <Processors/Transforms/MergeJoinTransform.h>
#include <base/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TableJoin.h>
#include <Core/SortDescription.h>
#include "Columns/IColumn.h"
#include "Core/SortCursor.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_ROWS;
}

namespace
{

constexpr size_t EMPTY_VALUE = std::numeric_limits<size_t>::max();

SortCursorImpl createCursor(const Block & block, const Names & columns)
{
    SortDescription res;
    res.reserve(columns.size());

    for (const auto & name : columns)
        res.emplace_back(block.getPositionByName(name));

    return SortCursorImpl(block.cloneEmpty().getColumns(), res);
}


int ALWAYS_INLINE compareCursors(const SortCursor & lhs, const SortCursor & rhs)
{
    for (size_t i = 0; i < lhs->sort_columns_size; ++i)
    {
        const auto & desc = lhs->desc[i];
        int direction = desc.direction;
        int nulls_direction = desc.nulls_direction;
        int res = direction * lhs->sort_columns[i]->compareAt(lhs->getRow(), rhs->getRow(), *(rhs.impl->sort_columns[i]), nulls_direction);
        if (res != 0)
            return res;
    }
    return 0;
}

}

MergeJoinAlgorithm::MergeJoinAlgorithm(
    const TableJoin & table_join_,
    const Blocks & input_headers)
    : table_join(table_join_)
    , log(&Poco::Logger::get("MergeJoinAlgorithm"))
{
    if (input_headers.size() != 2)
        throw Exception("MergeJoinAlgorithm requires exactly two inputs", ErrorCodes::LOGICAL_ERROR);

    if (table_join.strictness() != ASTTableJoin::Strictness::Any)
        throw Exception("MergeJoinAlgorithm is not implemented for strictness != ANY", ErrorCodes::NOT_IMPLEMENTED);

    const auto & join_on = table_join.getOnlyClause();

    cursors.push_back(createCursor(input_headers[0], join_on.key_names_left));
    cursors.push_back(createCursor(input_headers[1], join_on.key_names_right));
}

void MergeJoinAlgorithm::initialize(Inputs inputs)
{
    if (inputs.size() != 2)
        throw Exception("MergeJoinAlgorithm requires exactly two inputs", ErrorCodes::LOGICAL_ERROR);
    LOG_DEBUG(log, "MergeJoinAlgorithm initialize, number of inputs: {}", inputs.size());
    current_inputs.resize(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i)
        consume(inputs[i], i);
}

static void prepareChunk(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);
}

void MergeJoinAlgorithm::consume(Input & input, size_t source_num)
{
    LOG_DEBUG(log, "Consume from {} chunk: {}", source_num, bool(input.chunk));

    left_stream_finished = left_stream_finished || (!input.chunk && source_num == 0);
    right_stream_finished = right_stream_finished || (!input.chunk && source_num == 1);

    prepareChunk(input.chunk);

    if (input.chunk.getNumRows() >= EMPTY_VALUE)
        throw Exception("Too many rows in input", ErrorCodes::TOO_MANY_ROWS);

    current_inputs[source_num] = std::move(input);
    if (current_inputs[source_num].chunk)
    {
        cursors[source_num].reset(current_inputs[source_num].chunk.getColumns(), {}, current_inputs[source_num].permutation);
    }
}

IMergingAlgorithm::Status MergeJoinAlgorithm::merge()
{
    if (!current_inputs[0].chunk && !left_stream_finished)
    {
        return Status(0);
    }

    if (!current_inputs[1].chunk && !right_stream_finished)
    {
        return Status(1);
    }

    if (left_stream_finished || right_stream_finished)
    {
        return Status({}, true);
    }

    SortCursor left_cursor(&cursors[0]);
    SortCursor right_cursor(&cursors[1]);

    if (!left_cursor->isValid() || left_cursor.totallyLessOrEquals(right_cursor))
    {
        current_inputs[0] = {};
        if (left_stream_finished)
        {
            return Status({}, true);
        }
        return Status(0);
    }

    if (!right_cursor->isValid() || right_cursor.totallyLessOrEquals(left_cursor))
    {
        current_inputs[1] = {};
        if (right_stream_finished)
        {
            return Status({}, true);
        }
        return Status(1);
    }

    auto left_map = ColumnUInt64::create();
    auto right_map = ColumnUInt64::create();

    while (left_cursor->isValid() && right_cursor->isValid())
    {
        int cmp = compareCursors(left_cursor, right_cursor);
        if (cmp == 0)
        {
            left_map->getData().emplace_back(left_cursor->getPosRef());
            right_map->getData().emplace_back(right_cursor->getPosRef());
            left_cursor->next();
            right_cursor->next();
        }
        else if (cmp < 0)
        {
            left_cursor->next();
            left_map->getData().emplace_back(left_cursor->getPosRef());
            right_map->getData().emplace_back(right_cursor->rows);
        }
        else
        {
            right_cursor->next();
        }
    }

    while (left_cursor->isValid())
    {
        left_map->getData().emplace_back(left_cursor->getPosRef());
        right_map->getData().emplace_back(right_cursor->rows);
        left_cursor->next();
    }

    Chunk result;
    for (const auto & col : current_inputs[0].chunk.getColumns())
    {
        auto tmp_col = col->cloneResized(col->size() + 1);
        ColumnPtr new_col = tmp_col->index(*left_map, 0);
        result.addColumn(std::move(new_col));
    }

    for (const auto & col : current_inputs[1].chunk.getColumns())
    {
        auto tmp_col = col->cloneResized(col->size() + 1);
        ColumnPtr new_col = tmp_col->index(*right_map, 0);
        result.addColumn(std::move(new_col));
    }

    if (!left_cursor->isValid())
    {
        current_inputs[0] = {};
    }

    if (!right_cursor->isValid())
    {
        current_inputs[1] = {};
    }

    return Status(std::move(result), left_stream_finished || right_stream_finished);
}

MergeJoinTransform::MergeJoinTransform(
        const TableJoin & table_join,
        const Blocks & input_headers,
        const Block & output_header,
        UInt64 limit_hint)
    : IMergingTransform<MergeJoinAlgorithm>(input_headers, output_header, true, limit_hint, table_join, input_headers)
    , log(&Poco::Logger::get("MergeJoinTransform"))
{
    LOG_TRACE(log, "Will use MergeJoinTransform");
}

void MergeJoinTransform::onFinish()
{
    LOG_TRACE(log, "onFinish");
}


}
