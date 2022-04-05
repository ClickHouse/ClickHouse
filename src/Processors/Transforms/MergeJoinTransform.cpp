#include <cstddef>
#include <memory>
#include <vector>
#include <Processors/Transforms/MergeJoinTransform.h>
#include <base/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TableJoin.h>
#include <Core/SortDescription.h>
#include <boost/core/noncopyable.hpp>
#include "Columns/ColumnsNumber.h"
#include "Columns/IColumn.h"
#include "Core/SortCursor.h"
#include "Parsers/ASTTablesInSelectQuery.h"
#include "base/defines.h"
#include "base/types.h"


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


FullMergeJoinCursor createCursor(const Block & block, const Names & columns)
{
    SortDescription desc;
    desc.reserve(columns.size());

    for (const auto & name : columns)
        desc.emplace_back(block.getPositionByName(name));

    return FullMergeJoinCursor(block.cloneEmpty().getColumns(), desc);
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

void addIndexColumn(const Columns & columns, const IColumn & indices, Chunk & result)
{
    for (const auto & col : columns)
    {
        if (indices.empty())
        {
            result.addColumn(col);
        }
        else
        {
            auto tmp_col = col->cloneResized(col->size() + 1);
            ColumnPtr new_col = tmp_col->index(indices, 0);
            result.addColumn(std::move(new_col));
        }
    }
}

}

MergeJoinAlgorithm::MergeJoinAlgorithm(
    JoinPtr table_join_,
    const Blocks & input_headers)
    : table_join(table_join_)
    , log(&Poco::Logger::get("MergeJoinAlgorithm"))
{
    if (input_headers.size() != 2)
        throw Exception("MergeJoinAlgorithm requires exactly two inputs", ErrorCodes::LOGICAL_ERROR);

    if (table_join->getTableJoin().strictness() != ASTTableJoin::Strictness::Any)
        throw Exception("MergeJoinAlgorithm is not implemented for strictness != ANY", ErrorCodes::NOT_IMPLEMENTED);

    const auto & join_on = table_join->getTableJoin().getOnlyClause();

    cursors.push_back(createCursor(input_headers[0], join_on.key_names_left));
    cursors.push_back(createCursor(input_headers[1], join_on.key_names_right));
}


static void createSampleChunk(const Chunk & chunk, Chunk & sample_chunk, size_t size = 0)
{
    const auto & cols = chunk.getColumns();
    for (const auto & col : cols)
    {
        sample_chunk.addColumn(col->cloneResized(size));
    }
}

void MergeJoinAlgorithm::initialize(Inputs inputs)
{
    if (inputs.size() != 2)
        throw Exception("MergeJoinAlgorithm requires exactly two inputs", ErrorCodes::LOGICAL_ERROR);
    LOG_DEBUG(log, "MergeJoinAlgorithm initialize, number of inputs: {}", inputs.size());
    current_inputs.resize(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        createSampleChunk(inputs[i].chunk, sample_chunks.emplace_back());
        consume(inputs[i], i);
    }
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
        cursors[source_num].getImpl().reset(current_inputs[source_num].chunk.getColumns(), {}, current_inputs[source_num].permutation);
    }
}

static size_t ALWAYS_INLINE rowsLeft(SortCursor cursor)
{
    return cursor->rows - cursor->getPosRef();
}

using JoinKind = ASTTableJoin::Kind;

template <JoinKind kind>
static void leftOrFullAny(SortCursor left_cursor, SortCursor right_cursor, PaddedPODArray<UInt64> & left_map, PaddedPODArray<UInt64> & right_map)
{
    static_assert(kind == JoinKind::Left || kind == JoinKind::Right || kind == JoinKind::Inner, "Invalid join kind");

    size_t num_rows = kind == JoinKind::Left ? rowsLeft(left_cursor) :
                      kind == JoinKind::Right ? rowsLeft(right_cursor) :
                      std::min(rowsLeft(left_cursor), rowsLeft(right_cursor));

    constexpr bool is_left_or_inner = kind == JoinKind::Left || kind == JoinKind::Inner;
    constexpr bool is_right_or_inner = kind == JoinKind::Right || kind == JoinKind::Inner;

    if constexpr (is_left_or_inner)
        right_map.reserve(num_rows);

    if constexpr (is_right_or_inner)
        left_map.reserve(num_rows);

    while (left_cursor->isValid() && right_cursor->isValid())
    {
        int cmp = compareCursors(left_cursor, right_cursor);
        if (cmp == 0)
        {
            if constexpr (is_left_or_inner)
                right_map.emplace_back(right_cursor->getRow());

            if constexpr (is_right_or_inner)
                left_map.emplace_back(left_cursor->getRow());

            if constexpr (is_left_or_inner)
                left_cursor->next();

            if constexpr (is_right_or_inner)
                right_cursor->next();

        }
        else if (cmp < 0)
        {
            if constexpr (kind == JoinKind::Left)
                right_map.emplace_back(right_cursor->rows);
            left_cursor->next();
        }
        else
        {
            if constexpr (kind == JoinKind::Right)
                left_map.emplace_back(left_cursor->rows);
            right_cursor->next();
        }
    }

    while (left_cursor->isValid() && kind == JoinKind::Left)
    {
        right_map.emplace_back(right_cursor->rows);
        left_cursor->next();
    }

    while (right_cursor->isValid() && kind == JoinKind::Right)
    {
        left_map.emplace_back(left_cursor->rows);
        right_cursor->next();
    }
}

IMergingAlgorithm::Status MergeJoinAlgorithm::merge()
{
    if (current_inputs[0].skip_last_row || current_inputs[1].skip_last_row)
        throw Exception("MergeJoinAlgorithm does not support skipLastRow", ErrorCodes::LOGICAL_ERROR);

    if (!current_inputs[0].chunk && !left_stream_finished)
    {
        return Status(0);
    }

    if (!current_inputs[1].chunk && !right_stream_finished)
    {
        return Status(1);
    }

    JoinKind kind = table_join->getTableJoin().kind();

    if (left_stream_finished && right_stream_finished)
    {
        return Status({}, true);
    }

    if (isInner(kind) && (left_stream_finished || right_stream_finished))
    {
        return Status({}, true);
    }

    auto create_block_with_defaults = [] (const Chunk & lhs, const Chunk & rhs) -> Chunk
    {
        Chunk result;
        size_t num_rows = std::max(lhs.getNumRows(), rhs.getNumRows());
        createSampleChunk(lhs, result, num_rows);
        createSampleChunk(rhs, result, num_rows);
        return result;
    };

    if (isLeftOrFull(kind) && right_stream_finished)
    {
        Chunk result = create_block_with_defaults(current_inputs[0].chunk, sample_chunks[1]);
        current_inputs[0] = {};
        return Status(std::move(result), left_stream_finished && right_stream_finished);
    }

    if (isRightOrFull(kind) && left_stream_finished)
    {
        Chunk result = create_block_with_defaults(sample_chunks[0], current_inputs[1].chunk);
        current_inputs[1] = {};
        return Status(std::move(result), left_stream_finished && right_stream_finished);
    }

    SortCursor left_cursor = cursors[0].getCursor();
    SortCursor right_cursor = cursors[1].getCursor();

    if (!left_cursor->isValid() || (right_cursor->isValid() && left_cursor.totallyLessOrEquals(right_cursor)))
    {
        current_inputs[0] = {};
        if (left_stream_finished)
        {
            return Status({}, true);
        }
        return Status(0);
    }

    if (!right_cursor->isValid() || (left_cursor->isValid() && right_cursor.totallyLessOrEquals(left_cursor)))
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
    if (isInner(kind))
    {
        leftOrFullAny<JoinKind::Inner>(left_cursor, right_cursor, left_map->getData(), right_map->getData());
    }
    else if (isLeft(kind))
    {
        leftOrFullAny<JoinKind::Left>(left_cursor, right_cursor, left_map->getData(), right_map->getData());
    }
    else if (isRight(kind))
    {
        leftOrFullAny<JoinKind::Right>(left_cursor, right_cursor, left_map->getData(), right_map->getData());
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported join kind: \"{}\"", table_join->getTableJoin().kind());
    }

    Chunk result;
    addIndexColumn(current_inputs[0].chunk.getColumns(), *left_map, result);
    addIndexColumn(current_inputs[1].chunk.getColumns(), *right_map, result);

    if (!left_cursor->isValid())
    {
        current_inputs[0] = {};
    }

    if (!right_cursor->isValid())
    {
        current_inputs[1] = {};
    }

    return Status(std::move(result), left_stream_finished && right_stream_finished);
}

MergeJoinTransform::MergeJoinTransform(
        JoinPtr table_join,
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
