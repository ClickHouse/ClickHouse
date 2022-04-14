#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>
#include <Processors/Transforms/MergeJoinTransform.h>
#include <base/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TableJoin.h>
#include <Core/SortDescription.h>
#include <boost/core/noncopyable.hpp>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include <Core/SortCursor.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <base/defines.h>
#include <base/types.h>
#include <boost/logic/tribool.hpp>
#include "Common/Exception.h"
#include "Core/SettingsEnums.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_ROWS;
    extern const int LOGICAL_ERROR;
}

constexpr size_t EMPTY_VALUE_IDX = std::numeric_limits<size_t>::max();
using JoinKind = ASTTableJoin::Kind;


namespace
{

FullMergeJoinCursorPtr createCursor(const Block & block, const Names & columns)
{
    SortDescription desc;
    desc.reserve(columns.size());
    for (const auto & name : columns)
        desc.emplace_back(name);
    return std::make_unique<FullMergeJoinCursor>(block, desc);
}


template <bool has_left_nulls, bool has_right_nulls>
int nullableCompareAt(const IColumn & left_column, const IColumn & right_column, size_t lhs_pos, size_t rhs_pos, int null_direction_hint = 1)
{
    if constexpr (has_left_nulls && has_right_nulls)
    {
        const auto * left_nullable = checkAndGetColumn<ColumnNullable>(left_column);
        const auto * right_nullable = checkAndGetColumn<ColumnNullable>(right_column);

        if (left_nullable && right_nullable)
        {
            int res = left_column.compareAt(lhs_pos, rhs_pos, right_column, null_direction_hint);
            if (res)
                return res;

            /// NULL != NULL case
            if (left_column.isNullAt(lhs_pos))
                return null_direction_hint;

            return 0;
        }
    }

    if constexpr (has_left_nulls)
    {
        if (const auto * left_nullable = checkAndGetColumn<ColumnNullable>(left_column))
        {
            if (left_column.isNullAt(lhs_pos))
                return null_direction_hint;
            return left_nullable->getNestedColumn().compareAt(lhs_pos, rhs_pos, right_column, null_direction_hint);
        }
    }

    if constexpr (has_right_nulls)
    {
        if (const auto * right_nullable = checkAndGetColumn<ColumnNullable>(right_column))
        {
            if (right_column.isNullAt(rhs_pos))
                return -null_direction_hint;
            return left_column.compareAt(lhs_pos, rhs_pos, right_nullable->getNestedColumn(), null_direction_hint);
        }
    }

    return left_column.compareAt(lhs_pos, rhs_pos, right_column, null_direction_hint);
}

int ALWAYS_INLINE compareCursors(const SortCursorImpl & lhs, size_t lpos,
                                 const SortCursorImpl & rhs, size_t rpos)
{
    for (size_t i = 0; i < lhs.sort_columns_size; ++i)
    {
        const auto & desc = lhs.desc[i];
        int direction = desc.direction;
        int nulls_direction = desc.nulls_direction;

        int cmp = direction * nullableCompareAt<true, true>(
            *lhs.sort_columns[i],
            *rhs.sort_columns[i],
            lpos,
            rpos,
            nulls_direction);
        if (cmp != 0)
            return cmp;
    }
    return 0;
}

int ALWAYS_INLINE compareCursors(const SortCursorImpl & lhs, const SortCursorImpl & rhs)
{
    return compareCursors(lhs, lhs.getRow(), rhs, rhs.getRow());
}


bool ALWAYS_INLINE totallyLess(SortCursorImpl & lhs, SortCursorImpl & rhs)
{
    if (!lhs.isValid() || !rhs.isValid())
        return false;

    /// The last row of this cursor is no larger than the first row of the another cursor.
    int cmp = compareCursors(lhs, lhs.rows - 1, rhs, 0);
    return cmp < 0;
}

int ALWAYS_INLINE totallyCompare(SortCursorImpl & lhs, SortCursorImpl & rhs)
{
    if (totallyLess(lhs, rhs))
        return -1;
    if (totallyLess(rhs, lhs))
        return 1;
    return 0;
}

void addIndexColumn(const Columns & columns, ColumnUInt64 & indices, Chunk & result, size_t start, size_t limit)
{
    for (const auto & col : columns)
    {
        if (indices.empty())
        {
            result.addColumn(col->cut(start, limit));
        }
        else
        {
            if (limit == 0)
                limit = indices.size();

            assert(limit == indices.size());

            auto tmp_col = col->cloneResized(col->size() + 1);
            ColumnPtr new_col = tmp_col->index(indices, limit);
            result.addColumn(std::move(new_col));
        }
    }
}

bool sameNext(const SortCursorImpl & impl)
{
    for (size_t i = 0; i < impl.sort_columns_size; ++i)
    {
        const auto & col = *impl.sort_columns[i];
        int cmp = nullableCompareAt<true, true>(
            col, col, impl.getRow(), impl.getRow() + 1, 0);
        if (cmp != 0)
            return false;
    }
    return true;
}

size_t nextDistinct(SortCursorImpl & impl)
{
    assert(impl.isValid());
    size_t start_pos = impl.getRow();
    while (!impl.isLast() && sameNext(impl))
    {
        impl.next();
    }
    impl.next();

    if (impl.isValid())
        return impl.getRow() - start_pos;
    return impl.rows - start_pos;
}

}

FullMergeJoinCursor::CursorWithBlock & FullMergeJoinCursor::getCurrent()
{
    while (current != inputs.end() && !current->cursor.isValid())
        current++;

    return current != inputs.end() ? *current : empty_cursor;
}

void FullMergeJoinCursor::addChunk(Chunk && chunk)
{
    assert(!recieved_all_blocks);
    if (!chunk)
    {
        recieved_all_blocks = true;
        return;
    }

    dropBlocksUntilCurrent();
    inputs.emplace_back(sample_block, desc, std::move(chunk));

    if (current == inputs.end())
    {
        current = std::prev(inputs.end());
    }
}

void FullMergeJoinCursor::dropBlocksUntilCurrent()
{
    while (current != inputs.end() && !current->cursor.isValid())
        current++;

    inputs.erase(inputs.begin(), current);
}

bool FullMergeJoinCursor::fullyCompleted()
{
    return !getCurrent()->isValid() && recieved_all_blocks;
}

MergeJoinAlgorithm::MergeJoinAlgorithm(
    JoinPtr table_join_,
    const Blocks & input_headers)
    : table_join(table_join_)
    , log(&Poco::Logger::get("MergeJoinAlgorithm"))
{
    if (input_headers.size() != 2)
        throw Exception("MergeJoinAlgorithm requires exactly two inputs", ErrorCodes::LOGICAL_ERROR);

    auto strictness = table_join->getTableJoin().strictness();
    if (strictness != ASTTableJoin::Strictness::Any)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeJoinAlgorithm is not implemented for strictness != ANY");

    auto kind = table_join->getTableJoin().kind();
    if (!isInner(kind) && !isLeft(kind) && !isRight(kind) && !isFull(kind))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeJoinAlgorithm is not implemented for kind {}", kind);

    const auto & join_on = table_join->getTableJoin().getOnlyClause();

    cursors.push_back(createCursor(input_headers[0], join_on.key_names_left));
    cursors.push_back(createCursor(input_headers[1], join_on.key_names_right));
}


static ColumnPtr replicateRow(const IColumn & column, size_t num)
{
    assert(column.size() == 1);
    MutableColumnPtr res = column.cloneEmpty();
    res->insertManyFrom(column, 0, num);
    return std::move(res);
}

static void copyColumnsResized(const Chunk & chunk, size_t start, size_t size, Chunk & result_chunk)
{
    const auto & cols = chunk.getColumns();
    for (const auto & col : cols)
    {
        if (col->empty())
        {
            result_chunk.addColumn(col->cloneResized(size));
        }
        else if (col->size() == 1)
        {
            result_chunk.addColumn(replicateRow(*col, size));
        }
        else
        {
            assert(start + size <= col->size());
            result_chunk.addColumn(col->cut(start, size));
        }
    }
}

static Chunk createBlockWithDefaults(const Chunk & lhs, const Chunk & rhs, size_t start, size_t num_rows)
{
    Chunk result;
    copyColumnsResized(lhs, start, num_rows, result);
    copyColumnsResized(rhs, start, num_rows, result);
    return result;
}

static Chunk createBlockWithDefaults(const Chunk & lhs, FullMergeJoinCursor::CursorWithBlock & rhs)
{
    size_t start = rhs->getRow();
    size_t num_rows = rhs->rowsLeft();
    return createBlockWithDefaults(lhs, rhs.detach(), start, num_rows);
}

static Chunk createBlockWithDefaults(FullMergeJoinCursor::CursorWithBlock & lhs, const Chunk & rhs)
{
    size_t start = lhs->getRow();
    size_t num_rows = lhs->rowsLeft();
    return createBlockWithDefaults(lhs.detach(), rhs, start, num_rows);
}

void MergeJoinAlgorithm::initialize(Inputs inputs)
{
    if (inputs.size() != 2)
        throw Exception("MergeJoinAlgorithm requires exactly two inputs", ErrorCodes::LOGICAL_ERROR);

    LOG_DEBUG(log, "MergeJoinAlgorithm initialize, number of inputs: {}", inputs.size());

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        copyColumnsResized(inputs[i].chunk, 0, 0, sample_chunks.emplace_back());
        consume(inputs[i], i);
    }
}

static void prepareChunk(Chunk & chunk)
{
    if (!chunk)
        return;

    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);
}

void MergeJoinAlgorithm::consume(Input & input, size_t source_num)
{
    if (input.skip_last_row)
        throw Exception("skip_last_row is not supported", ErrorCodes::NOT_IMPLEMENTED);

    if (input.permutation)
        throw DB::Exception("permutation is not supported", ErrorCodes::NOT_IMPLEMENTED);

    LOG_DEBUG(log, "XXXX: consume from {} chunk: {}", source_num, input.chunk.getNumRows());

    if (input.chunk.getNumRows() >= EMPTY_VALUE_IDX)
        throw Exception("Too many rows in input", ErrorCodes::TOO_MANY_ROWS);

    if (input.chunk)
    {
        stat.num_blocks[source_num] += 1;
        stat.num_rows[source_num] += input.chunk.getNumRows();
    }

    prepareChunk(input.chunk);
    cursors[source_num]->addChunk(std::move(input.chunk));
}

static Chunk getRowFromChunk(const Chunk & chunk, size_t pos)
{
    Chunk result;
    copyColumnsResized(chunk, pos, 1, result);
    return result;
}

template <JoinKind kind>
static std::unique_ptr<AnyJoinState> anyJoinImpl(FullMergeJoinCursor::CursorWithBlock & left_cursor,
                                                 FullMergeJoinCursor::CursorWithBlock & right_cursor,
                                                 PaddedPODArray<UInt64> & left_map,
                                                 PaddedPODArray<UInt64> & right_map)
{
    static_assert(kind == JoinKind::Left || kind == JoinKind::Right || kind == JoinKind::Inner, "Invalid join kind");

    size_t num_rows = isLeft(kind) ? left_cursor->rowsLeft() :
                      isRight(kind) ? right_cursor->rowsLeft() :
                      std::min(left_cursor->rowsLeft(), right_cursor->rowsLeft());

    if constexpr (isLeft(kind) || isInner(kind))
        right_map.reserve(num_rows);

    if constexpr (isRight(kind) || isInner(kind))
        left_map.reserve(num_rows);

    size_t rpos = std::numeric_limits<size_t>::max();
    size_t lpos = std::numeric_limits<size_t>::max();
    int cmp = 0;
    assert(left_cursor->isValid() && right_cursor->isValid());
    while (left_cursor->isValid() && right_cursor->isValid())
    {
        lpos = left_cursor->getRow();
        rpos = right_cursor->getRow();

        cmp = compareCursors(left_cursor.cursor, right_cursor.cursor);
        if (cmp == 0)
        {
            if constexpr (isLeft(kind))
            {
                size_t lnum = nextDistinct(left_cursor.cursor);
                right_map.resize_fill(right_map.size() + lnum, rpos);
            }

            if constexpr (isRight(kind))
            {
                size_t rnum = nextDistinct(right_cursor.cursor);
                left_map.resize_fill(left_map.size() + rnum, lpos);
            }

            if constexpr (isInner(kind))
            {
                nextDistinct(left_cursor.cursor);
                nextDistinct(right_cursor.cursor);
                left_map.emplace_back(lpos);
                right_map.emplace_back(rpos);
            }
        }
        else if (cmp < 0)
        {
            size_t num = nextDistinct(left_cursor.cursor);
            if constexpr (isLeft(kind))
                right_map.resize_fill(right_map.size() + num, right_cursor->rows);
        }
        else
        {
            size_t num = nextDistinct(right_cursor.cursor);
            if constexpr (isRight(kind))
                left_map.resize_fill(left_map.size() + num, left_cursor->rows);
        }
    }

    std::unique_ptr<AnyJoinState> result = std::make_unique<AnyJoinState>();

    if (!left_cursor->isValid())
    {
        Chunk value = cmp == 0 ? getRowFromChunk(right_cursor.input, rpos): Chunk{};
        result->setLeft(left_cursor.cursor, lpos, std::move(value));

    }

    if (!right_cursor->isValid())
    {
        Chunk value = cmp == 0 ? getRowFromChunk(left_cursor.input, lpos): Chunk{};
        result->setRight(right_cursor.cursor, rpos, std::move(value));
    }

    return result;
}

static std::unique_ptr<AnyJoinState> anyJoinDispatch(JoinKind kind,
                                                     FullMergeJoinCursor::CursorWithBlock & left_cursor,
                                                     FullMergeJoinCursor::CursorWithBlock & right_cursor,
                                                     PaddedPODArray<UInt64> & left_map,
                                                     PaddedPODArray<UInt64> & right_map)
{
    if (isInner(kind))
    {
        return anyJoinImpl<JoinKind::Inner>(left_cursor, right_cursor, left_map, right_map);
    }
    else if (isLeft(kind))
    {
        return anyJoinImpl<JoinKind::Left>(left_cursor, right_cursor, left_map, right_map);
    }
    else if (isRight(kind))
    {
        return anyJoinImpl<JoinKind::Right>(left_cursor, right_cursor, left_map, right_map);
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported join kind: \"{}\"", kind);
    }
    __builtin_unreachable();
}

static std::pair<size_t, size_t> handleAnyJoinState(std::unique_ptr<AnyJoinState::Row> & state, FullMergeJoinCursor::CursorWithBlock & current)
{
    if (!state)
        return {};

    if (state->equals(current.cursor))
    {
        size_t start_pos = current->getRow();
        size_t num = nextDistinct(current.cursor);

        /// We've found row with other key, no need to skip any rows with current key.
        if (current->isValid())
            state.reset();

        return std::make_pair(start_pos, num);
    }
    else
    {
        state.reset();
    }
    return {};
}

MergeJoinAlgorithm::Status MergeJoinAlgorithm::anyJoin(JoinKind kind)
{
    if (any_join_state)
    {
        auto & left_current = cursors[0]->getCurrent();
        Chunk right_chunk = (any_join_state->left && any_join_state->left->value) ? any_join_state->left->value.clone() : sample_chunks[1].clone();
        if (auto [start, length] = handleAnyJoinState(any_join_state->left, left_current); length > 0 && isLeft(kind))
        {
            return Status(createBlockWithDefaults(left_current.input, right_chunk, start, length));
        }

        auto & right_current = cursors[1]->getCurrent();
        Chunk left_chunk = (any_join_state->right && any_join_state->right->value) ? any_join_state->right->value.clone() : sample_chunks[0].clone();
        if (auto [start, length] = handleAnyJoinState(any_join_state->right, right_current); length > 0 && isRight(kind))
        {
            return Status(createBlockWithDefaults(left_chunk, right_current.input, start, length));
        }
    }

    auto & current_left = cursors[0]->getCurrent();
    if (!current_left->isValid())
        return Status(0);

    auto & current_right = cursors[1]->getCurrent();
    if (!current_right->isValid())
        return Status(1);

    auto left_map = ColumnUInt64::create();
    auto right_map = ColumnUInt64::create();
    size_t prev_pos[] = {current_left->getRow(), current_right->getRow()};

    any_join_state = anyJoinDispatch(kind, current_left, current_right, left_map->getData(), right_map->getData());

    assert(left_map->empty() || right_map->empty() || left_map->size() == right_map->size());
    size_t num_result_rows = std::max(left_map->size(), right_map->size());

    Chunk result;
    addIndexColumn(current_left.input.getColumns(), *left_map, result, prev_pos[0], num_result_rows);
    addIndexColumn(current_right.input.getColumns(), *right_map, result, prev_pos[1], num_result_rows);
    return Status(std::move(result));
}

IMergingAlgorithm::Status MergeJoinAlgorithm::mergeImpl()
{
    auto kind = table_join->getTableJoin().kind();
    auto strictness = table_join->getTableJoin().strictness();

    LOG_DEBUG(log, "XXXX: merge, {} {}", kind, strictness);
    if (required_input.has_value())
    {
        size_t r = required_input.value();
        required_input = {};
        return Status(r);
    }

    if (!cursors[0]->getCurrent()->isValid() && !cursors[0]->fullyCompleted())
        return Status(0);

    if (!cursors[1]->getCurrent()->isValid() && !cursors[1]->fullyCompleted())
        return Status(1);

    if (cursors[0]->fullyCompleted() || cursors[1]->fullyCompleted())
    {
        if (!cursors[0]->fullyCompleted() && isLeftOrFull(kind))
            return Status(createBlockWithDefaults(cursors[0]->getCurrent(), sample_chunks[1]));

        if (!cursors[1]->fullyCompleted() && isRightOrFull(kind))
            return Status(createBlockWithDefaults(sample_chunks[0], cursors[1]->getCurrent()));

        return Status({}, true);
    }


    if (int cmp = totallyCompare(cursors[0]->getCurrent().cursor, cursors[1]->getCurrent().cursor); cmp == 6666)
    {
        if (cmp < 0)
        {
            if (isLeftOrFull(kind))
                return Status(createBlockWithDefaults(cursors[0]->getCurrent(), sample_chunks[1]));
            cursors[0]->getCurrent().detach();
            return Status(0);
        }

        if (cmp > 0)
        {
            if (isRightOrFull(kind))
                return Status(createBlockWithDefaults(sample_chunks[0], cursors[1]->getCurrent()));
            cursors[1]->getCurrent().detach();
            return Status(1);
        }
    }

    if (strictness == ASTTableJoin::Strictness::Any)
        return anyJoin(kind);

    throw Exception("Unsupported strictness: " + toString(strictness), ErrorCodes::NOT_IMPLEMENTED);
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
    algorithm.onFinish(total_stopwatch.elapsedSeconds());
}


}
