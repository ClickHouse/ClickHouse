#include <cassert>
#include <cstddef>
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


bool ALWAYS_INLINE totallyLess(const FullMergeJoinCursor & lhs, const FullMergeJoinCursor & rhs)
{
    if (!lhs.isValid() || !rhs.isValid())
        return false;

    const auto & lhs_impl = lhs.getCurrent().impl;
    const auto & rhs_impl = rhs.getCurrent().impl;
    /// The last row of this cursor is no larger than the first row of the another cursor.
    int cmp = compareCursors(lhs_impl, lhs_impl.rows - 1, rhs_impl, 0);
    return cmp < 0;
}

int ALWAYS_INLINE totallyCompare(const FullMergeJoinCursor & lhs, const FullMergeJoinCursor & rhs)
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
    size_t start_pos = impl.getRow();
    while (sameNext(impl))
    {
        impl.next();
    }
    impl.next();
    return impl.getRow() - start_pos;
}

Chunk createBlockWithDefaults(const Chunk & lhs, const Chunk & rhs, size_t start, size_t num_rows)
{
    Chunk result;
    copyColumnsResized(lhs, start, num_rows, result);
    copyColumnsResized(rhs, start, num_rows, result);
    return result;
}

Chunk createBlockWithDefaults(const Chunk & lhs, FullMergeJoinCursor & rhs)
{
    size_t start = rhs.getCurrent().impl.getRow();
    size_t rows_left = rhs.getCurrent().impl.rowsLeft();
    return createBlockWithDefaults(lhs, rhs.detachCurrentChunk(), start, rows_left);
}

Chunk createBlockWithDefaults(FullMergeJoinCursor & lhs, const Chunk & rhs)
{
    size_t start = lhs.getCurrent().impl.getRow();
    size_t rows_left = lhs.getCurrent().impl.rowsLeft();
    return createBlockWithDefaults(lhs.detachCurrentChunk(), rhs, start, rows_left);
}

}

void FullMergeJoinCursor::next()
{
    if (current == inputs.end())
        return;

    if (current->impl.isValid())
    {
        current->impl.next();
        return;
    }
    current++;

    if (current == inputs.end())
        return;

    assert(current->impl.isValid());
}

/// The current row of is not equal to the last avaliable row
/// Also returns false if the current row is not valid and we stream wasn't finished.
bool FullMergeJoinCursor::haveAllCurrentRange() const
{
    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} haveAllCurrentRange: "
        "size {}, "
        "end: {} [{}/{}]"
        " isValid: {} isLast {}  "
        "recieved_all_blocks {}", __FILE__, __LINE__,
        inputs.size(),
        current == inputs.end(),
        current != inputs.end() ? current->impl.getRow() : 0,
        current != inputs.end() ? current->impl.rows : 0,
        current != inputs.end() ? current->impl.isValid() : false,
        current != inputs.end() ? current->impl.isLast() : true,
        recieved_all_blocks);

    if (recieved_all_blocks)
        return true;

    if (isLast())
        return false;

    assert(current->impl.isValid() && inputs.back().impl.isValid());

    return compareCursors(
        current->impl, current->impl.getRow(),
        inputs.back().impl, inputs.back().impl.rows - 1) != 0;
}

size_t FullMergeJoinCursor::nextDistinct()
{
    while (current != inputs.end() && !current->impl.isValid())
        current++;

    if (recieved_all_blocks)
    {
        if (!isValid())
            return 0;
        return DB::nextDistinct(current->impl);
    }

    if (!haveAllCurrentRange())
        return 0;

    size_t skipped_rows_in_blocks = 0;

    while (compareCursors(current->impl, current->impl.getRow(),
                          current->impl, current->impl.rows - 1) == 0)
    {
        skipped_rows_in_blocks += current->input.getNumRows();
        current++;
    }

    assert(isValid());

    size_t skipped_rows = DB::nextDistinct(current->impl);
    return skipped_rows + skipped_rows_in_blocks;
}

Chunk FullMergeJoinCursor::detachCurrentChunk()
{
    if (!isValid())
        throw DB::Exception("Cursor is not valid", ErrorCodes::LOGICAL_ERROR);
    Chunk res = std::move(current->input);
    current++;
    dropBlocksUntilCurrent();
    return res;
}

const ColumnRawPtrs & FullMergeJoinCursor::getSortColumns() const
{
    return current->impl.sort_columns;
}


void FullMergeJoinCursor::dropBlocksUntilCurrent()
{
    inputs.erase(inputs.begin(), current);
}

bool FullMergeJoinCursor::isValid() const
{
    return current != inputs.end() &&
        (current != std::prev(inputs.end()) || current->impl.isValid());
}

bool FullMergeJoinCursor::isLast() const
{
    return current == inputs.end() ||
        (current == std::prev(inputs.end()) && current->impl.isLast());
}

bool FullMergeJoinCursor::fullyCompleted() const
{
    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} end: {} currentIsValid: {} allIsValid {}, result {}", __FILE__, __LINE__,
        current == inputs.end(),
        current != inputs.end() ? current->impl.isValid() : false,
        isValid(),
        !isValid() && recieved_all_blocks
    );
    return !isValid() && recieved_all_blocks;
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


static void copyColumnsResized(const Chunk & chunk, size_t start, size_t size, Chunk & result_chunk)
{
    const auto & cols = chunk.getColumns();
    for (const auto & col : cols)
    {
        if (!start || start > col->size())
            result_chunk.addColumn(col->cloneResized(size));
        else
        {
            assert(size <= col->size());
            result_chunk.addColumn(col->cut(start, size));
        }
    }
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

    LOG_DEBUG(log, "TODO: remove. XXXX Consume from {} chunk: {}", source_num, bool(input.chunk));


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

template <JoinKind kind>
static std::optional<size_t> anyJoinImpl(SortCursorImpl & left_cursor, SortCursorImpl & right_cursor, PaddedPODArray<UInt64> & left_map, PaddedPODArray<UInt64> & right_map)
{
    static_assert(kind == JoinKind::Left || kind == JoinKind::Right || kind == JoinKind::Inner, "Invalid join kind");

    size_t num_rows = kind == JoinKind::Left ? left_cursor.rowsLeft() :
                      kind == JoinKind::Right ? right_cursor.rowsLeft() :
                      std::min(left_cursor.rowsLeft(), right_cursor.rowsLeft());

    constexpr bool is_left_or_inner = kind == JoinKind::Left || kind == JoinKind::Inner;
    constexpr bool is_right_or_inner = kind == JoinKind::Right || kind == JoinKind::Inner;

    if constexpr (is_left_or_inner)
        right_map.reserve(num_rows);

    if constexpr (is_right_or_inner)
        left_map.reserve(num_rows);

    while (left_cursor.isValid() && right_cursor.isValid())
    {
        int cmp = compareCursors(left_cursor, right_cursor);
        if (cmp == 0)
        {
            if constexpr (is_left_or_inner)
                right_map.emplace_back(right_cursor.getRow());

            if constexpr (is_right_or_inner)
                left_map.emplace_back(left_cursor.getRow());

            if constexpr (is_left_or_inner)
                left_cursor.next();

            if constexpr (is_right_or_inner)
                right_cursor.next();

        }
        else if (cmp < 0)
        {
            size_t num = nextDistinct(left_cursor);
            if (num == 0)
                return 0;

            if constexpr (kind == JoinKind::Left)
                right_map.resize_fill(right_map.size() + num, right_cursor.rows);
        }
        else
        {
            size_t num = nextDistinct(right_cursor);
            if (num == 0)
                return 1;

            if constexpr (kind == JoinKind::Right)
                left_map.resize_fill(left_map.size() + num, left_cursor.rows);
        }
    }

    return std::nullopt;
}

static std::optional<size_t> anyJoinDispatch(const std::vector<FullMergeJoinCursorPtr> & cursors, JoinKind kind, PaddedPODArray<UInt64> & left_map, PaddedPODArray<UInt64> & right_map)
{
    auto & left_cursor = cursors[0]->getCurrentMutable();
    auto & right_cursor = cursors[1]->getCurrentMutable();
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

static bool isFinished(const std::vector<FullMergeJoinCursorPtr> & cursors, JoinKind kind)
{
    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} {} - {} {}", __FILE__, __LINE__,
        kind,
        cursors[0]->fullyCompleted(), cursors[1]->fullyCompleted()
    );
    return (cursors[0]->fullyCompleted() && cursors[1]->fullyCompleted())
        || ((isLeft(kind) || isInner(kind)) && cursors[0]->fullyCompleted())
        || ((isRight(kind) || isInner(kind)) && cursors[1]->fullyCompleted());
}

Chunk MergeJoinAlgorithm::anyJoin(JoinKind kind)
{
    auto left_map = ColumnUInt64::create();
    auto right_map = ColumnUInt64::create();
    size_t prev_pos[] = {cursors[0]->getCurrent().impl.getRow(), cursors[1]->getCurrent().impl.getRow()};

    required_input = anyJoinDispatch(cursors, kind, left_map->getData(), right_map->getData());

    assert(left_map->empty() || right_map->empty() || left_map->size() == right_map->size());

    {
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} [{}] [{}]", __FILE__, __LINE__,
            fmt::join(left_map->getData(), ", "),
            fmt::join(right_map->getData(), ", ")
        );
    }
    Chunk result;
    size_t num_result_rows = std::max(left_map->size(), right_map->size());
    addIndexColumn(cursors[0]->getCurrent().input.getColumns(), *left_map, result, prev_pos[0], num_result_rows);
    addIndexColumn(cursors[1]->getCurrent().input.getColumns(), *right_map, result, prev_pos[1], num_result_rows);

    if (required_input != 0)
        cursors[0]->dropBlocksUntilCurrent();

    if (required_input != 1)
        cursors[1]->dropBlocksUntilCurrent();

    return result;
}

IMergingAlgorithm::Status MergeJoinAlgorithm::merge()
{
    LOG_DEBUG(log, "TODO: remove. XXXX Merge");

    if (required_input.has_value())
    {
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}", __FILE__, __LINE__);
        size_t r = required_input.value();
        required_input = {};
        return Status(r);
    }

    if (!cursors[0]->haveAllCurrentRange() && !cursors[0]->fullyCompleted())
    {
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}", __FILE__, __LINE__);
        return Status(0);
    }

    if (!cursors[1]->haveAllCurrentRange() && !cursors[1]->fullyCompleted())
    {
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}", __FILE__, __LINE__);
        return Status(1);
    }

    auto kind = table_join->getTableJoin().kind();
    auto strictness = table_join->getTableJoin().strictness();

    if (isFinished(cursors, kind))
    {
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}", __FILE__, __LINE__);
        return Status({}, true);
    }

    if (cursors[0]->fullyCompleted() && isRightOrFull(kind))
    {
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}", __FILE__, __LINE__);

        Chunk result = createBlockWithDefaults(sample_chunks[0], *cursors[1]);
        return Status(std::move(result));
    }

    if (isLeftOrFull(kind) && cursors[1]->fullyCompleted())
    {
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}", __FILE__, __LINE__);

        Chunk result = createBlockWithDefaults(*cursors[0], sample_chunks[1]);
        return Status(std::move(result));
    }

    assert(!cursors[0]->fullyCompleted() && cursors[0]->isValid() &&
           !cursors[1]->fullyCompleted() && cursors[1]->isValid());

    if (int cmp = totallyCompare(*cursors[0], *cursors[1]); cmp != 0)
    {
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}", __FILE__, __LINE__);
        if (cmp < 0)
        {
            if (isLeftOrFull(kind))
                return Status(createBlockWithDefaults(*cursors[0], sample_chunks[1]));
            cursors[0]->detachCurrentChunk();
            return Status(0);
        }

        if (cmp > 0)
        {
            if (isRightOrFull(kind))
                return Status(createBlockWithDefaults(sample_chunks[0], *cursors[1]));
            cursors[1]->detachCurrentChunk();
            return Status(1);
        }
    }

    if (strictness == ASTTableJoin::Strictness::Any)
    {
        Chunk result = anyJoin(kind);
        return Status(std::move(result), isFinished(cursors, kind));
    }

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
