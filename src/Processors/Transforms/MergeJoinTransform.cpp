#include <cstddef>
#include <memory>
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

/// If on_pos == true, compare two columns at specified positions.
/// Otherwise, compare two columns at the current positions, `lpos` and `rpos` are ignored.
template <typename Cursor, bool on_pos = false>
int ALWAYS_INLINE compareCursors(const Cursor & lhs, const Cursor & rhs,
                                 [[ maybe_unused ]] size_t lpos = 0,
                                 [[ maybe_unused ]] size_t rpos = 0)
{
    for (size_t i = 0; i < lhs->sort_columns_size; ++i)
    {
        const auto & desc = lhs->desc[i];
        int direction = desc.direction;
        int nulls_direction = desc.nulls_direction;

        int cmp = direction * nullableCompareAt<true, true>(
            *lhs->sort_columns[i],
            *rhs->sort_columns[i],
            on_pos ? lpos : lhs->getRow(),
            on_pos ? rpos : rhs->getRow(),
            nulls_direction);
        if (cmp != 0)
            return cmp;
    }
    return 0;
}

bool ALWAYS_INLINE totallyLess(const FullMergeJoinCursor & lhs, const FullMergeJoinCursor & rhs)
{
    if (lhs->rows == 0 || rhs->rows == 0)
        return false;

    if (!lhs->isValid() || !rhs->isValid())
        return false;

    /// The last row of this cursor is no larger than the first row of the another cursor.
    int cmp = compareCursors<FullMergeJoinCursor, true>(lhs, rhs, lhs->rows - 1, 0);
    return cmp < 0;
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

    if (!input.chunk)
        cursors[source_num].completeAll();

    prepareChunk(input.chunk);

    if (input.chunk.getNumRows() >= EMPTY_VALUE)
        throw Exception("Too many rows in input", ErrorCodes::TOO_MANY_ROWS);

    cursors[source_num].setInput(std::move(input));
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

static Chunk createBlockWithDefaults(const Chunk & lhs, const Chunk & rhs)
{
    Chunk result;
    size_t num_rows = std::max(lhs.getNumRows(), rhs.getNumRows());
    createSampleChunk(lhs, result, num_rows);
    createSampleChunk(rhs, result, num_rows);
    return result;
}

IMergingAlgorithm::Status MergeJoinAlgorithm::merge()
{
    if (!cursors[0].isValid() && !cursors[0].fullyCompleted())
        return Status(0);

    if (!cursors[1].isValid() && !cursors[1].fullyCompleted())
        return Status(1);

    JoinKind kind = table_join->getTableJoin().kind();

    if (cursors[0].fullyCompleted() && cursors[1].fullyCompleted())
    {
        return Status({}, true);
    }

    if (isInner(kind) && (cursors[0].fullyCompleted() || cursors[1].fullyCompleted()))
    {
        LOG_DEBUG(log, "{}:{} ", __FILE__, __LINE__);
        return Status({}, true);
    }

    if (cursors[0].fullyCompleted() && isRightOrFull(kind))
    {
        Chunk result = createBlockWithDefaults(sample_chunks[0], cursors[1].moveCurrentChunk());
        return Status(std::move(result));
    }

    if (isLeftOrFull(kind) && cursors[1].fullyCompleted())
    {
        Chunk result = createBlockWithDefaults(cursors[0].moveCurrentChunk(), sample_chunks[1]);
        return Status(std::move(result));
    }

    if (!cursors[0]->isValid() || totallyLess(cursors[0], cursors[1]))
    {
        if (cursors[0]->isValid() && isLeft(kind))
        {
            Chunk result = createBlockWithDefaults(cursors[0].moveCurrentChunk(), sample_chunks[1]);
            return Status(std::move(result), false);
        }
        cursors[0].moveCurrentChunk();
        if (cursors[0].fullyCompleted())
            return Status({}, true);
        return Status(0);
    }

    // if (!cursors[1]->isValid() || totallyLess(cursors[1], cursors[0]))
    // ...

    auto left_map = ColumnUInt64::create();
    auto right_map = ColumnUInt64::create();
    if (isInner(kind))
    {
        leftOrFullAny<JoinKind::Inner>(cursors[0].getCursor(), cursors[1].getCursor(), left_map->getData(), right_map->getData());
    }
    else if (isLeft(kind))
    {
        leftOrFullAny<JoinKind::Left>(cursors[0].getCursor(), cursors[1].getCursor(), left_map->getData(), right_map->getData());
    }
    else if (isRight(kind))
    {
        leftOrFullAny<JoinKind::Right>(cursors[0].getCursor(), cursors[1].getCursor(), left_map->getData(), right_map->getData());
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported join kind: \"{}\"", table_join->getTableJoin().kind());
    }

    Chunk result;
    addIndexColumn(cursors[0].getCurrentChunk().getColumns(), *left_map, result);
    addIndexColumn(cursors[1].getCurrentChunk().getColumns(), *right_map, result);

    return Status(std::move(result), cursors[0].fullyCompleted() && cursors[1].fullyCompleted());
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
