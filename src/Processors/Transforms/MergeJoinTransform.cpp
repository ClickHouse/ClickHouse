#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

#include <base/defines.h>
#include <base/types.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/SortCursor.h>
#include <Core/SortDescription.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/TableJoin.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/MergeJoinTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{

FullMergeJoinCursorPtr createCursor(const Block & block, const Names & columns, JoinStrictness strictness)
{
    SortDescription desc;
    desc.reserve(columns.size());
    for (const auto & name : columns)
        desc.emplace_back(name);
    return std::make_unique<FullMergeJoinCursor>(block, desc, strictness == JoinStrictness::Asof);
}

bool ALWAYS_INLINE isNullAt(const IColumn & column, size_t row)
{
    if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(&column))
        return nullable_column->isNullAt(row);
    return false;
}

template <bool has_left_nulls, bool has_right_nulls>
int nullableCompareAt(const IColumn & left_column, const IColumn & right_column, size_t lhs_pos, size_t rhs_pos, int null_direction_hint)
{
    if constexpr (has_left_nulls && has_right_nulls)
    {
        const auto * left_nullable = checkAndGetColumn<ColumnNullable>(&left_column);
        const auto * right_nullable = checkAndGetColumn<ColumnNullable>(&right_column);

        if (left_nullable && right_nullable)
        {
            int res = left_nullable->compareAt(lhs_pos, rhs_pos, right_column, null_direction_hint);
            if (res != 0)
                return res;

            /// NULL != NULL case
            if (left_nullable->isNullAt(lhs_pos))
                return null_direction_hint;

            return 0;
        }
    }

    if constexpr (has_left_nulls)
    {
        if (const auto * left_nullable = checkAndGetColumn<ColumnNullable>(&left_column))
        {
            if (left_nullable->isNullAt(lhs_pos))
                return null_direction_hint;
            return left_nullable->getNestedColumn().compareAt(lhs_pos, rhs_pos, right_column, null_direction_hint);
        }
    }

    if constexpr (has_right_nulls)
    {
        if (const auto * right_nullable = checkAndGetColumn<ColumnNullable>(&right_column))
        {
            if (right_nullable->isNullAt(rhs_pos))
                return -null_direction_hint;
            return left_column.compareAt(lhs_pos, rhs_pos, right_nullable->getNestedColumn(), null_direction_hint);
        }
    }

    return left_column.compareAt(lhs_pos, rhs_pos, right_column, null_direction_hint);
}

int ALWAYS_INLINE compareCursors(const SortCursorImpl & lhs, size_t lpos,
                                 const SortCursorImpl & rhs, size_t rpos,
                                 size_t key_length,
                                 int null_direction_hint)
{
    for (size_t i = 0; i < key_length; ++i)
    {
        /// TODO(@vdimir): use nullableCompareAt only if there's nullable columns
        int cmp = nullableCompareAt<true, true>(*lhs.sort_columns[i], *rhs.sort_columns[i], lpos, rpos, null_direction_hint);
        if (cmp != 0)
            return cmp;
    }
    return 0;
}

int ALWAYS_INLINE compareCursors(const SortCursorImpl & lhs, const SortCursorImpl & rhs, int null_direction_hint)
{
    return compareCursors(lhs, lhs.getRow(), rhs, rhs.getRow(), lhs.sort_columns_size, null_direction_hint);
}

int compareAsofCursors(const FullMergeJoinCursor & lhs, const FullMergeJoinCursor & rhs, int null_direction_hint)
{
    return nullableCompareAt<true, true>(*lhs.getAsofColumn(), *rhs.getAsofColumn(), lhs->getRow(), rhs->getRow(), null_direction_hint);
}

bool ALWAYS_INLINE totallyLess(SortCursorImpl & lhs, SortCursorImpl & rhs, int null_direction_hint)
{
    /// The last row of left cursor is less than the current row of the right cursor.
    int cmp = compareCursors(lhs, lhs.rows - 1, rhs, rhs.getRow(), lhs.sort_columns_size, null_direction_hint);
    return cmp < 0;
}

int ALWAYS_INLINE totallyCompare(SortCursorImpl & lhs, SortCursorImpl & rhs, int null_direction_hint)
{
    if (totallyLess(lhs, rhs, null_direction_hint))
        return -1;
    if (totallyLess(rhs, lhs, null_direction_hint))
        return 1;
    return 0;
}

ColumnPtr indexColumn(const ColumnPtr & column, const PaddedPODArray<UInt64> & indices)
{
    auto new_col = column->cloneEmpty();
    new_col->reserve(indices.size());
    for (size_t idx : indices)
    {
        /// rows where default value should be inserted have index == size
        if (idx < column->size())
            new_col->insertFrom(*column, idx);
        else
            new_col->insertDefault();
    }
    return new_col;
}

Columns indexColumns(const Columns & columns, const PaddedPODArray<UInt64> & indices)
{
    Columns new_columns;
    new_columns.reserve(columns.size());
    for (const auto & column : columns)
    {
        new_columns.emplace_back(indexColumn(column, indices));
    }
    return new_columns;
}

bool sameNext(const SortCursorImpl & impl, std::optional<size_t> pos_opt = {})
{
    size_t pos = pos_opt.value_or(impl.getRow());
    for (size_t i = 0; i < impl.sort_columns_size; ++i)
    {
        const auto & col = *impl.sort_columns[i];
        if (auto cmp = col.compareAt(pos, pos + 1, col, impl.desc[i].nulls_direction); cmp != 0)
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

ColumnPtr replicateRow(const IColumn & column, size_t num)
{
    MutableColumnPtr res = column.cloneEmpty();
    res->insertManyFrom(column, 0, num);
    return res;
}

template <typename TColumns>
void copyColumnsResized(const TColumns & cols, size_t start, size_t size, Chunk & result_chunk)
{
    for (const auto & col : cols)
    {
        if (col->empty())
        {
            /// add defaults
            result_chunk.addColumn(col->cloneResized(size));
        }
        else if (col->size() == 1)
        {
            /// copy same row n times
            result_chunk.addColumn(replicateRow(*col, size));
        }
        else
        {
            /// cut column
            assert(start + size <= col->size());
            result_chunk.addColumn(col->cut(start, size));
        }
    }
}

Chunk copyChunkResized(const Chunk & lhs, const Chunk & rhs, size_t start, size_t num_rows)
{
    Chunk result;
    copyColumnsResized(lhs.getColumns(), start, num_rows, result);
    copyColumnsResized(rhs.getColumns(), start, num_rows, result);
    return result;
}

Chunk getRowFromChunk(const Chunk & chunk, size_t pos)
{
    Chunk result;
    copyColumnsResized(chunk.getColumns(), pos, 1, result);
    return result;
}

void inline addRange(PaddedPODArray<UInt64> & values, UInt64 start, UInt64 end)
{
    assert(end > start);
    for (UInt64 i = start; i < end; ++i)
        values.push_back(i);
}

void inline addMany(PaddedPODArray<UInt64> & values, UInt64 value, size_t num)
{
    values.resize_fill(values.size() + num, value);
}
}

JoinKeyRow::JoinKeyRow(const FullMergeJoinCursor & cursor, size_t pos)
{
    row.reserve(cursor->sort_columns.size());
    for (const auto & col : cursor->sort_columns)
    {
        auto new_col = col->cloneEmpty();
        new_col->insertFrom(*col, pos);
        row.push_back(std::move(new_col));
    }
    if (const IColumn * asof_column = cursor.getAsofColumn())
    {
        if (const auto * nullable_asof_column = checkAndGetColumn<ColumnNullable>(asof_column))
        {
            /// We save matched column, and since NULL do not match anything, we can't use it as a key
            chassert(!nullable_asof_column->isNullAt(pos));
            asof_column = nullable_asof_column->getNestedColumnPtr().get();
        }
        auto new_col = asof_column->cloneEmpty();
        new_col->insertFrom(*asof_column, pos);
        row.push_back(std::move(new_col));
    }
}

void JoinKeyRow::reset()
{
    row.clear();
}

bool JoinKeyRow::equals(const FullMergeJoinCursor & cursor) const
{
    if (row.empty())
        return false;

    for (size_t i = 0; i < cursor->sort_columns_size; ++i)
    {
        // int cmp = this->row[i]->compareAt(0, cursor->getRow(), *(cursor->sort_columns[i]), cursor->desc[i].nulls_direction);
        int cmp = nullableCompareAt<true, true>(*this->row[i], *cursor->sort_columns[i], 0, cursor->getRow(), cursor->desc[i].nulls_direction);
        if (cmp != 0)
            return false;
    }
    return true;
}

bool JoinKeyRow::asofMatch(const FullMergeJoinCursor & cursor, ASOFJoinInequality asof_inequality) const
{
    chassert(this->row.size() == cursor->sort_columns_size + 1);
    if (!equals(cursor))
        return false;

    const auto & asof_row = row.back();
    if (isNullAt(*asof_row, 0) || isNullAt(*cursor.getAsofColumn(), cursor->getRow()))
        return false;

    int cmp = 0;
    if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(cursor.getAsofColumn()))
        cmp = nullable_column->getNestedColumn().compareAt(cursor->getRow(), 0, *asof_row, 1);
    else
        cmp = cursor.getAsofColumn()->compareAt(cursor->getRow(), 0, *asof_row, 1);

    return (asof_inequality == ASOFJoinInequality::Less && cmp < 0)
        || (asof_inequality == ASOFJoinInequality::LessOrEquals && cmp <= 0)
        || (asof_inequality == ASOFJoinInequality::Greater && cmp > 0)
        || (asof_inequality == ASOFJoinInequality::GreaterOrEquals && cmp >= 0);
}

void AnyJoinState::set(size_t source_num, const FullMergeJoinCursor & cursor)
{
    assert(cursor->rows);
    keys[source_num] = JoinKeyRow(cursor, cursor->rows - 1);
}

void AnyJoinState::reset(size_t source_num)
{
    keys[source_num].reset();
    value.clear();
}

void AnyJoinState::setValue(Chunk value_)
{
    value = std::move(value_);
}

bool AnyJoinState::empty() const { return keys[0].row.empty() && keys[1].row.empty(); }


void AsofJoinState::set(const FullMergeJoinCursor & rcursor, size_t rpos)
{
    key = JoinKeyRow(rcursor, rpos);
    value = rcursor.getCurrent().clone();
    value_row = rpos;
}

void AsofJoinState::reset()
{
    key.reset();
    value.clear();
}

FullMergeJoinCursor::FullMergeJoinCursor(const Block & sample_block_, const SortDescription & description_, bool is_asof)
    : sample_block(materializeBlock(sample_block_).cloneEmpty())
    , desc(description_)
{
    if (desc.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty sort description for FullMergeJoinCursor");

    if (is_asof)
    {
        /// For ASOF join prefix of sort description is used for equality comparison
        /// and the last column is used for inequality comparison and is handled separately

        auto asof_column_description = desc.back();
        desc.pop_back();

        chassert(asof_column_description.direction == 1 && asof_column_description.nulls_direction == 1);
        asof_column_position = sample_block.getPositionByName(asof_column_description.column_name);
    }
}

const Chunk & FullMergeJoinCursor::getCurrent() const
{
    return current_chunk;
}

Chunk FullMergeJoinCursor::detach()
{
    cursor = SortCursorImpl();
    return std::move(current_chunk);
}

void FullMergeJoinCursor::setChunk(Chunk && chunk)
{
    assert(!recieved_all_blocks);
    assert(!cursor.isValid());

    if (!chunk)
    {
        recieved_all_blocks = true;
        detach();
        return;
    }

    // should match the structure of sample_block (after materialization)
    convertToFullIfConst(chunk);
    convertToFullIfSparse(chunk);

    current_chunk = std::move(chunk);
    cursor = SortCursorImpl(sample_block, current_chunk.getColumns(), desc);
}

bool FullMergeJoinCursor::fullyCompleted() const
{
    return !cursor.isValid() && recieved_all_blocks;
}

String FullMergeJoinCursor::dump() const
{
    Strings row_dump;
    if (cursor.isValid())
    {
        Field val;
        for (size_t i = 0; i < cursor.sort_columns_size; ++i)
        {
            cursor.sort_columns[i]->get(cursor.getRow(), val);
            row_dump.push_back(val.dump());
        }

        if (const auto * asof_column = getAsofColumn())
        {
            asof_column->get(cursor.getRow(), val);
            row_dump.push_back(val.dump());
        }
    }

    return fmt::format("<{}/{}{}>[{}]",
        cursor.getRow(), cursor.rows,
        recieved_all_blocks ? "(finished)" : "",
        fmt::join(row_dump, ", "));
}

MergeJoinAlgorithm::MergeJoinAlgorithm(
    JoinKind kind_,
    JoinStrictness strictness_,
    const TableJoin::JoinOnClause & on_clause_,
    const Blocks & input_headers,
    size_t max_block_size_)
    : kind(kind_)
    , strictness(strictness_)
    , max_block_size(max_block_size_)
    , log(getLogger("MergeJoinAlgorithm"))
{
    if (input_headers.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeJoinAlgorithm requires exactly two inputs");

    if (strictness != JoinStrictness::Any && strictness != JoinStrictness::All && strictness != JoinStrictness::Asof)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeJoinAlgorithm is not implemented for strictness {}", strictness);

    if (strictness == JoinStrictness::Asof)
    {
        if (kind != JoinKind::Left && kind != JoinKind::Inner)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeJoinAlgorithm does not implement ASOF {} join", kind);
    }

    if (!isInner(kind) && !isLeft(kind) && !isRight(kind) && !isFull(kind))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeJoinAlgorithm is not implemented for kind {}", kind);

    if (on_clause_.on_filter_condition_left || on_clause_.on_filter_condition_right)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeJoinAlgorithm does not support ON filter conditions");

    cursors = {
        createCursor(input_headers[0], on_clause_.key_names_left, strictness),
        createCursor(input_headers[1], on_clause_.key_names_right, strictness),
    };
}

MergeJoinAlgorithm::MergeJoinAlgorithm(
    JoinPtr join_ptr,
    const Blocks & input_headers,
    size_t max_block_size_)
    : MergeJoinAlgorithm(
        join_ptr->getTableJoin().kind(),
        join_ptr->getTableJoin().strictness(),
        join_ptr->getTableJoin().getOnlyClause(),
        input_headers,
        max_block_size_)
{
    for (const auto & [left_key, right_key] : join_ptr->getTableJoin().leftToRightKeyRemap())
    {
        size_t left_idx = input_headers[0].getPositionByName(left_key);
        size_t right_idx = input_headers[1].getPositionByName(right_key);
        left_to_right_key_remap[left_idx] = right_idx;
    }

    const auto *smjPtr = typeid_cast<const FullSortingMergeJoin *>(join_ptr.get());
    if (smjPtr)
    {
        null_direction_hint = smjPtr->getNullDirection();
    }

    if (strictness == JoinStrictness::Asof)
        setAsofInequality(join_ptr->getTableJoin().getAsofInequality());
}

void MergeJoinAlgorithm::setAsofInequality(ASOFJoinInequality asof_inequality_)
{
    if (strictness != JoinStrictness::Asof)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "setAsofInequality is only supported for ASOF joins");

    if (asof_inequality_ == ASOFJoinInequality::None)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ASOF inequality cannot be None");

    asof_inequality = asof_inequality_;
}

void MergeJoinAlgorithm::logElapsed(double seconds)
{
    LOG_TRACE(log,
        "Finished pocessing in {} seconds"
        ", left: {} blocks, {} rows; right: {} blocks, {} rows"
        ", max blocks loaded to memory: {}",
        seconds, stat.num_blocks[0], stat.num_rows[0], stat.num_blocks[1], stat.num_rows[1],
        stat.max_blocks_loaded);
}

IMergingAlgorithm::MergedStats MergeJoinAlgorithm::getMergedStats() const
{
    return
    {
        .bytes = stat.num_bytes[0] + stat.num_bytes[1],
        .rows = stat.num_rows[0] + stat.num_rows[1],
        .blocks = stat.num_blocks[0] + stat.num_blocks[1],
    };
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

void MergeJoinAlgorithm::initialize(Inputs inputs)
{
    if (inputs.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Two inputs are required, got {}", inputs.size());

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        consume(inputs[i], i);
    }
}

void MergeJoinAlgorithm::consume(Input & input, size_t source_num)
{
    if (input.skip_last_row)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "skip_last_row is not supported");

    if (input.permutation)
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "permutation is not supported");

    if (input.chunk)
    {
        stat.num_blocks[source_num] += 1;
        stat.num_rows[source_num] += input.chunk.getNumRows();
        stat.num_bytes[source_num] += input.chunk.allocatedBytes();
    }

    prepareChunk(input.chunk);
    cursors[source_num]->setChunk(std::move(input.chunk));
}

template <JoinKind kind>
struct AllJoinImpl
{
    constexpr static bool enabled = isInner(kind) || isLeft(kind) || isRight(kind) || isFull(kind);

    static void join(FullMergeJoinCursor & left_cursor,
                     FullMergeJoinCursor & right_cursor,
                     size_t max_block_size,
                     PaddedPODArray<UInt64> & left_map,
                     PaddedPODArray<UInt64> & right_map,
                     std::unique_ptr<AllJoinState> & state,
                     int null_direction_hint)
    {
        right_map.clear();
        right_map.reserve(max_block_size);
        left_map.clear();
        left_map.reserve(max_block_size);

        size_t rpos = std::numeric_limits<size_t>::max();
        size_t lpos = std::numeric_limits<size_t>::max();
        int cmp = 0;
        assert(left_cursor->isValid() && right_cursor->isValid());
        while (left_cursor->isValid() && right_cursor->isValid())
        {
            lpos = left_cursor->getRow();
            rpos = right_cursor->getRow();

            cmp = compareCursors(left_cursor.cursor, right_cursor.cursor, null_direction_hint);
            if (cmp == 0)
            {
                size_t lnum = nextDistinct(left_cursor.cursor);
                size_t rnum = nextDistinct(right_cursor.cursor);

                bool all_fit_in_block = !max_block_size || std::max(left_map.size(), right_map.size()) + lnum * rnum <= max_block_size;
                bool have_all_ranges = left_cursor.cursor.isValid() && right_cursor.cursor.isValid();
                if (all_fit_in_block && have_all_ranges)
                {
                    /// fast path if all joined rows fit in one block
                    for (size_t i = 0; i < rnum; ++i)
                    {
                        addRange(left_map, lpos, left_cursor.cursor.getRow());
                        addMany(right_map, rpos + i, lnum);
                    }
                }
                else
                {
                    assert(state == nullptr);
                    state = std::make_unique<AllJoinState>(left_cursor, lpos, right_cursor, rpos);
                    state->addRange(0, left_cursor.getCurrent().clone(), lpos, lnum);
                    state->addRange(1, right_cursor.getCurrent().clone(), rpos, rnum);
                    return;
                }
            }
            else if (cmp < 0)
            {
                size_t num = nextDistinct(left_cursor.cursor);
                if constexpr (isLeftOrFull(kind))
                {
                    right_map.resize_fill(right_map.size() + num, right_cursor->rows);
                    for (size_t i = lpos; i < left_cursor->getRow(); ++i)
                        left_map.push_back(i);
                }
            }
            else
            {
                size_t num = nextDistinct(right_cursor.cursor);
                if constexpr (isRightOrFull(kind))
                {
                    left_map.resize_fill(left_map.size() + num, left_cursor->rows);
                    for (size_t i = rpos; i < right_cursor->getRow(); ++i)
                        right_map.push_back(i);
                }
            }
        }
    }
};

template <template<JoinKind> class Impl, typename ... Args>
void dispatchKind(JoinKind kind, Args && ... args)
{
    if (Impl<JoinKind::Inner>::enabled && kind == JoinKind::Inner)
        return Impl<JoinKind::Inner>::join(std::forward<Args>(args)...);
    if (Impl<JoinKind::Left>::enabled && kind == JoinKind::Left)
        return Impl<JoinKind::Left>::join(std::forward<Args>(args)...);
    if (Impl<JoinKind::Right>::enabled && kind == JoinKind::Right)
        return Impl<JoinKind::Right>::join(std::forward<Args>(args)...);
    if (Impl<JoinKind::Full>::enabled && kind == JoinKind::Full)
        return Impl<JoinKind::Full>::join(std::forward<Args>(args)...);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported join kind: \"{}\"", kind);
}

MutableColumns MergeJoinAlgorithm::getEmptyResultColumns() const
{
    MutableColumns result_cols;
    for (size_t i = 0; i < 2; ++i)
    {
        for (const auto & col : cursors[i]->sampleColumns())
            result_cols.push_back(col->cloneEmpty());
    }
    return result_cols;
}

std::optional<MergeJoinAlgorithm::Status> MergeJoinAlgorithm::handleAllJoinState()
{
    if (all_join_state && all_join_state->finished())
    {
        all_join_state.reset();
    }

    if (all_join_state)
    {
        assert(cursors.size() == 2);
        /// Accumulate blocks with same key in all_join_state
        for (size_t i = 0; i < 2; ++i)
        {
            if (cursors[i]->cursor.isValid() && all_join_state->keys[i].equals(*cursors[i]))
            {
                size_t pos = cursors[i]->cursor.getRow();
                size_t num = nextDistinct(cursors[i]->cursor);
                all_join_state->addRange(i, cursors[i]->getCurrent().clone(), pos, num);
            }
        }

        for (size_t i = 0; i < cursors.size(); ++i)
        {
            if (!cursors[i]->cursor.isValid() && !cursors[i]->fullyCompleted())
            {
                return Status(i);
            }
        }
        /// If current position is valid, then we've found new key, can join accumulated data

        stat.max_blocks_loaded = std::max(stat.max_blocks_loaded, all_join_state->blocksStored());

        /// join all rows with current key
        MutableColumns result_cols = getEmptyResultColumns();

        size_t total_rows = 0;
        while (!max_block_size || total_rows < max_block_size)
        {
            const auto & left_range = all_join_state->getLeft();
            const auto & right_range = all_join_state->getRight();

            total_rows += left_range.length;

            size_t i = 0;
            /// Copy left block
            for (const auto & col : left_range.chunk.getColumns())
                result_cols[i++]->insertRangeFrom(*col, left_range.begin, left_range.length);
            /// And replicate current right column
            for (const auto & col : right_range.chunk.getColumns())
                result_cols[i++]->insertManyFrom(*col, right_range.current, left_range.length);
            bool valid = all_join_state->next();
            if (!valid)
                break;
        }

        if (total_rows)
            return Status(Chunk(std::move(result_cols), total_rows));
    }
    return {};
}

std::optional<MergeJoinAlgorithm::Status> MergeJoinAlgorithm::handleAsofJoinState()
{
    if (strictness != JoinStrictness::Asof)
        return {};

    if (!cursors[1]->fullyCompleted())
        return {};

    auto & left_cursor = *cursors[0];
    const auto & left_columns = left_cursor.getCurrent().getColumns();

    MutableColumns result_cols = getEmptyResultColumns();

    while (left_cursor->isValid() && asof_join_state.hasMatch(left_cursor, asof_inequality))
    {
        size_t i = 0;
        for (const auto & col : left_columns)
            result_cols[i++]->insertFrom(*col, left_cursor->getRow());
        for (const auto & col : asof_join_state.value.getColumns())
            result_cols[i++]->insertFrom(*col, asof_join_state.value_row);
        chassert(i == result_cols.size());
        left_cursor->next();
    }

    while (isLeft(kind) && left_cursor->isValid())
    {
        /// return row with default values at right side
        size_t i = 0;
        for (const auto & col : left_columns)
            result_cols[i++]->insertFrom(*col, left_cursor->getRow());
        for (; i < result_cols.size(); ++i)
            result_cols[i]->insertDefault();
        chassert(i == result_cols.size());

        left_cursor->next();
    }

    size_t result_rows = result_cols.empty() ? 0 : result_cols.front()->size();
    if (result_rows)
        return Status(Chunk(std::move(result_cols), result_rows));

    return {};
}


MergeJoinAlgorithm::Status MergeJoinAlgorithm::allJoin()
{
    PaddedPODArray<UInt64> idx_map[2];

    dispatchKind<AllJoinImpl>(kind, *cursors[0], *cursors[1], max_block_size, idx_map[0], idx_map[1], all_join_state, null_direction_hint);
    assert(idx_map[0].size() == idx_map[1].size());

    Chunk result;

    Columns rcols = indexColumns(cursors[1]->getCurrent().getColumns(), idx_map[1]);
    Columns lcols;
    if (!left_to_right_key_remap.empty())
    {
        /// If we have remapped columns, then we need to get values from right columns instead of defaults
        const auto & indices = idx_map[0];

        const auto & left_src = cursors[0]->getCurrent().getColumns();
        for (size_t col_idx = 0; col_idx < left_src.size(); ++col_idx)
        {
            const auto & col = left_src[col_idx];
            auto new_col = col->cloneEmpty();
            new_col->reserve(indices.size());
            for (size_t i = 0; i < indices.size(); ++i)
            {
                if (indices[i] < col->size())
                {
                    new_col->insertFrom(*col, indices[i]);
                }
                else
                {
                    if (auto it = left_to_right_key_remap.find(col_idx); it != left_to_right_key_remap.end())
                        new_col->insertFrom(*rcols[it->second], i);
                    else
                        new_col->insertDefault();
                }
            }
            lcols.push_back(std::move(new_col));
        }
    }
    else
    {
        lcols = indexColumns(cursors[0]->getCurrent().getColumns(), idx_map[0]);
    }

    for (auto & col : lcols)
        result.addColumn(std::move(col));

    for (auto & col : rcols)
        result.addColumn(std::move(col));

    return Status(std::move(result));
}


template <JoinKind kind>
struct AnyJoinImpl
{
    constexpr static bool enabled = isInner(kind) || isLeft(kind) || isRight(kind);

    static void join(FullMergeJoinCursor & left_cursor,
                     FullMergeJoinCursor & right_cursor,
                     PaddedPODArray<UInt64> & left_map,
                     PaddedPODArray<UInt64> & right_map,
                     AnyJoinState & any_join_state,
                     int null_direction_hint)
    {
        assert(enabled);

        size_t num_rows = isLeft(kind) ? left_cursor->rowsLeft() :
                          isRight(kind) ? right_cursor->rowsLeft() :
                          std::min(left_cursor->rowsLeft(), right_cursor->rowsLeft());

        if constexpr (isLeft(kind) || isInner(kind))
            right_map.reserve(num_rows);

        if constexpr (isRight(kind) || isInner(kind))
            left_map.reserve(num_rows);

        size_t rpos = std::numeric_limits<size_t>::max();
        size_t lpos = std::numeric_limits<size_t>::max();
        assert(left_cursor->isValid() && right_cursor->isValid());
        int cmp = 0;
        while (left_cursor->isValid() && right_cursor->isValid())
        {
            lpos = left_cursor->getRow();
            rpos = right_cursor->getRow();

            cmp = compareCursors(left_cursor.cursor, right_cursor.cursor, null_direction_hint);
            if (cmp == 0)
            {
                if constexpr (isLeftOrFull(kind))
                {
                    size_t lnum = nextDistinct(left_cursor.cursor);
                    right_map.resize_fill(right_map.size() + lnum, rpos);
                }

                if constexpr (isRightOrFull(kind))
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
                if constexpr (isLeftOrFull(kind))
                    right_map.resize_fill(right_map.size() + num, right_cursor->rows);
            }
            else
            {
                size_t num = nextDistinct(right_cursor.cursor);
                if constexpr (isRightOrFull(kind))
                    left_map.resize_fill(left_map.size() + num, left_cursor->rows);
            }
        }

        /// Remember last joined row to propagate it to next block

        any_join_state.setValue({});
        if (!left_cursor->isValid())
        {
            any_join_state.set(0, left_cursor);
            if (cmp == 0 && isLeft(kind))
                any_join_state.setValue(getRowFromChunk(right_cursor.getCurrent(), rpos));
        }

        if (!right_cursor->isValid())
        {
            any_join_state.set(1, right_cursor);
            if (cmp == 0 && isRight(kind))
                any_join_state.setValue(getRowFromChunk(left_cursor.getCurrent(), lpos));
        }
    }
};

std::optional<MergeJoinAlgorithm::Status> MergeJoinAlgorithm::handleAnyJoinState()
{
    if (any_join_state.empty())
        return {};

    Chunk result;

    for (size_t source_num = 0; source_num < 2; ++source_num)
    {
        auto & current = *cursors[source_num];
        if (any_join_state.keys[source_num].equals(current))
        {
            size_t start_pos = current->getRow();
            size_t length = nextDistinct(current.cursor);

            if (length && isLeft(kind) && source_num == 0)
            {
                if (any_join_state.value)
                    result = copyChunkResized(current.getCurrent(), any_join_state.value, start_pos, length);
                else
                    result = createBlockWithDefaults(source_num, start_pos, length);
            }

            if (length && isRight(kind) && source_num == 1)
            {
                if (any_join_state.value)
                    result = copyChunkResized(any_join_state.value, current.getCurrent(), start_pos, length);
                else
                    result = createBlockWithDefaults(source_num, start_pos, length);
            }

            if (current->isValid())
                any_join_state.keys[source_num].reset();
        }
        else
        {
            any_join_state.keys[source_num].reset();
        }
    }

    if (result)
        return Status(std::move(result));
    return {};
}

MergeJoinAlgorithm::Status MergeJoinAlgorithm::anyJoin()
{
    if (auto result = handleAnyJoinState())
        return std::move(*result);

    auto & current_left = cursors[0]->cursor;
    if (!current_left.isValid())
        return Status(0);

    auto & current_right = cursors[1]->cursor;
    if (!current_right.isValid())
        return Status(1);

    /// join doesn't build result block, but returns indices where result rows should be placed
    PaddedPODArray<UInt64> idx_map[2];
    size_t prev_pos[] = {current_left.getRow(), current_right.getRow()};

    dispatchKind<AnyJoinImpl>(kind, *cursors[0], *cursors[1], idx_map[0], idx_map[1], any_join_state, null_direction_hint);

    assert(idx_map[0].empty() || idx_map[1].empty() || idx_map[0].size() == idx_map[1].size());
    size_t num_result_rows = std::max(idx_map[0].size(), idx_map[1].size());

    /// build result block from indices
    Chunk result;
    for (size_t source_num = 0; source_num < 2; ++source_num)
    {
        /// empty map means identity mapping
        if (!idx_map[source_num].empty())
        {
            for (const auto & col : cursors[source_num]->getCurrent().getColumns())
            {
                result.addColumn(indexColumn(col, idx_map[source_num]));
            }
        }
        else
        {
            for (const auto & col : cursors[source_num]->getCurrent().getColumns())
            {
                result.addColumn(col->cut(prev_pos[source_num], num_result_rows));
            }
        }
    }
    return Status(std::move(result));
}


MergeJoinAlgorithm::Status MergeJoinAlgorithm::asofJoin()
{
    auto & left_cursor = *cursors[0];
    if (!left_cursor->isValid())
        return Status(0);

    auto & right_cursor = *cursors[1];
    if (!right_cursor->isValid())
        return Status(1);

    const auto & left_columns = left_cursor.getCurrent().getColumns();
    const auto & right_columns = right_cursor.getCurrent().getColumns();

    MutableColumns result_cols = getEmptyResultColumns();

    while (left_cursor->isValid() && right_cursor->isValid())
    {
        auto lpos = left_cursor->getRow();
        auto rpos = right_cursor->getRow();
        auto cmp = compareCursors(*left_cursor, *right_cursor, null_direction_hint);
        if (cmp == 0)
        {
            if (isNullAt(*left_cursor.getAsofColumn(), lpos))
                cmp = -1;
            if (isNullAt(*right_cursor.getAsofColumn(), rpos))
                cmp = 1;
        }

        if (cmp == 0)
        {
            auto asof_cmp = compareAsofCursors(left_cursor, right_cursor, null_direction_hint);

            if ((asof_inequality == ASOFJoinInequality::Less && asof_cmp <= -1)
             || (asof_inequality == ASOFJoinInequality::LessOrEquals && asof_cmp <= 0))
            {
                /// First row in right table that is greater (or equal) than current row in left table
                /// matches asof join condition the best
                size_t i = 0;
                for (const auto & col : left_columns)
                    result_cols[i++]->insertFrom(*col, lpos);
                for (const auto & col : right_columns)
                    result_cols[i++]->insertFrom(*col, rpos);
                chassert(i == result_cols.size());

                left_cursor->next();
                continue;
            }

            if (asof_inequality == ASOFJoinInequality::Less || asof_inequality == ASOFJoinInequality::LessOrEquals)
            {
                /// Asof condition is not (yet) satisfied, skip row in right table
                right_cursor->next();
                continue;
            }

            if ((asof_inequality == ASOFJoinInequality::Greater && asof_cmp >= 1)
             || (asof_inequality == ASOFJoinInequality::GreaterOrEquals && asof_cmp >= 0))
            {
                /// condition is satisfied, remember this row and move next to try to find better match
                asof_join_state.set(right_cursor, rpos);
                right_cursor->next();
                continue;
            }

            if (asof_inequality == ASOFJoinInequality::Greater || asof_inequality == ASOFJoinInequality::GreaterOrEquals)
            {
                /// Asof condition is not satisfied anymore, use last matched row from right table
                if (asof_join_state.hasMatch(left_cursor, asof_inequality))
                {
                    size_t i = 0;
                    for (const auto & col : left_columns)
                        result_cols[i++]->insertFrom(*col, lpos);
                    for (const auto & col : asof_join_state.value.getColumns())
                        result_cols[i++]->insertFrom(*col, asof_join_state.value_row);
                    chassert(i == result_cols.size());
                }
                else
                {
                    asof_join_state.reset();
                    if (isLeft(kind))
                    {
                        /// return row with default values at right side
                        size_t i = 0;
                        for (const auto & col : left_columns)
                            result_cols[i++]->insertFrom(*col, lpos);
                        for (; i < result_cols.size(); ++i)
                            result_cols[i]->insertDefault();
                        chassert(i == result_cols.size());
                    }
                }
                left_cursor->next();
                continue;
            }

            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO: implement ASOF equality join");
        }
        if (cmp < 0)
        {
            if (asof_join_state.hasMatch(left_cursor, asof_inequality))
            {
                size_t i = 0;
                for (const auto & col : left_columns)
                    result_cols[i++]->insertFrom(*col, lpos);
                for (const auto & col : asof_join_state.value.getColumns())
                    result_cols[i++]->insertFrom(*col, asof_join_state.value_row);
                chassert(i == result_cols.size());
                left_cursor->next();
                continue;
            }

            asof_join_state.reset();


            /// no matches for rows in left table, just pass them through
            size_t num = nextDistinct(*left_cursor);

            if (isLeft(kind) && num)
            {
                /// return them with default values at right side
                size_t i = 0;
                for (const auto & col : left_columns)
                    result_cols[i++]->insertRangeFrom(*col, lpos, num);
                for (; i < result_cols.size(); ++i)
                    result_cols[i]->insertManyDefaults(num);
                chassert(i == result_cols.size());
            }
        }
        else
        {
            /// skip rows in right table until we find match for current row in left table
            nextDistinct(*right_cursor);
        }
    }
    size_t num_rows = result_cols.empty() ? 0 : result_cols.front()->size();
    return Status(Chunk(std::move(result_cols), num_rows));
}


/// if `source_num == 0` get data from left cursor and fill defaults at right
/// otherwise - vice versa
Chunk MergeJoinAlgorithm::createBlockWithDefaults(size_t source_num, size_t start, size_t num_rows) const
{

    ColumnRawPtrs cols;
    {
        const auto & columns_left = source_num == 0 ? cursors[0]->getCurrent().getColumns() : cursors[0]->sampleColumns();
        const auto & columns_right = source_num == 1 ? cursors[1]->getCurrent().getColumns() : cursors[1]->sampleColumns();

        for (size_t i = 0; i < columns_left.size(); ++i)
        {
            if (auto it = left_to_right_key_remap.find(i); source_num == 0 || it == left_to_right_key_remap.end())
            {
                cols.push_back(columns_left[i].get());
            }
            else
            {
                cols.push_back(columns_right[it->second].get());
            }
        }

        for (const auto & col : columns_right)
        {
            cols.push_back(col.get());
        }
    }
    Chunk result_chunk;
    copyColumnsResized(cols, start, num_rows, result_chunk);
    return result_chunk;
}

/// This function also flushes cursor
Chunk MergeJoinAlgorithm::createBlockWithDefaults(size_t source_num)
{
    Chunk result_chunk = createBlockWithDefaults(source_num, cursors[source_num]->cursor.getRow(), cursors[source_num]->cursor.rowsLeft());
    cursors[source_num]->detach();
    return result_chunk;
}

IMergingAlgorithm::Status MergeJoinAlgorithm::merge()
{

    if (!cursors[0]->cursor.isValid() && !cursors[0]->fullyCompleted())
        return Status(0);

    if (!cursors[1]->cursor.isValid() && !cursors[1]->fullyCompleted())
        return Status(1);

    if (auto result = handleAllJoinState())
        return std::move(*result);

    if (auto result = handleAsofJoinState())
        return std::move(*result);

    if (cursors[0]->fullyCompleted() || cursors[1]->fullyCompleted())
    {
        if (!cursors[0]->fullyCompleted() && isLeftOrFull(kind))
            return Status(createBlockWithDefaults(0));

        if (!cursors[1]->fullyCompleted() && isRightOrFull(kind))
            return Status(createBlockWithDefaults(1));

        return Status({}, true);
    }

    /// check if blocks are not intersecting at all
    if (int cmp = totallyCompare(cursors[0]->cursor, cursors[1]->cursor, null_direction_hint); cmp != 0 && strictness != JoinStrictness::Asof)
    {
        if (cmp < 0)
        {
            if (isLeftOrFull(kind))
                return Status(createBlockWithDefaults(0));
            cursors[0]->detach();
            return Status(0);
        }

        if (cmp > 0)
        {
            if (isRightOrFull(kind))
                return Status(createBlockWithDefaults(1));
            cursors[1]->detach();
            return Status(1);
        }
    }

    if (strictness == JoinStrictness::Any)
        return anyJoin();

    if (strictness == JoinStrictness::All)
        return allJoin();

    if (strictness == JoinStrictness::Asof)
        return asofJoin();

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported strictness '{}'", strictness);
}

MergeJoinTransform::MergeJoinTransform(
        JoinPtr table_join,
        const Blocks & input_headers,
        const Block & output_header,
        size_t max_block_size,
        UInt64 limit_hint_)
    : IMergingTransform<MergeJoinAlgorithm>(
        input_headers,
        output_header,
        /* have_all_inputs_= */ true,
        limit_hint_,
        /* always_read_till_end_= */ false,
        /* empty_chunk_on_finish_= */ true,
        table_join, input_headers, max_block_size)
{
}

MergeJoinTransform::MergeJoinTransform(
        JoinKind kind_,
        JoinStrictness strictness_,
        const TableJoin::JoinOnClause & on_clause_,
        const Blocks & input_headers,
        const Block & output_header,
        size_t max_block_size,
        UInt64 limit_hint_)
    : IMergingTransform<MergeJoinAlgorithm>(
        input_headers,
        output_header,
        /* have_all_inputs_= */ true,
        limit_hint_,
        /* always_read_till_end_= */ false,
        /* empty_chunk_on_finish_= */ true,
        kind_, strictness_, on_clause_, input_headers, max_block_size)
{
}

void MergeJoinTransform::onFinish()
{
    algorithm.logElapsed(static_cast<double>(merging_elapsed_ns) / 1000000000ULL);
}

}
