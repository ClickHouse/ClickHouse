#include <atomic>
#include <limits>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Common/CurrentMetrics.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Interpreters/IJoin.h>
#include <Core/Defines.h>
#include <Core/SortCursor.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Sources/BlocksListSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Common/Stopwatch.h>


namespace CurrentMetrics
{
    extern const Metric MergeJoinBlocksCacheBytes;
    extern const Metric MergeJoinBlocksCacheCount;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

namespace
{

String deriveTempName(const String & name, JoinTableSide block_side)
{
    if (block_side == JoinTableSide::Left)
        return "--pmj_cond_left_" + name;
    return "--pmj_cond_right_" + name;
}

/*
 * Convert column with conditions for left or right table to join to joining key.
 * Input column type is UInt8 output is Nullable(UInt8).
 * 0 converted to NULL and such rows won't be joined,
 * 1 converted to 0 (any constant non-NULL value to join)
 */
ColumnWithTypeAndName conditionColumnToJoinable(const Block & block, const String & src_column_name, JoinTableSide block_side)
{
    size_t res_size = block.rows();
    auto data_col = ColumnUInt8::create(res_size, static_cast<UInt8>(0));
    auto null_map = ColumnUInt8::create(res_size, static_cast<UInt8>(0));

    if (!src_column_name.empty())
    {
        auto join_mask = JoinCommon::getColumnAsMask(block, src_column_name);
        if (join_mask.hasData())
        {
            for (size_t i = 0; i < res_size; ++i)
                null_map->getData()[i] = join_mask.isRowFiltered(i);
        }
    }

    ColumnPtr res_col = ColumnNullable::create(std::move(data_col), std::move(null_map));
    DataTypePtr res_col_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
    String res_name = deriveTempName(src_column_name, block_side);

    if (block.has(res_name))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Conflicting column name '{}' in block {}", res_name, block.dumpStructure());

    return {res_col, res_col_type, res_name};
}

// Compare with (left NULL != right NULL) logic
template <bool has_left_nulls, bool has_right_nulls>
Int64 nullableCompareAt(const IColumn & left_column, const IColumn & right_column, size_t lhs_pos, size_t rhs_pos)
{
    const IColumn * left_notnull = &left_column;
    const IColumn * right_notnull = &right_column;
    const ColumnNullable * left_nullable = nullptr;
    const ColumnNullable * right_nullable = nullptr;

    if constexpr (has_left_nulls)
    {
        left_nullable = checkAndGetColumn<ColumnNullable>(&left_column);
        if (left_nullable)
        {
            if (left_nullable->isNullAt(lhs_pos))
                return MergeJoin::nulls_direction;

            left_notnull = &left_nullable->getNestedColumn();
        }
    }

    if constexpr (has_right_nulls)
    {
        right_nullable = checkAndGetColumn<ColumnNullable>(&right_column);
        if (right_nullable)
        {
            if (right_nullable->isNullAt(rhs_pos))
                return -MergeJoin::nulls_direction;

            right_notnull = &right_nullable->getNestedColumn();
        }
    }

    return left_notnull->compareAt(lhs_pos, rhs_pos, *right_notnull, MergeJoin::nulls_direction);
}

// Compare first key column with (left NULL != right NULL) logic & track
template <bool has_left_nulls, bool has_right_nulls>
Int64 nullableCompareTrackAt(const IColumn & left_column, const IColumn & right_column, size_t lhs_pos, size_t rhs_pos)
{
    static_assert(MergeJoin::nulls_direction == -1); // NULLs are first

    const IColumn * left_notnull = &left_column;
    const IColumn * right_notnull = &right_column;
    const ColumnNullable * left_nullable = nullptr;
    const ColumnNullable * right_nullable = nullptr;

    if constexpr (has_left_nulls)
    {
        left_nullable = checkAndGetColumn<ColumnNullable>(&left_column);
        if (left_nullable)
        {
            // Source block is sorted with 'NULLs are first' order. Check NULLs only for lhs_pos == 0.
            if (!lhs_pos)
            {
                size_t null_pos = 0;
                while (null_pos < left_nullable->size() && left_nullable->isNullAt(null_pos))
                    ++null_pos;
                if (null_pos)
                    return -static_cast<Int64>(null_pos);
            }

            left_notnull = &left_nullable->getNestedColumn();
        }
    }

    if constexpr (has_right_nulls)
    {
        right_nullable = checkAndGetColumn<ColumnNullable>(&right_column);
        if (right_nullable)
        {
            // Source block is sorted with 'NULLs are first' order. Check NULLs only for rhs_pos == 0.
            // It also known we've already skipped all left NULLs.
            if (!rhs_pos)
            {
                size_t null_pos = 0;
                while (null_pos < right_nullable->size() && right_nullable->isNullAt(null_pos))
                    ++null_pos;
                if (null_pos)
                    return null_pos;
            }

            right_notnull = &right_nullable->getNestedColumn();
        }
    }

    // No need to check if column values have NULLs inside the track: it's the first key column,
    // NULLs are sorted first and the leading NULL runs were skipped above.
    return left_notnull->compareTrackAt(lhs_pos, rhs_pos, *right_notnull, MergeJoin::nulls_direction);
}

/// Get first and last row from sorted block
Block extractMinMax(const Block & block, const Block & keys)
{
    if (block.rows() == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected empty block");

    Block min_max = keys.cloneEmpty();
    MutableColumns columns = min_max.mutateColumns();

    for (size_t i = 0; i < columns.size(); ++i)
    {
        const auto & src_column = block.getByName(min_max.getByPosition(i).name);

        columns[i]->insertFrom(*src_column.column, 0);
        columns[i]->insertFrom(*src_column.column, block.rows() - 1);
    }

    min_max.setColumns(std::move(columns));

    for (auto & column : min_max)
        column.column = column.column->convertToFullColumnIfLowCardinality();
    return min_max;
}

Columns extractColumnsByNames(const Block & src, const Block & names)
{
    Columns columns;
    columns.reserve(names.columns());
    for (size_t i = 0; i < names.columns(); ++i)
        columns.push_back(src.getByName(names.getByPosition(i).name).column);
    return columns;
}

void addElapsed(UInt64 & target, UInt64 elapsed_ns)
{
    target += elapsed_ns;
}

void addElapsed(std::atomic<UInt64> & target, UInt64 elapsed_ns)
{
    target.fetch_add(elapsed_ns, std::memory_order_relaxed);
}

/// Adds the time spent in the enclosing scope to target
// Reads no clock when enabled is false.
/// Like std::lock_guard, it holds a reference and must not outlive target, so it is not copyable.
template <typename Counter>
class ScopedSortTimer
{
public:
    ScopedSortTimer(bool enabled, Counter & target_) : target(target_)
    {
        if (enabled)
            watch.emplace();
    }

    ScopedSortTimer(const ScopedSortTimer &) = delete;
    ScopedSortTimer & operator=(const ScopedSortTimer &) = delete;

    ~ScopedSortTimer()
    {
        if (watch)
            addElapsed(target, watch->elapsedNanoseconds());
    }

private:
    std::optional<Stopwatch> watch;
    Counter & target;
};

template <typename Counter>
ScopedSortTimer(bool, Counter &) -> ScopedSortTimer<Counter>;
}


class RowBitmaps
{
public:
    struct Bitmap
    {
        using Container = std::vector<bool>;

        std::mutex mutex;
        Container bitmap;

        size_t size() const { return bitmap.size(); }
        bool empty() const { return bitmap.empty(); }

        void applyOr(Container && addition) noexcept
        {
            std::lock_guard lock(mutex);

            if (bitmap.empty())
            {
                bitmap.swap(addition);
                return;
            }

            /// TODO: simd bit or (need padding and tail in container)
            for (size_t i = 0; i < bitmap.size(); ++i)
                if (addition[i])
                    bitmap[i] = true;
        }
    };

    using Container = Bitmap::Container;

    explicit RowBitmaps(size_t size)
    {
        maps.reserve(size);
        for (size_t i = 0; i < size; ++i)
            maps.emplace_back(std::make_unique<Bitmap>());
    }

    bool used(size_t bitmap_number) const
    {
        return !maps[bitmap_number]->empty();
    }

    void applyOr(size_t bitmap_number, Container && addition) noexcept
    {
        maps[bitmap_number]->applyOr(std::move(addition));
    }

    IColumn::Filter getNotUsed(size_t bitmap_number) const
    {
        const Container & bitmap = maps[bitmap_number]->bitmap;

        IColumn::Filter filter(bitmap.size());
        for (size_t i = 0; i < bitmap.size(); ++i)
            filter[i] = !bitmap[i];
        return filter;
    }

    /// Number of distinct right rows marked as used across all blocks.
    /// Non-used blocks keep an empty bitmap and contribute 0.
    size_t countUsed() const
    {
        size_t count = 0;
        for (const auto & map : maps)
            for (bool bit : map->bitmap)
                count += bit;
        return count;
    }

private:
    std::vector<std::unique_ptr<Bitmap>> maps;
};

struct MergeJoinEqualRange
{
    size_t left_start = 0;
    size_t right_start = 0;
    size_t left_length = 0;
    size_t right_length = 0;

    bool empty() const { return !left_length && !right_length; }
};


class MergeJoinCursor
{
public:
    MergeJoinCursor(const Block & block, const SortDescription & desc_)
        : impl(block, desc_)
    {
        for (auto *& column : impl.sort_columns)
        {
            const auto * lowcard_column = typeid_cast<const ColumnLowCardinality *>(column);
            if (lowcard_column)
            {
                auto & new_col = column_holder.emplace_back(lowcard_column->convertToFullColumn());
                column = new_col.get();
            }
        }

        /// SortCursorImpl can work with permutation, but MergeJoinCursor can't.
        if (impl.permutation)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeJoinCursor doesn't support permutation");

        if (impl.sort_columns_size == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeJoinCursor requires sort_columns size greater then 0");

        /// We use zero position as 'is first range in block' detector
        if (position())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeJoinCursor is expected to have initial position 0");
    }

    size_t position() const { return impl.getPos(); }
    size_t end() const { return impl.rows; }
    bool atEnd() const { return impl.getPos() >= impl.rows; }
    void next() { ++impl.getPosRef(); }
    void nextN(size_t num) { impl.getPosRef() += num; }

    void setCompareNullability(const MergeJoinCursor & rhs)
    {
        has_left_nullable = false;
        has_right_nullable = false;

        for (size_t i = 0; i < impl.sort_columns_size; ++i)
        {
            has_left_nullable = has_left_nullable || isColumnNullable(*impl.sort_columns[i]);
            has_right_nullable = has_right_nullable || isColumnNullable(*rhs.impl.sort_columns[i]);
        }
    }

    MergeJoinEqualRange getNextEqualRange(MergeJoinCursor & rhs)
    {
        if (has_left_nullable && has_right_nullable)
            return getNextEqualRangeImpl<true, true>(rhs);
        if (has_left_nullable)
            return getNextEqualRangeImpl<true, false>(rhs);
        if (has_right_nullable)
            return getNextEqualRangeImpl<false, true>(rhs);
        return getNextEqualRangeImpl<false, false>(rhs);
    }

    int intersect(const Block & min_max, const Names & key_names)
    {
        if (end() == 0 || min_max.rows() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected block size");

        size_t last_position = end() - 1;
        Int64 first_vs_max = 0;
        Int64 last_vs_min = 0;

        for (size_t i = 0; i < impl.sort_columns_size; ++i)
        {
            const auto & left_column = *impl.sort_columns[i];
            const auto & right_column = *min_max.getByName(key_names[i]).column; /// cannot get by position cause of possible duplicates

            if (!first_vs_max)
                first_vs_max = nullableCompareAt<true, true>(left_column, right_column, position(), 1);

            if (!last_vs_min)
                last_vs_min = nullableCompareAt<true, true>(left_column, right_column, last_position, 0);
        }

        if (first_vs_max > 0)
            return 1;
        if (last_vs_min < 0)
            return -1;
        return 0;
    }

private:
    SortCursorImpl impl;
    Columns column_holder;
    bool has_left_nullable = false;
    bool has_right_nullable = false;

    template <bool left_nulls, bool right_nulls>
    MergeJoinEqualRange getNextEqualRangeImpl(MergeJoinCursor & rhs)
    {
        if (atEnd() || rhs.atEnd())
            return MergeJoinEqualRange{position(), rhs.position(), 0, 0};

        while (true)
        {
            Int64 cmp = nullableCompareTrackAt<left_nulls, right_nulls>(
                *impl.sort_columns[0], *rhs.impl.sort_columns[0], position(), rhs.position());

            for (size_t i = 1; (!cmp) && i < impl.sort_columns_size; ++i)
            {
                const auto * left_column = impl.sort_columns[i];
                const auto * right_column = rhs.impl.sort_columns[i];

                cmp = nullableCompareAt<left_nulls, right_nulls>(*left_column, *right_column, position(), rhs.position());
            }

            if (cmp < 0)
            {
                nextN(-cmp);
                if (atEnd())
                    break;
            }
            else if (cmp > 0)
            {
                rhs.nextN(cmp);
                if (rhs.atEnd())
                    break;
            }
            else
                return MergeJoinEqualRange{position(), rhs.position(), getEqualLength(), rhs.getEqualLength()};
        }

        return MergeJoinEqualRange{position(), rhs.position(), 0, 0};
    }

    /// Expects !atEnd()
    size_t getEqualLength()
    {
        const size_t base_pos = impl.getPos();
        return getEqualRangeEndAssumeSorted(impl.sort_columns, base_pos, impl.rows, 1) - base_pos;
    }
};


namespace
{

MutableColumns makeMutableColumns(const Block & block, size_t rows_to_reserve = 0)
{
    MutableColumns columns;
    columns.reserve(block.columns());

    for (const auto & src_column : block)
    {
        columns.push_back(src_column.column->cloneEmpty());
        columns.back()->reserve(rows_to_reserve);
    }
    return columns;
}

void makeSortAndMerge(const Names & keys, SortDescription & sort, SortDescription & merge)
{
    NameSet unique_keys;
    for (const auto & sd: merge)
        unique_keys.insert(sd.column_name);

    for (const auto & key_name : keys)
    {
        merge.emplace_back(key_name);

        if (!unique_keys.contains(key_name))
        {
            unique_keys.insert(key_name);
            sort.emplace_back(key_name, /*direction*/1, MergeJoin::nulls_direction);
        }
    }
}

void copyLeftRange(const Block & block, MutableColumns & columns, size_t start, size_t rows_to_add)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & src_column = block.getByPosition(i).column;
        columns[i]->insertRangeFrom(*src_column, start, rows_to_add);
    }
}

void copyRightRange(const Columns & columns_to_add, MutableColumns & columns, size_t row_position, size_t rows_to_add)
{
    for (size_t i = 0; i < columns_to_add.size(); ++i)
    {
        const auto & src_column = columns_to_add[i];
        auto & dst_column = columns[i];
        auto * dst_nullable = typeid_cast<ColumnNullable *>(dst_column.get());

        if (dst_nullable && !isColumnNullable(*src_column))
            dst_nullable->insertManyFromNotNullable(*src_column, row_position, rows_to_add);
        else
            dst_column->insertManyFrom(*src_column, row_position, rows_to_add);
    }
}

void joinEqualsAnyLeft(const Columns & columns_to_add, MutableColumns & right_columns, const MergeJoinEqualRange & range)
{
    copyRightRange(columns_to_add, right_columns, range.right_start, range.left_length);
}

template <bool is_all>
bool joinEquals(
    const Block & left_block,
    const Columns & columns_to_add,
    MutableColumns & left_columns,
    MutableColumns & right_columns,
    MergeJoinEqualRange & range,
    size_t max_rows [[maybe_unused]])
{
    bool one_more = true;

    if constexpr (is_all)
    {
        size_t range_rows = range.left_length * range.right_length;
        if (range_rows > max_rows)
        {
            /// We need progress. So we join at least one right row.
            range.right_length = max_rows / range.left_length;
            if (!range.right_length)
                range.right_length = 1;
            one_more = false;
        }

        size_t left_rows_to_add = range.left_length;
        size_t row_position = range.right_start;
        for (size_t right_row = 0; right_row < range.right_length; ++right_row, ++row_position)
        {
            copyLeftRange(left_block, left_columns, range.left_start, left_rows_to_add);
            copyRightRange(columns_to_add, right_columns, row_position, left_rows_to_add);
        }
    }
    else
    {
        size_t left_rows_to_add = range.left_length;
        copyLeftRange(left_block, left_columns, range.left_start, left_rows_to_add);
        copyRightRange(columns_to_add, right_columns, range.right_start, left_rows_to_add);
    }

    return one_more;
}

/// Emits left rows [start, end) that have no match as result rows of a LEFT/FULL join:
/// copies the left rows (unless the left block itself is the result, as for LEFT ANY)
/// and appends default values to every right column.
template <bool copy_left>
void addNotMatchedLeftRange(const Block & left_block, MutableColumns & left_columns,
                            const Block & right_block, MutableColumns & right_columns,
                            size_t start, size_t end)
{
    if (end <= start)
        return;

    size_t rows_to_add = end - start;
    if constexpr (copy_left)
        copyLeftRange(left_block, left_columns, start, rows_to_add);

    for (size_t i = 0; i < right_columns.size(); ++i)
    {
        JoinCommon::addDefaultValues(*right_columns[i], right_block.getByPosition(i).type, rows_to_add);
    }
}

/// Emits the non-matched left rows of LEFT/FULL joins when the join has a mixed ON condition.
/// Without such a condition, the key comparison alone decides the match status, and the rows
/// are emitted in place during the probe (addNotMatchedLeftRange call sites in leftJoin and
/// joinSortedBlock). With such a condition, a key-matched row can still fail all candidate
/// pairs, and its last candidate pair can be in a later right block (an equal-key run can span
/// right blocks). Thus the decision is deferred: this pass runs once, after the whole left
/// block was probed, and emits every row without a set flag in `left_row_matched`.
void addNotMatchedLeftRows(const Block & left_block, MutableColumns & left_columns,
                           const Block & right_block, MutableColumns & right_columns,
                           const MergeJoin::LeftMatchedBitmap & left_row_matched)
{
    size_t rows = left_row_matched.empty() ? 0 : left_block.rows();
    size_t i = 0;
    while (i < rows)
    {
        if (left_row_matched.test(i))
        {
            ++i;
            continue;
        }

        size_t run_start = i;
        while (i < rows && !left_row_matched.test(i))
            ++i;
        addNotMatchedLeftRange<true>(left_block, left_columns, right_block, right_columns, run_start, i);
    }
}

}


MergeJoin::MergeJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block_)
    : table_join(table_join_)
    , size_limits(table_join->sizeLimits())
    , right_sample_block(*right_sample_block_)
    , is_any_join(table_join->strictness() == JoinStrictness::Any)
    , is_all_join(table_join->strictness() == JoinStrictness::All)
    , is_semi_join(table_join->strictness() == JoinStrictness::Semi)
    , is_inner(isInner(table_join->kind()))
    , is_left(isLeft(table_join->kind()))
    , is_right(isRight(table_join->kind()))
    , is_full(isFull(table_join->kind()))
    , max_joined_block_rows(table_join->maxJoinedBlockRows())
    , max_rows_in_right_block(table_join->maxRowsInRightBlock())
    , max_files_to_merge(table_join->maxFilesToMerge())
    , log(getLogger("MergeJoin"))
{
    switch (table_join->strictness())
    {
        case JoinStrictness::All:
            break;
        case JoinStrictness::Any:
        case JoinStrictness::Semi:
            if (!is_left && !is_inner)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not supported. MergeJoin supports SEMI and ANY variants only for LEFT and INNER JOINs.");
            break;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not supported. MergeJoin supports ALL, ANY and SEMI JOINs variants.");
    }

    if (!max_rows_in_right_block)
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "partial_merge_join_rows_in_right_blocks cannot be zero");

    if (max_files_to_merge < 2)
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "max_files_to_merge cannot be less than 2");

    if (!size_limits.hasLimits())
    {
        size_limits.max_bytes = table_join->defaultMaxBytes();
        if (!size_limits.max_bytes)
            throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND,
                            "No limit for MergeJoin (max_rows_in_join or max_bytes_in_join settings must be set)");
    }

    if (!table_join->oneDisjunct())
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeJoin does not support OR in JOIN ON section");

    if (const auto & mixed_expression = table_join->getMixedJoinExpression())
    {
        mixed_join_expression = mixed_expression;
        validateMixedJoinExpression();
    }

    const auto & onexpr = table_join->getOnlyClause();
    std::tie(mask_column_name_left, mask_column_name_right) = onexpr.condColumnNames();

    /// Add auxiliary joining keys to join only rows where conditions from JOIN ON sections holds
    /// Input boolean column converted to nullable and only rows with non NULLS value will be joined
    if (!mask_column_name_left.empty() || !mask_column_name_right.empty())
    {
        JoinCommon::checkTypesOfMasks({}, "", right_sample_block, mask_column_name_right);

        key_names_left.push_back(deriveTempName(mask_column_name_left, JoinTableSide::Left));
        key_names_right.push_back(deriveTempName(mask_column_name_right, JoinTableSide::Right));
    }

    key_names_left.insert(key_names_left.end(), onexpr.key_names_left.begin(), onexpr.key_names_left.end());
    key_names_right.insert(key_names_right.end(), onexpr.key_names_right.begin(), onexpr.key_names_right.end());

    addConditionJoinColumn(right_sample_block, JoinTableSide::Right);
    JoinCommon::splitAdditionalColumns(key_names_right, right_sample_block, right_table_keys, right_columns_to_add);

    const NameSet required_right_keys = table_join->requiredRightKeys();
    for (const auto & right_key : key_names_right)
    {
        if (required_right_keys.contains(right_key) && right_table_keys.getByName(right_key).type->lowCardinality())
            lowcard_right_keys.push_back(right_key);
    }

    for (const auto & column : right_table_keys)
        if (required_right_keys.contains(column.name))
            right_columns_to_add.insert(ColumnWithTypeAndName{nullptr, column.type, column.name});

    JoinCommon::createMissedColumns(right_columns_to_add);

    makeSortAndMerge(key_names_left, left_sort_description, left_merge_description);
    makeSortAndMerge(key_names_right, right_sort_description, right_merge_description);

    LOG_DEBUG(log, "Joining keys: left [{}], right [{}]", fmt::join(key_names_left, ", "), fmt::join(key_names_right, ", "));

    if (size_t max_bytes = table_join->maxBytesInLeftBuffer(); max_bytes > 0)
    {
        /// Disabled due to https://github.com/ClickHouse/ClickHouse/issues/31009
        // left_blocks_buffer = std::make_shared<SortedBlocksBuffer>(left_sort_description, max_bytes);
        LOG_WARNING(log, "`partial_merge_join_left_table_buffer_bytes` is disabled in current version of ClickHouse");
        UNUSED(left_blocks_buffer);
    }
}

void MergeJoin::setTotals(const Block & totals_block)
{
    IJoin::setTotals(totals_block);
}

/// Finalizes the right side. Unlike `setTotals`, the post build phase is always reached, and it
/// runs in the work context, which `mergeRightBlocks` requires because it drives a nested
/// pipeline.
void MergeJoin::runPostBuildPhase()
{
    mergeRightBlocks();

    if (is_right || is_full || (is_all_join && table_join->collectExactMatches()))
        used_rows_bitmap = std::make_shared<RowBitmaps>(getRightBlocksCount());
}

void MergeJoin::mergeRightBlocks()
{
    if (is_in_memory)
        mergeInMemoryRightBlocks();
    else
        mergeFlushedRightBlocks();
}

void MergeJoin::mergeInMemoryRightBlocks()
{
    std::lock_guard lock(rwlock);

    if (right_blocks.empty())
        return;

    ScopedSortTimer merge_timer(table_join->collectAnalyzeStats(), build_sort_time_ns);

    Pipe source(std::make_shared<BlocksListSource>(std::move(right_blocks.blocks)));
    right_blocks.clear();

    QueryPipelineBuilder builder;
    builder.init(std::move(source));

    /// TODO: there should be no split keys by blocks for RIGHT|FULL JOIN
    builder.addTransform(std::make_shared<MergeSortingTransform>(
        builder.getSharedHeader(),
        right_sort_description,
        max_rows_in_right_block,
        /*max_block_bytes=*/0,
        /*limit_=*/0,
        /*increase_sort_description_compile_attempts=*/false,
        /*max_bytes_before_remerge_*/0,
        /*remerge_lowered_memory_bytes_ratio_*/0,
        /*max_bytes_in_block_before_external_sort_*/0,
        /*max_bytes_in_query_before_external_sort_*/0,
        /*tmp_data_*/nullptr,
        /*min_free_disk_space_*/0));

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PullingPipelineExecutor executor(pipeline);

    Block block;
    while (executor.pull(block))
    {
        if (!block.rows()) // NOLINT(clang-analyzer-cplusplus.Move)
            continue;

        if (skip_not_intersected)
            min_max_right_blocks.emplace_back(extractMinMax(block, right_table_keys));
        right_blocks.countBlockSize(block);
        loaded_right_blocks.emplace_back(std::make_shared<Block>(std::move(block)));
    }
}

void MergeJoin::mergeFlushedRightBlocks()
{
    std::lock_guard lock(rwlock);

    auto callback = [&](const Block & block)
    {
        if (skip_not_intersected)
            min_max_right_blocks.emplace_back(extractMinMax(block, right_table_keys));
        right_blocks.countBlockSize(block);
    };

    {
        ScopedSortTimer merge_timer(table_join->collectAnalyzeStats(), build_sort_time_ns);

        flushed_right_blocks = disk_writer->finishMerge(callback);
        disk_writer.reset();
    }

    if (table_join->collectAnalyzeStats())
        for (const auto & file : flushed_right_blocks)
            right_spilled_compressed_bytes += file.getHolder()->getStat().compressed_size;

    /// Get memory limit or approximate it from row limit and bytes per row factor
    UInt64 memory_limit = size_limits.max_bytes;
    UInt64 rows_limit = size_limits.max_rows;
    if (!memory_limit && rows_limit)
        memory_limit = right_blocks.bytes * rows_limit / right_blocks.row_count;

    cached_right_blocks = std::make_unique<Cache>(CurrentMetrics::MergeJoinBlocksCacheBytes, CurrentMetrics::MergeJoinBlocksCacheCount, memory_limit);
}

bool MergeJoin::saveRightBlock(Block && block)
{
    if (is_in_memory)
    {
        std::lock_guard lock(rwlock);

        if (!is_in_memory)
        {
            disk_writer->insert(std::move(block));
            return true;
        }

        right_blocks.insert(std::move(block));

        bool has_memory = size_limits.softCheck(right_blocks.row_count, right_blocks.bytes);
        if (!has_memory)
        {
            initRightTableWriter();
            is_in_memory = false;
        }
    }
    else
        disk_writer->insert(std::move(block));
    return true;
}

Block MergeJoin::modifyRightBlock(const Block & src_block) const
{
    return materializeBlock(src_block);
}

bool MergeJoin::addBlockToJoin(const Block & src_block, bool)
{
    Block block = modifyRightBlock(src_block);

    addConditionJoinColumn(block, JoinTableSide::Right);

    {
        ScopedSortTimer sort_timer(table_join->collectAnalyzeStats(), build_sort_time_ns);
        sortBlock(block, right_sort_description);
    }

    return saveRightBlock(std::move(block));
}

void MergeJoin::checkTypesOfKeys(const Block & block) const
{
    /// Do not check auxailary column for extra conditions, use original key names
    const auto & onexpr = table_join->getOnlyClause();
    JoinCommon::checkTypesOfKeys(block, onexpr.key_names_left, right_table_keys, onexpr.key_names_right);
}

class MergeJoinResult : public IJoinResult
{
    Block block;
    std::optional<MergeJoin::NotProcessed> not_processed;
    MergeJoin & merge_join;

public:
    MergeJoinResult(Block block_, MergeJoin & merge_join_)
        : block(std::move(block_)), merge_join(merge_join_) {}

    JoinResultBlock next() override
    {
        if (!not_processed)
        {
            merge_join.joinBlock(block, not_processed);
            return {std::move(block), nullptr, !not_processed.has_value()};
        }

        block = not_processed->block;
        merge_join.joinBlock(block, not_processed);
        return {std::move(block), nullptr, !not_processed.has_value()};
    }
};

JoinResultPtr MergeJoin::joinBlock(Block block)
{
    return std::make_unique<MergeJoinResult>(std::move(block), *this);
}

void MergeJoin::joinBlock(Block & block, std::optional<MergeJoin::NotProcessed> & not_processed)
{
    Names lowcard_keys = lowcard_right_keys;
    if (!block.empty())
    {
        /// We need to check type of masks before `addConditionJoinColumn`, because it assumes that types is correct
        JoinCommon::checkTypesOfMasks(block, mask_column_name_left, right_sample_block, mask_column_name_right);

        if (!not_processed)
            /// Add an auxiliary column, which will be removed after joining
            /// We do not need to add it twice when we are continuing to process the block from the previous iteration
            addConditionJoinColumn(block, JoinTableSide::Left);

        /// Types of keys can be checked only after `checkTypesOfKeys`
        JoinCommon::checkTypesOfKeys(block, key_names_left, right_table_keys, key_names_right);

        materializeBlockInplace(block);

        for (const auto & column_name : key_names_left)
        {
            if (block.getByName(column_name).type->lowCardinality())
                lowcard_keys.push_back(column_name);
        }

        /// A continuation block was already sorted by the first pass. Do not sort it again:
        /// the sort is unstable and can reorder rows with equal keys, while the continuation
        /// state (cursor positions, per-row matched flags of the mixed condition) refers to
        /// the previous order.
        if (!not_processed)
        {
            ScopedSortTimer sort_timer(table_join->collectAnalyzeStats(), probe_sort_time_ns);
            sortBlock(block, left_sort_description);
        }
    }

    if (!not_processed && left_blocks_buffer)
    {
        if (block.empty() || block.rows())
            block = left_blocks_buffer->exchange(std::move(block));
        if (block.empty())
            return;
    }

    if (is_in_memory)
    {
        if (is_all_join)
            joinSortedBlock<true, true>(block, not_processed);
        else
            joinSortedBlock<true, false>(block, not_processed);
    }
    else
    {
        if (is_all_join)
            joinSortedBlock<false, true>(block, not_processed);
        else
            joinSortedBlock<false, false>(block, not_processed);
    }

    /// Back thread even with no data. We have some unfinished data in buffer.
    if (!not_processed && left_blocks_buffer)
        not_processed = NotProcessed{{}, 0, 0, 0, 0};

    if (needConditionJoinColumn())
        block.erase(deriveTempName(mask_column_name_left, JoinTableSide::Left));
}

template <bool in_memory, bool is_all>
void MergeJoin::joinSortedBlock(Block & block, std::optional<NotProcessed> & not_processed)
{
    size_t rows_to_reserve = is_left ? block.rows() : 0;
    MutableColumns left_columns = makeMutableColumns(block, (is_all ? rows_to_reserve : 0));
    MutableColumns right_columns = makeMutableColumns(right_columns_to_add, rows_to_reserve);
    MergeJoinCursor left_cursor(block, left_merge_description);
    size_t left_key_tail = 0;
    size_t skip_right = 0;
    size_t right_blocks_count = rightBlocksCount<in_memory>();

    /// With a mixed JOIN ON condition: per-row flags of the sorted left block, set once a row
    /// produces a result row. Used to emit the first passing pair only (ANY/SEMI) and to emit
    /// non-matched rows of LEFT/FULL joins after the whole block was probed.
    MergeJoin::LeftMatchedBitmap left_row_matched;

    size_t starting_right_block = 0;
    if (not_processed)
    {
        auto & continuation = static_cast<NotProcessed &>(*not_processed);
        left_cursor.nextN(continuation.left_position);
        left_key_tail = continuation.left_key_tail;
        skip_right = continuation.right_position;
        starting_right_block = continuation.right_block;
        left_row_matched = std::move(continuation.left_row_matched);
        not_processed.reset();
    }
    else
    {
        /// Count the block rows only when we are processing it for the first time
        total_left_rows.fetch_add(block.rows(), std::memory_order_relaxed);
    }

    bool with_left_inequals = (is_left && !is_semi_join) || is_full;
    size_t matched_rows = 0;

    /// ALL INNER/RIGHT joins with a mixed condition do not need the flags for the join itself:
    /// they emit neither non-matched left rows nor at-most-one-pair outputs. They still need
    /// them under EXPLAIN ANALYZE: `matched_left_rows` counts a left row when a pair passes
    /// the condition (bit transition), like the hash join does.
    bool need_left_row_matched = mixed_join_expression
        && (with_left_inequals || !is_all || table_join->collectAnalyzeStats());
    if (need_left_row_matched && left_row_matched.empty())
        left_row_matched.reset(block.rows());

    MergeJoin::LeftMatchedBitmap * left_row_matched_ptr = need_left_row_matched ? &left_row_matched : nullptr;

    if (with_left_inequals)
    {
        for (size_t i = starting_right_block; i < right_blocks_count; ++i)
        {
            if (left_cursor.atEnd())
                break;

            if (skip_not_intersected)
            {
                int intersection = left_cursor.intersect(min_max_right_blocks[i], key_names_right);
                if (intersection < 0)
                    break; /// (left) ... (right)
                if (intersection > 0)
                {
                    skip_right = 0;
                    continue; /// (right) ... (left)
                }
            }

            /// Use skip_right as ref. It would be updated in join.
            RightBlockInfo right_block(loadRightBlock<in_memory>(i), i, skip_right, used_rows_bitmap.get());

            if (!leftJoin<is_all>(left_cursor, block, right_block, left_columns, right_columns, left_key_tail, matched_rows, left_row_matched_ptr))
            {
                matched_left_rows.fetch_add(matched_rows, std::memory_order_relaxed);
                not_processed = extraBlock<is_all>(block, std::move(left_columns), std::move(right_columns),
                                                   left_cursor.position(), left_key_tail, skip_right, i,
                                                   std::move(left_row_matched));
                return;
            }
        }

        if (mixed_join_expression)
        {
            /// Non-matched rows were not emitted in place: whether a key-matched row has a pair
            /// that passes the condition is known only after its last right block.
            addNotMatchedLeftRows(block, left_columns, right_columns_to_add, right_columns, left_row_matched);
        }
        else
        {
            left_cursor.nextN(left_key_tail);
            addNotMatchedLeftRange<is_all>(block, left_columns, right_columns_to_add, right_columns, left_cursor.position(), left_cursor.end());
        }

        changeLeftColumns(block, std::move(left_columns));
        addRightColumns(block, std::move(right_columns));
    }
    else /// no inequals
    {
        for (size_t i = starting_right_block; i < right_blocks_count; ++i)
        {
            if (left_cursor.atEnd())
                break;

            if (skip_not_intersected)
            {
                int intersection = left_cursor.intersect(min_max_right_blocks[i], key_names_right);
                if (intersection < 0)
                    break; /// (left) ... (right)
                if (intersection > 0)
                {
                    skip_right = 0;
                    continue; /// (right) ... (left)
                }
            }

            /// Use skip_right as ref. It would be updated in join.
            RightBlockInfo right_block(loadRightBlock<in_memory>(i), i, skip_right, used_rows_bitmap.get());

            if constexpr (is_all)
            {
                if (!allInnerJoin(left_cursor, block, right_block, left_columns, right_columns, left_key_tail, matched_rows, left_row_matched_ptr))
                {
                    matched_left_rows.fetch_add(matched_rows, std::memory_order_relaxed);
                    not_processed = extraBlock<is_all>(block, std::move(left_columns), std::move(right_columns),
                                                       left_cursor.position(), left_key_tail, skip_right, i,
                                                       std::move(left_row_matched));
                    return;
                }
            }
            else
                semiLeftJoin(left_cursor, block, right_block, left_columns, right_columns, left_key_tail, matched_rows, left_row_matched_ptr);
        }

        left_cursor.nextN(left_key_tail);
        changeLeftColumns(block, std::move(left_columns));
        addRightColumns(block, std::move(right_columns));
    }
    matched_left_rows.fetch_add(matched_rows, std::memory_order_relaxed);
}

static size_t maxRangeRows(size_t current_rows, size_t max_rows)
{
    if (!max_rows)
        return std::numeric_limits<size_t>::max();
    if (current_rows >= max_rows)
        return 0;
    return max_rows - current_rows;
}

template <bool is_all>
bool MergeJoin::leftJoin(MergeJoinCursor & left_cursor, const Block & left_block, RightBlockInfo & right_block_info,
                         MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail, size_t & matched_rows,
                         MergeJoin::LeftMatchedBitmap * left_row_matched)
{
    const Block & right_block = *right_block_info.block;
    MergeJoinCursor right_cursor(right_block, right_merge_description);
    left_cursor.setCompareNullability(right_cursor);

    auto r_columns_to_add = extractColumnsByNames(right_block, right_columns_to_add);

    /// Set right cursor position in first continuation right block
    if constexpr (is_all)
    {
        right_cursor.nextN(right_block_info.skip);
        right_block_info.skip = 0;
    }

    while (!left_cursor.atEnd() && !right_cursor.atEnd())
    {
        /// Not zero left_key_tail means there were equality for the last left key in previous leftJoin() call.
        /// Do not join it twice: join only if it's equal with a first right key of current leftJoin() call and skip otherwise.
        size_t left_unequal_position = left_cursor.position() + left_key_tail;
        left_key_tail = 0;

        MergeJoinEqualRange range = left_cursor.getNextEqualRange(right_cursor);

        /// With a mixed condition non-matched rows are emitted by addNotMatchedLeftRows instead:
        /// key-matched rows can still end up non-matched when no candidate pair passes the condition.
        if (!mixed_join_expression)
            addNotMatchedLeftRange<is_all>(left_block, left_columns, right_columns_to_add, right_columns, left_unequal_position, range.left_start);

        if (range.empty())
            break;

        /// With a mixed condition the matched left rows are counted inside the mixed helpers,
        /// on the bit transitions of `left_row_matched`: a left row counts as matched only when
        /// a candidate pair passes the condition, not on key equality.
        if (!mixed_join_expression && left_unequal_position <= range.left_start)
            matched_rows += range.left_length;

        if constexpr (is_all)
        {
            size_t max_rows = maxRangeRows(left_columns[0]->size(), max_joined_block_rows);

            bool range_is_complete = true;
            if (mixed_join_expression)
            {
                range_is_complete = joinEqualsWithMixedCondition(
                    left_block, right_block_info, r_columns_to_add, left_columns, right_columns, range, max_rows, left_row_matched, matched_rows);
            }
            else
            {
                right_block_info.setUsed(range.right_start, range.right_length);
                range_is_complete = joinEquals<true>(left_block, r_columns_to_add, left_columns, right_columns, range, max_rows);
            }

            if (!range_is_complete)
            {
                right_cursor.nextN(range.right_length);
                right_block_info.skip = right_cursor.position();
                left_key_tail = range.left_length;
                return false;
            }
        }
        else
        {
            if (mixed_join_expression)
                joinAnyWithMixedCondition(left_block, right_block, r_columns_to_add, left_columns, right_columns, range, *left_row_matched, matched_rows);
            else
                joinEqualsAnyLeft(r_columns_to_add, right_columns, range);
        }

        right_cursor.nextN(range.right_length);

        /// Do not run over last left keys for ALL JOIN (cause of possible duplicates in next right block)
        if constexpr (is_all)
        {
            if (right_cursor.atEnd())
            {
                left_key_tail = range.left_length;
                break;
            }
        }
        else
        {
            /// Same for ANY with a mixed condition: rows of the last range may still be
            /// non-matched here, and the equal-key run can continue in the next right block.
            if (mixed_join_expression && right_cursor.atEnd())
            {
                left_key_tail = range.left_length;
                break;
            }
        }
        left_cursor.nextN(range.left_length);
    }

    return true;
}

bool MergeJoin::allInnerJoin(MergeJoinCursor & left_cursor, const Block & left_block, RightBlockInfo & right_block_info,
                             MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail, size_t & matched_rows,
                             MergeJoin::LeftMatchedBitmap * left_row_matched)
{
    const Block & right_block = *right_block_info.block;
    MergeJoinCursor right_cursor(right_block, right_merge_description);
    left_cursor.setCompareNullability(right_cursor);

    auto r_columns_to_add = extractColumnsByNames(right_block, right_columns_to_add);

    /// Set right cursor position in first continuation right block
    right_cursor.nextN(right_block_info.skip);
    right_block_info.skip = 0;

    while (!left_cursor.atEnd() && !right_cursor.atEnd())
    {
        size_t starting_position = left_cursor.position() + left_key_tail;
        left_key_tail = 0;
        MergeJoinEqualRange range = left_cursor.getNextEqualRange(right_cursor);

        if (range.empty())
            break;

        /// With a mixed condition the matched left rows are counted inside the mixed helper,
        /// on the bit transitions of `left_row_matched` (null when the stats are not collected).
        if (!mixed_join_expression && starting_position <= range.left_start)
            matched_rows += range.left_length;

        size_t max_rows = maxRangeRows(left_columns[0]->size(), max_joined_block_rows);

        bool range_is_complete = true;
        if (mixed_join_expression)
        {
            range_is_complete = joinEqualsWithMixedCondition(
                left_block, right_block_info, r_columns_to_add, left_columns, right_columns, range, max_rows, left_row_matched, matched_rows);
        }
        else
        {
            right_block_info.setUsed(range.right_start, range.right_length);
            range_is_complete = joinEquals<true>(left_block, r_columns_to_add, left_columns, right_columns, range, max_rows);
        }

        if (!range_is_complete)
        {
            right_cursor.nextN(range.right_length);
            right_block_info.skip = right_cursor.position();
            left_key_tail = range.left_length;
            return false;
        }

        right_cursor.nextN(range.right_length);

        /// Do not run over last left keys for ALL JOIN (cause of possible duplicates in next right block)
        if (right_cursor.atEnd())
        {
            left_key_tail = range.left_length;
            break;
        }
        left_cursor.nextN(range.left_length);
    }

    return true;
}

bool MergeJoin::semiLeftJoin(MergeJoinCursor & left_cursor, const Block & left_block, RightBlockInfo & right_block_info,
                             MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail,
                             size_t & matched_rows, MergeJoin::LeftMatchedBitmap * left_row_matched)
{
    const Block & right_block = *right_block_info.block;
    MergeJoinCursor right_cursor(right_block, right_merge_description);
    left_cursor.setCompareNullability(right_cursor);

    auto r_columns_to_add = extractColumnsByNames(right_block, right_columns_to_add);

    while (!left_cursor.atEnd() && !right_cursor.atEnd())
    {
        MergeJoinEqualRange range = left_cursor.getNextEqualRange(right_cursor);
        if (range.empty())
            break;

        if (mixed_join_expression)
        {
            /// The helper counts one matched row for each left row that gets its first passing
            /// pair here. Counting the range length would count the same rows again when the
            /// equal-key run continues in the next right block (the left_key_tail revisit below).
            joinAnyWithMixedCondition(left_block, right_block, r_columns_to_add, left_columns, right_columns, range, *left_row_matched, matched_rows);
        }
        else
        {
            matched_rows += range.left_length;
            joinEquals<false>(left_block, r_columns_to_add, left_columns, right_columns, range, 0);
        }

        right_cursor.nextN(range.right_length);

        /// With a mixed condition, rows of the last range may still be non-matched here, and the
        /// equal-key run can continue in the next right block: keep them under the left cursor.
        if (mixed_join_expression && right_cursor.atEnd())
        {
            left_key_tail = range.left_length;
            break;
        }
        left_cursor.nextN(range.left_length);
    }

    return true;
}

void MergeJoin::validateMixedJoinExpression()
{
    Block expression_sample_block = mixed_join_expression->getSampleBlock();

    if (expression_sample_block.columns() != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected expression in JOIN ON section. Expected single column, got '{}', expression:\n{}",
            expression_sample_block.dumpStructure(),
            mixed_join_expression->dumpActions());
    }

    auto type = removeNullable(expression_sample_block.getByPosition(0).type);
    if (!type->equals(*std::make_shared<DataTypeUInt8>()))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected expression in JOIN ON section. Expected boolean (UInt8), got '{}'. expression:\n{}",
            expression_sample_block.getByPosition(0).type->getName(),
            mixed_join_expression->dumpActions());
    }

    for (const auto & input : mixed_join_expression->getRequiredColumnsWithTypes())
    {
        bool from_right = right_sample_block.has(input.name);
        if (from_right)
        {
            /// `evaluateMixedJoinExpression` creates the column for this input with the stored
            /// right column class and declares it with `input.type`, so resolving the input by
            /// name alone is not enough: a same-named column of a different type would be read
            /// through a mismatched `IColumn` interface. Fail here, where both types are known.
            const auto & stored = right_sample_block.getByName(input.name);
            if (!stored.type->equals(*input.type))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Column {} required by the mixed JOIN ON condition has type {}, "
                    "but the stored right column of that name has type {}",
                    input.name,
                    input.type->getName(),
                    stored.type->getName());
        }
        mixed_condition_columns.emplace_back(MixedConditionColumn{input.name, input.type, from_right});
    }
}

ColumnPtr MergeJoin::evaluateMixedJoinExpression(const Block & left_block, size_t left_start, size_t left_length,
                                                 const Block & right_block, size_t right_start, size_t right_length) const
{
    size_t result_rows = left_length * right_length;
    if (!result_rows)
        return ColumnUInt8::create();

    ColumnPtr result_column;
    if (mixed_condition_columns.empty())
    {
        /// A constant condition without inputs.
        Block executed_block;
        mixed_join_expression->execute(executed_block);
        result_column = executed_block.getByPosition(0).column->cloneResized(result_rows);
    }
    else
    {
        /// The input columns hold the cross product of the ranges in [right][left] order:
        /// the left range repeated for every right row, against that right row's value.
        ColumnsWithTypeAndName inputs;
        inputs.reserve(mixed_condition_columns.size());
        for (const auto & required : mixed_condition_columns)
        {
            if (required.from_right)
            {
                /// Right column exists, ensured in ctor based on right sample blocks
                const auto & src = right_block.getByName(required.name).column;
                auto column = src->cloneEmpty();
                column->reserve(result_rows);
                for (size_t right_row = 0; right_row < right_length; ++right_row)
                    column->insertManyFrom(*src, right_start + right_row, left_length);
                inputs.emplace_back(std::move(column), required.type, required.name);
            }
            else
            {
                const auto * src = left_block.findByName(required.name);
                if (!src)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Column {} required by the mixed JOIN ON condition is not found in the left block {}",
                        required.name, left_block.dumpNames());

                auto column = src->column->cloneEmpty();
                column->reserve(result_rows);
                for (size_t right_row = 0; right_row < right_length; ++right_row)
                    column->insertRangeFrom(*src->column, left_start, left_length);
                inputs.emplace_back(std::move(column), src->type, required.name);
            }
        }

        Block executed_block(std::move(inputs));
        mixed_join_expression->execute(executed_block);
        result_column = executed_block.getByPosition(0).column;
    }

    result_column = result_column->convertToFullColumnIfConst()->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();
    if (result_column->size() != result_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected size of the mixed JOIN ON condition result: {} instead of {}", result_column->size(), result_rows);

    if (result_column->isNullable())
    {
        /// Convert Nullable(UInt8) to UInt8 ensuring that nulls are zeros.
        /// Trying to avoid copying data, since we are the only owner of the column.
        ColumnPtr mask_column = assert_cast<const ColumnNullable &>(*result_column).getNullMapColumnPtr();

        MutableColumnPtr mutable_column;
        {
            ColumnPtr nested_column = assert_cast<const ColumnNullable &>(*result_column).getNestedColumnPtr();
            result_column.reset();
            mutable_column = IColumn::mutate(std::move(nested_column));
        }

        auto & column_data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
        const auto & mask_column_data = assert_cast<const ColumnUInt8 &>(*mask_column).getData();
        for (size_t i = 0; i < column_data.size(); ++i)
        {
            if (mask_column_data[i])
                column_data[i] = 0;
        }
        return mutable_column;
    }
    return result_column;
}

bool MergeJoin::joinEqualsWithMixedCondition(const Block & left_block, RightBlockInfo & right_block_info,
                                             const Columns & right_columns_to_add_, MutableColumns & left_columns,
                                             MutableColumns & right_columns, MergeJoinEqualRange & range, size_t max_rows,
                                             MergeJoin::LeftMatchedBitmap * left_row_matched, size_t & matched_rows)
{
    bool one_more = true;

    /// Cap the candidate batch the same way joinEquals caps the emitted cross product:
    /// at least one right row per call to guarantee progress.
    size_t range_rows = range.left_length * range.right_length;
    if (range_rows > max_rows)
    {
        range.right_length = max_rows / range.left_length;
        if (!range.right_length)
            range.right_length = 1;
        one_more = false;
    }

    const Block & right_block = *right_block_info.block;
    auto mask_column = evaluateMixedJoinExpression(
        left_block, range.left_start, range.left_length, right_block, range.right_start, range.right_length);
    const auto & mask = assert_cast<const ColumnUInt8 &>(*mask_column).getData();

    size_t left_length = range.left_length;
    for (size_t right_row = 0; right_row < range.right_length; ++right_row)
    {
        const UInt8 * stripe = mask.data() + right_row * left_length;
        size_t rows_added = 0;

        size_t i = 0;
        while (i < left_length)
        {
            if (!stripe[i])
            {
                ++i;
                continue;
            }

            size_t run_start = i;
            while (i < left_length && stripe[i])
                ++i;

            copyLeftRange(left_block, left_columns, range.left_start + run_start, i - run_start);
            if (left_row_matched)
            {
                for (size_t j = run_start; j < i; ++j)
                    matched_rows += left_row_matched->setOnce(range.left_start + j);
            }
            rows_added += i - run_start;
        }

        if (rows_added)
        {
            copyRightRange(right_columns_to_add_, right_columns, range.right_start + right_row, rows_added);
            right_block_info.setUsed(range.right_start + right_row, 1);
        }
    }

    return one_more;
}

void MergeJoin::joinAnyWithMixedCondition(const Block & left_block, const Block & right_block,
                                          const Columns & right_columns_to_add_, MutableColumns & left_columns,
                                          MutableColumns & right_columns, const MergeJoinEqualRange & range,
                                          MergeJoin::LeftMatchedBitmap & left_row_matched, size_t & matched_rows) const
{
    size_t left_length = range.left_length;

    /// The output is at most one row per left row, but the mask is evaluated on candidate pairs.
    /// Process the right rows of the range in chunks to bound the candidate batch.
    size_t batch_cap = max_joined_block_rows ? max_joined_block_rows : DEFAULT_BLOCK_SIZE;
    size_t right_chunk_size = std::max<size_t>(1, batch_cap / left_length);

    for (size_t chunk_start = 0; chunk_start < range.right_length; chunk_start += right_chunk_size)
    {
        bool any_unmatched = false;
        for (size_t i = 0; i < left_length && !any_unmatched; ++i)
            any_unmatched = !left_row_matched.test(range.left_start + i);
        if (!any_unmatched)
            return;

        size_t chunk_length = std::min(right_chunk_size, range.right_length - chunk_start);
        auto mask_column = evaluateMixedJoinExpression(
            left_block, range.left_start, left_length, right_block, range.right_start + chunk_start, chunk_length);
        const auto & mask = assert_cast<const ColumnUInt8 &>(*mask_column).getData();

        for (size_t i = 0; i < left_length; ++i)
        {
            if (left_row_matched.test(range.left_start + i))
                continue;

            for (size_t right_row = 0; right_row < chunk_length; ++right_row)
            {
                if (mask[right_row * left_length + i])
                {
                    copyLeftRange(left_block, left_columns, range.left_start + i, 1);
                    copyRightRange(right_columns_to_add_, right_columns, range.right_start + chunk_start + right_row, 1);
                    left_row_matched.set(range.left_start + i);
                    ++matched_rows;
                    break;
                }
            }
        }
    }
}

void MergeJoin::changeLeftColumns(Block & block, MutableColumns && columns) const
{
    /// For LEFT ANY JOIN without a mixed condition the left block is the result as-is: right
    /// columns are built 1:1 to it. With a mixed condition left rows are copied pairwise instead
    /// (the first passing right row is not necessarily found in the first right block that
    /// matches the key), so the accumulated columns replace the block like for other kinds.
    if (is_left && is_any_join && !mixed_join_expression)
        return;
    block.setColumns(std::move(columns));
}

void MergeJoin::addRightColumns(Block & block, MutableColumns && right_columns)
{
    for (size_t i = 0; i < right_columns_to_add.columns(); ++i)
    {
        const auto & column = right_columns_to_add.getByPosition(i);
        block.insert(ColumnWithTypeAndName{std::move(right_columns[i]), column.type, column.name});
    }
}

/// Split block into processed (result) and not processed. Not processed block would be joined next time.
template <bool is_all>
std::optional<MergeJoin::NotProcessed> MergeJoin::extraBlock(Block & processed, MutableColumns && left_columns, MutableColumns && right_columns,
                                    size_t left_position [[maybe_unused]], size_t left_key_tail [[maybe_unused]],
                                    size_t right_position [[maybe_unused]], size_t right_block_number [[maybe_unused]],
                                    MergeJoin::LeftMatchedBitmap && left_row_matched [[maybe_unused]])
{
    std::optional<NotProcessed> not_processed;

    if constexpr (is_all)
    {
        not_processed = NotProcessed{
            {processed.cloneEmpty()}, left_position, left_key_tail, right_position, right_block_number, std::move(left_row_matched)};
        not_processed->block.swap(processed);

        changeLeftColumns(processed, std::move(left_columns));
        addRightColumns(processed, std::move(right_columns));
    }

    return not_processed;
}

template <bool in_memory>
size_t MergeJoin::rightBlocksCount() const
{
    if constexpr (!in_memory)
        return flushed_right_blocks.size();
    else
        return loaded_right_blocks.size();
}

template <bool in_memory>
std::shared_ptr<Block> MergeJoin::loadRightBlock(size_t pos) const
{
    if constexpr (!in_memory)
    {
        auto load_func = [&]() -> std::shared_ptr<Block>
        {
            auto input = flushed_right_blocks[pos].getReadStream();
            auto result = std::make_shared<Block>(input->read());
            if (Block eof_block = input->read(); !eof_block.empty())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected one block per file, got block {} in file {}",
                    eof_block.dumpStructure(), flushed_right_blocks[pos].getHolder()->describeFilePath());
            }
            return result;
        };

        return cached_right_blocks->getOrSet(pos, load_func).first;
    }
    else
        return loaded_right_blocks[pos];
}

void MergeJoin::initRightTableWriter()
{
    disk_writer = std::make_unique<SortedBlocksWriter>(size_limits, table_join->getTempDataOnDisk(),
                    right_sample_block, right_sort_description, max_rows_in_right_block, max_files_to_merge);
    disk_writer->addBlocks(right_blocks);
    right_blocks.clear();
}

/// Stream from not joined earlier rows of the right table.
class NotJoinedMerge final : public NotJoinedBlocks::RightColumnsFiller
{
public:
    NotJoinedMerge(const MergeJoin & parent_, UInt64 max_block_size_)
        : parent(parent_), max_block_size(max_block_size_)
    {}

    Block getEmptyBlock() override { return parent.modifyRightBlock(parent.right_sample_block).cloneEmpty(); }

    size_t fillColumns(MutableColumns & columns_right) override
    {
        const RowBitmaps & bitmaps = *parent.used_rows_bitmap;
        size_t rows_added = 0;

        size_t blocks_count = parent.getRightBlocksCount();
        for (; block_number < blocks_count; ++block_number)
        {
            auto right_block = parent.getRightBlock(block_number);

            if (bitmaps.used(block_number))
            {
                IColumn::Filter not_used = bitmaps.getNotUsed(block_number);

                for (const auto & row : not_used)
                    if (row)
                        ++rows_added;

                for (size_t col = 0; col < columns_right.size(); ++col)
                {
                    /// TODO: IColumn::filteredInsertRangeFrom() ?
                    ColumnPtr portion = right_block->getByPosition(col).column->filter(not_used, 1);
                    columns_right[col]->insertRangeFrom(*portion, 0, portion->size());
                }
            }
            else
            {
                rows_added += right_block->rows();
                for (size_t col = 0; col < columns_right.size(); ++col)
                {
                    const IColumn & column = *right_block->getByPosition(col).column;
                    columns_right[col]->insertRangeFrom(column, 0, column.size());
                }
            }

            if (rows_added >= max_block_size)
            {
                ++block_number;
                break;
            }
        }

        return rows_added;
    }

private:
    const MergeJoin & parent;
    size_t max_block_size;
    size_t block_number = 0;
};


IBlocksStreamPtr MergeJoin::getNonJoinedBlocks(
    const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const
{
    if (table_join->strictness() == JoinStrictness::All && (is_right || is_full))
    {
        size_t left_columns_count = left_sample_block.columns();
        chassert(left_columns_count == result_sample_block.columns() - right_columns_to_add.columns());
        auto non_joined = std::make_unique<NotJoinedMerge>(*this, max_block_size);
        return std::make_unique<NotJoinedBlocks>(std::move(non_joined), result_sample_block, left_columns_count, *table_join);
    }
    return nullptr;
}

StepAnalysisReport MergeJoin::getAnalysisReport() const
{
    StepAnalysisReport report;

    const UInt64 left_rows = total_left_rows.load(std::memory_order_relaxed);
    const UInt64 matched_left = matched_left_rows.load(std::memory_order_relaxed);
    report.push_back({MetricGroupKey::Left, joinSideMetrics(left_rows, matched_left)});

    const std::optional<UInt64> matched_right
        = used_rows_bitmap ? std::optional<UInt64>(used_rows_bitmap->countUsed()) : std::nullopt;

    const bool in_memory = is_in_memory.load(std::memory_order_relaxed);
    MetricList right_metrics = joinSideMetrics(getTotalRowCount(), matched_right);
    right_metrics.emplace_back(MetricKey::Size, getTotalByteCount());
    right_metrics.emplace_back(MetricKey::Blocks, getRightBlocksCount());
    right_metrics.emplace_back(MetricKey::Storage, std::string(in_memory ? "in-memory" : "external"));
    if (!in_memory)
        right_metrics.emplace_back(MetricKey::Spilled, right_spilled_compressed_bytes);
    report.push_back({MetricGroupKey::Right, std::move(right_metrics)});

    MetricList build_metrics;
    build_metrics.emplace_back(MetricKey::SortTime, build_sort_time_ns);
    report.push_back({MetricGroupKey::Build, std::move(build_metrics)});

    MetricList probe_metrics;
    probe_metrics.emplace_back(MetricKey::SortTime, probe_sort_time_ns.load(std::memory_order_relaxed));
    report.push_back({MetricGroupKey::Probe, std::move(probe_metrics)});

    return report;
}

bool MergeJoin::needConditionJoinColumn() const
{
    return !mask_column_name_left.empty() || !mask_column_name_right.empty();
}

void MergeJoin::addConditionJoinColumn(Block & block, JoinTableSide block_side) const
{
    if (needConditionJoinColumn())
    {
        if (block_side == JoinTableSide::Left)
            block.insert(conditionColumnToJoinable(block, mask_column_name_left, block_side));
        else
            block.insert(conditionColumnToJoinable(block, mask_column_name_right, block_side));
    }
}

bool MergeJoin::isSupported(const std::shared_ptr<TableJoin> & table_join)
{
    return isSupported(table_join->kind(), table_join->strictness()) && table_join->oneDisjunct();
}

bool MergeJoin::isSupported(JoinKind kind, JoinStrictness strictness)
{
    bool is_any = (strictness == JoinStrictness::Any);
    bool is_all = (strictness == JoinStrictness::All);
    bool is_semi = (strictness == JoinStrictness::Semi);

    bool all_join = is_all && (isInner(kind) || isLeft(kind) || isRight(kind) || isFull(kind));
    bool special_left = isInnerOrLeft(kind) && (is_any || is_semi);

    return all_join || special_left;
}

MergeJoin::RightBlockInfo::RightBlockInfo(std::shared_ptr<Block> block_, size_t block_number_, size_t & skip_, RowBitmaps * bitmaps_)
    : block(block_)
    , block_number(block_number_)
    , skip(skip_)
    , bitmaps(bitmaps_)
{}

MergeJoin::RightBlockInfo::~RightBlockInfo()
{
    if (used_bitmap)
        bitmaps->applyOr(block_number, std::move(*used_bitmap));
}

void MergeJoin::RightBlockInfo::setUsed(size_t start, size_t length)
{
    if (bitmaps)
    {
        if (!used_bitmap)
            used_bitmap = std::make_unique<std::vector<bool>>(block->rows(), false);

        for (size_t i = 0; i < length; ++i)
            (*used_bitmap)[start + i] = true;
    }
}

}
