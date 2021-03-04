#include <limits>

#include <Core/NamesAndTypes.h>
#include <Core/SortCursor.h>
#include <Columns/ColumnNullable.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/sortBlock.h>
#include <Interpreters/join_common.h>
#include <DataStreams/materializeBlock.h>
#include <DataStreams/TemporaryFileStream.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <DataStreams/BlocksListBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}

namespace
{

template <bool has_left_nulls, bool has_right_nulls>
int nullableCompareAt(const IColumn & left_column, const IColumn & right_column, size_t lhs_pos, size_t rhs_pos)
{
    static constexpr int null_direction_hint = 1;

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

Block extractMinMax(const Block & block, const Block & keys)
{
    if (block.rows() == 0)
        throw Exception("Unexpected empty block", ErrorCodes::LOGICAL_ERROR);

    Block min_max = keys.cloneEmpty();
    MutableColumns columns = min_max.mutateColumns();

    for (size_t i = 0; i < columns.size(); ++i)
    {
        const auto & src_column = block.getByName(keys.getByPosition(i).name);

        columns[i]->insertFrom(*src_column.column, 0);
        columns[i]->insertFrom(*src_column.column, block.rows() - 1);
    }

    min_max.setColumns(std::move(columns));
    return min_max;
}

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

using Range = MergeJoinEqualRange;


class MergeJoinCursor
{
public:
    MergeJoinCursor(const Block & block, const SortDescription & desc_)
        : impl(SortCursorImpl(block, desc_))
    {
        /// SortCursorImpl can work with permutation, but MergeJoinCursor can't.
        if (impl.permutation)
            throw Exception("Logical error: MergeJoinCursor doesn't support permutation", ErrorCodes::LOGICAL_ERROR);
    }

    size_t position() const { return impl.getRow(); }
    size_t end() const { return impl.rows; }
    bool atEnd() const { return impl.getRow() >= impl.rows; }
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

    Range getNextEqualRange(MergeJoinCursor & rhs)
    {
        if (has_left_nullable && has_right_nullable)
            return getNextEqualRangeImpl<true, true>(rhs);
        else if (has_left_nullable)
            return getNextEqualRangeImpl<true, false>(rhs);
        else if (has_right_nullable)
            return getNextEqualRangeImpl<false, true>(rhs);
        return getNextEqualRangeImpl<false, false>(rhs);
    }

    int intersect(const Block & min_max, const Names & key_names)
    {
        if (end() == 0 || min_max.rows() != 2)
            throw Exception("Unexpected block size", ErrorCodes::LOGICAL_ERROR);

        size_t last_position = end() - 1;
        int first_vs_max = 0;
        int last_vs_min = 0;

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
    bool has_left_nullable = false;
    bool has_right_nullable = false;

    template <bool left_nulls, bool right_nulls>
    Range getNextEqualRangeImpl(MergeJoinCursor & rhs)
    {
        while (!atEnd() && !rhs.atEnd())
        {
            int cmp = compareAtCursor<left_nulls, right_nulls>(rhs);
            if (cmp < 0)
                impl.next();
            else if (cmp > 0)
                rhs.impl.next();
            else if (!cmp)
                return Range{impl.getRow(), rhs.impl.getRow(), getEqualLength(), rhs.getEqualLength()};
        }

        return Range{impl.getRow(), rhs.impl.getRow(), 0, 0};
    }

    template <bool left_nulls, bool right_nulls>
    int ALWAYS_INLINE compareAtCursor(const MergeJoinCursor & rhs) const
    {
        for (size_t i = 0; i < impl.sort_columns_size; ++i)
        {
            const auto * left_column = impl.sort_columns[i];
            const auto * right_column = rhs.impl.sort_columns[i];

            int res = nullableCompareAt<left_nulls, right_nulls>(*left_column, *right_column, impl.getRow(), rhs.impl.getRow());
            if (res)
                return res;
        }
        return 0;
    }

    /// Expects !atEnd()
    size_t getEqualLength()
    {
        size_t pos = impl.getRow() + 1;
        for (; pos < impl.rows; ++pos)
            if (!samePrev(pos))
                break;
        return pos - impl.getRow();
    }

    /// Expects lhs_pos > 0
    bool ALWAYS_INLINE samePrev(size_t lhs_pos) const
    {
        for (size_t i = 0; i < impl.sort_columns_size; ++i)
            if (impl.sort_columns[i]->compareAt(lhs_pos - 1, lhs_pos, *(impl.sort_columns[i]), 1) != 0)
                return false;
        return true;
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
    for (const auto & key_name : keys)
    {
        merge.emplace_back(SortColumnDescription(key_name, 1, 1));

        if (!unique_keys.count(key_name))
        {
            unique_keys.insert(key_name);
            sort.emplace_back(SortColumnDescription(key_name, 1, 1));
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

void copyRightRange(const Block & right_block, const Block & right_columns_to_add, MutableColumns & columns,
                    size_t row_position, size_t rows_to_add)
{
    for (size_t i = 0; i < right_columns_to_add.columns(); ++i)
    {
        const auto & src_column = right_block.getByName(right_columns_to_add.getByPosition(i).name).column;
        auto & dst_column = columns[i];
        auto * dst_nullable = typeid_cast<ColumnNullable *>(dst_column.get());

        if (dst_nullable && !isColumnNullable(*src_column))
            dst_nullable->insertManyFromNotNullable(*src_column, row_position, rows_to_add);
        else
            dst_column->insertManyFrom(*src_column, row_position, rows_to_add);
    }
}

void joinEqualsAnyLeft(const Block & right_block, const Block & right_columns_to_add, MutableColumns & right_columns, const Range & range)
{
    copyRightRange(right_block, right_columns_to_add, right_columns, range.right_start, range.left_length);
}

template <bool is_all>
bool joinEquals(const Block & left_block, const Block & right_block, const Block & right_columns_to_add,
                MutableColumns & left_columns, MutableColumns & right_columns, Range & range, size_t max_rows [[maybe_unused]])
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
            copyRightRange(right_block, right_columns_to_add, right_columns, row_position, left_rows_to_add);
        }
    }
    else
    {
        size_t left_rows_to_add = range.left_length;
        copyLeftRange(left_block, left_columns, range.left_start, left_rows_to_add);
        copyRightRange(right_block, right_columns_to_add, right_columns, range.right_start, left_rows_to_add);
    }

    return one_more;
}

template <bool copy_left>
void joinInequalsLeft(const Block & left_block, MutableColumns & left_columns,
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

}


MergeJoin::MergeJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_)
    : table_join(table_join_)
    , size_limits(table_join->sizeLimits())
    , right_sample_block(right_sample_block_)
    , nullable_right_side(table_join->forceNullableRight())
    , nullable_left_side(table_join->forceNullableLeft())
    , is_any_join(table_join->strictness() == ASTTableJoin::Strictness::Any)
    , is_all_join(table_join->strictness() == ASTTableJoin::Strictness::All)
    , is_semi_join(table_join->strictness() == ASTTableJoin::Strictness::Semi)
    , is_inner(isInner(table_join->kind()))
    , is_left(isLeft(table_join->kind()))
    , is_right(isRight(table_join->kind()))
    , is_full(isFull(table_join->kind()))
    , max_joined_block_rows(table_join->maxJoinedBlockRows())
    , max_rows_in_right_block(table_join->maxRowsInRightBlock())
    , max_files_to_merge(table_join->maxFilesToMerge())
{
    switch (table_join->strictness())
    {
        case ASTTableJoin::Strictness::All:
            break;
        case ASTTableJoin::Strictness::Any:
        case ASTTableJoin::Strictness::Semi:
            if (!is_left && !is_inner)
                throw Exception("Not supported. MergeJoin supports SEMI and ANY variants only for LEFT and INNER JOINs.",
                                ErrorCodes::NOT_IMPLEMENTED);
            break;
        default:
            throw Exception("Not supported. MergeJoin supports ALL, ANY and SEMI JOINs variants.", ErrorCodes::NOT_IMPLEMENTED);
    }

    if (!max_rows_in_right_block)
        throw Exception("partial_merge_join_rows_in_right_blocks cannot be zero", ErrorCodes::PARAMETER_OUT_OF_BOUND);

    if (max_files_to_merge < 2)
        throw Exception("max_files_to_merge cannot be less than 2", ErrorCodes::PARAMETER_OUT_OF_BOUND);

    if (!size_limits.hasLimits())
    {
        size_limits.max_bytes = table_join->defaultMaxBytes();
        if (!size_limits.max_bytes)
            throw Exception("No limit for MergeJoin (max_rows_in_join, max_bytes_in_join or default_max_bytes_in_join have to be set)",
                            ErrorCodes::PARAMETER_OUT_OF_BOUND);
    }

    table_join->splitAdditionalColumns(right_sample_block, right_table_keys, right_columns_to_add);
    JoinCommon::removeLowCardinalityInplace(right_table_keys);

    const NameSet required_right_keys = table_join->requiredRightKeys();
    for (const auto & column : right_table_keys)
        if (required_right_keys.count(column.name))
            right_columns_to_add.insert(ColumnWithTypeAndName{nullptr, column.type, column.name});

    JoinCommon::createMissedColumns(right_columns_to_add);

    if (nullable_right_side)
        JoinCommon::convertColumnsToNullable(right_columns_to_add);

    makeSortAndMerge(table_join->keyNamesLeft(), left_sort_description, left_merge_description);
    makeSortAndMerge(table_join->keyNamesRight(), right_sort_description, right_merge_description);

    /// Temporary disable 'partial_merge_join_left_table_buffer_bytes' without 'partial_merge_join_optimizations'
    if (table_join->enablePartialMergeJoinOptimizations())
        if (size_t max_bytes = table_join->maxBytesInLeftBuffer())
            left_blocks_buffer = std::make_shared<SortedBlocksBuffer>(left_sort_description, max_bytes);
}

void MergeJoin::setTotals(const Block & totals_block)
{
    totals = totals_block;
    mergeRightBlocks();

    if (is_right || is_full)
        used_rows_bitmap = std::make_shared<RowBitmaps>(getRightBlocksCount());
}

void MergeJoin::joinTotals(Block & block) const
{
    JoinCommon::joinTotals(totals, right_columns_to_add, *table_join, block);
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
    std::unique_lock lock(rwlock);

    if (right_blocks.empty())
        return;

    auto stream = std::make_shared<BlocksListBlockInputStream>(std::move(right_blocks.blocks));
    Pipe source(std::make_shared<SourceFromInputStream>(std::move(stream)));
    right_blocks.clear();

    QueryPipeline pipeline;
    pipeline.init(std::move(source));

    /// TODO: there should be no split keys by blocks for RIGHT|FULL JOIN
    pipeline.addTransform(std::make_shared<MergeSortingTransform>(pipeline.getHeader(), right_sort_description, max_rows_in_right_block, 0, 0, 0, 0, nullptr, 0));

    auto sorted_input = PipelineExecutingBlockInputStream(std::move(pipeline));

    while (Block block = sorted_input.read())
    {
        if (!block.rows())
            continue;

        if (skip_not_intersected)
            min_max_right_blocks.emplace_back(extractMinMax(block, right_table_keys));
        right_blocks.countBlockSize(block);
        loaded_right_blocks.emplace_back(std::make_shared<Block>(std::move(block)));
    }
}

void MergeJoin::mergeFlushedRightBlocks()
{
    std::unique_lock lock(rwlock);

    auto callback = [&](const Block & block)
    {
        if (skip_not_intersected)
            min_max_right_blocks.emplace_back(extractMinMax(block, right_table_keys));
        right_blocks.countBlockSize(block);
    };

    flushed_right_blocks = disk_writer->finishMerge(callback);
    disk_writer.reset();

    /// Get memory limit or approximate it from row limit and bytes per row factor
    UInt64 memory_limit = size_limits.max_bytes;
    UInt64 rows_limit = size_limits.max_rows;
    if (!memory_limit && rows_limit)
        memory_limit = right_blocks.bytes * rows_limit / right_blocks.row_count;

    cached_right_blocks = std::make_unique<Cache>(memory_limit);
}

bool MergeJoin::saveRightBlock(Block && block)
{
    if (is_in_memory)
    {
        std::unique_lock lock(rwlock);

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
    Block block = materializeBlock(src_block);
    JoinCommon::removeLowCardinalityInplace(block, table_join->keyNamesRight());
    return block;
}

bool MergeJoin::addJoinedBlock(const Block & src_block, bool)
{
    Block block = modifyRightBlock(src_block);

    sortBlock(block, right_sort_description);
    return saveRightBlock(std::move(block));
}

void MergeJoin::joinBlock(Block & block, ExtraBlockPtr & not_processed)
{
    if (block)
    {
        JoinCommon::checkTypesOfKeys(block, table_join->keyNamesLeft(), right_table_keys, table_join->keyNamesRight());
        materializeBlockInplace(block);
        JoinCommon::removeLowCardinalityInplace(block, table_join->keyNamesLeft(), false);

        sortBlock(block, left_sort_description);

        if (nullable_left_side)
            JoinCommon::convertColumnsToNullable(block);
    }

    if (!not_processed && left_blocks_buffer)
    {
        if (!block || block.rows())
            block = left_blocks_buffer->exchange(std::move(block));
        if (!block)
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
        not_processed = std::make_shared<NotProcessed>(NotProcessed{{}, 0, 0, 0});

    JoinCommon::restoreLowCardinalityInplace(block);
}

template <bool in_memory, bool is_all>
void MergeJoin::joinSortedBlock(Block & block, ExtraBlockPtr & not_processed)
{
    //std::shared_lock lock(rwlock);

    size_t rows_to_reserve = is_left ? block.rows() : 0;
    MutableColumns left_columns = makeMutableColumns(block, (is_all ? rows_to_reserve : 0));
    MutableColumns right_columns = makeMutableColumns(right_columns_to_add, rows_to_reserve);
    MergeJoinCursor left_cursor(block, left_merge_description);
    size_t left_key_tail = 0;
    size_t skip_right = 0;
    size_t right_blocks_count = rightBlocksCount<in_memory>();

    size_t starting_right_block = 0;
    if (not_processed)
    {
        auto & continuation = static_cast<NotProcessed &>(*not_processed);
        left_cursor.nextN(continuation.left_position);
        skip_right = continuation.right_position;
        starting_right_block = continuation.right_block;
        not_processed.reset();
    }

    bool with_left_inequals = (is_left && !is_semi_join) || is_full;
    if (with_left_inequals)
    {
        for (size_t i = starting_right_block; i < right_blocks_count; ++i)
        {
            if (left_cursor.atEnd())
                break;

            if (skip_not_intersected)
            {
                int intersection = left_cursor.intersect(min_max_right_blocks[i], table_join->keyNamesRight());
                if (intersection < 0)
                    break; /// (left) ... (right)
                if (intersection > 0)
                    continue; /// (right) ... (left)
            }

            /// Use skip_right as ref. It would be updated in join.
            RightBlockInfo right_block(loadRightBlock<in_memory>(i), i, skip_right, used_rows_bitmap.get());

            if (!leftJoin<is_all>(left_cursor, block, right_block, left_columns, right_columns, left_key_tail))
            {
                not_processed = extraBlock<is_all>(block, std::move(left_columns), std::move(right_columns),
                                                   left_cursor.position(), skip_right, i);
                return;
            }
        }

        left_cursor.nextN(left_key_tail);
        joinInequalsLeft<is_all>(block, left_columns, right_columns_to_add, right_columns, left_cursor.position(), left_cursor.end());
        //left_cursor.nextN(left_cursor.end() - left_cursor.position());

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
                int intersection = left_cursor.intersect(min_max_right_blocks[i], table_join->keyNamesRight());
                if (intersection < 0)
                    break; /// (left) ... (right)
                if (intersection > 0)
                    continue; /// (right) ... (left)
            }

            /// Use skip_right as ref. It would be updated in join.
            RightBlockInfo right_block(loadRightBlock<in_memory>(i), i, skip_right, used_rows_bitmap.get());

            if constexpr (is_all)
            {
                if (!allInnerJoin(left_cursor, block, right_block, left_columns, right_columns, left_key_tail))
                {
                    not_processed = extraBlock<is_all>(block, std::move(left_columns), std::move(right_columns),
                                                       left_cursor.position(), skip_right, i);
                    return;
                }
            }
            else
                semiLeftJoin(left_cursor, block, right_block, left_columns, right_columns);
        }

        left_cursor.nextN(left_key_tail);
        changeLeftColumns(block, std::move(left_columns));
        addRightColumns(block, std::move(right_columns));
    }
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
                         MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail)
{
    const Block & right_block = *right_block_info.block;
    MergeJoinCursor right_cursor(right_block, right_merge_description);
    left_cursor.setCompareNullability(right_cursor);

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

        Range range = left_cursor.getNextEqualRange(right_cursor);

        joinInequalsLeft<is_all>(left_block, left_columns, right_columns_to_add, right_columns, left_unequal_position, range.left_start);

        if (range.empty())
            break;

        if constexpr (is_all)
        {
            right_block_info.setUsed(range.right_start, range.right_length);

            size_t max_rows = maxRangeRows(left_columns[0]->size(), max_joined_block_rows);

            if (!joinEquals<true>(left_block, right_block, right_columns_to_add, left_columns, right_columns, range, max_rows))
            {
                right_cursor.nextN(range.right_length);
                right_block_info.skip = right_cursor.position();
                return false;
            }
        }
        else
            joinEqualsAnyLeft(right_block, right_columns_to_add, right_columns, range);

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
        left_cursor.nextN(range.left_length);
    }

    return true;
}

bool MergeJoin::allInnerJoin(MergeJoinCursor & left_cursor, const Block & left_block, RightBlockInfo & right_block_info,
                          MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail)
{
    const Block & right_block = *right_block_info.block;
    MergeJoinCursor right_cursor(right_block, right_merge_description);
    left_cursor.setCompareNullability(right_cursor);

    /// Set right cursor position in first continuation right block
    right_cursor.nextN(right_block_info.skip);
    right_block_info.skip = 0;

    while (!left_cursor.atEnd() && !right_cursor.atEnd())
    {
        Range range = left_cursor.getNextEqualRange(right_cursor);
        if (range.empty())
            break;

        right_block_info.setUsed(range.right_start, range.right_length);

        size_t max_rows = maxRangeRows(left_columns[0]->size(), max_joined_block_rows);

        if (!joinEquals<true>(left_block, right_block, right_columns_to_add, left_columns, right_columns, range, max_rows))
        {
            right_cursor.nextN(range.right_length);
            right_block_info.skip = right_cursor.position();
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

bool MergeJoin::semiLeftJoin(MergeJoinCursor & left_cursor, const Block & left_block, const RightBlockInfo & right_block_info,
                             MutableColumns & left_columns, MutableColumns & right_columns)
{
    const Block & right_block = *right_block_info.block;
    MergeJoinCursor right_cursor(right_block, right_merge_description);
    left_cursor.setCompareNullability(right_cursor);

    while (!left_cursor.atEnd() && !right_cursor.atEnd())
    {
        Range range = left_cursor.getNextEqualRange(right_cursor);
        if (range.empty())
            break;

        joinEquals<false>(left_block, right_block, right_columns_to_add, left_columns, right_columns, range, 0);

        right_cursor.nextN(range.right_length);
        left_cursor.nextN(range.left_length);
    }

    return true;
}

void MergeJoin::changeLeftColumns(Block & block, MutableColumns && columns) const
{
    if (is_left && is_any_join)
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
ExtraBlockPtr MergeJoin::extraBlock(Block & processed, MutableColumns && left_columns, MutableColumns && right_columns,
                                    size_t left_position [[maybe_unused]], size_t right_position [[maybe_unused]],
                                    size_t right_block_number [[maybe_unused]])
{
    ExtraBlockPtr not_processed;

    if constexpr (is_all)
    {
        not_processed = std::make_shared<NotProcessed>(
            NotProcessed{{processed.cloneEmpty()}, left_position, right_position, right_block_number});
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
            TemporaryFileStream input(flushed_right_blocks[pos]->path(), materializeBlock(right_sample_block));
            return std::make_shared<Block>(input.block_in->read());
        };

        return cached_right_blocks->getOrSet(pos, load_func).first;
    }
    else
        return loaded_right_blocks[pos];
}

void MergeJoin::initRightTableWriter()
{
    disk_writer = std::make_unique<SortedBlocksWriter>(size_limits, table_join->getTemporaryVolume(),
                    right_sample_block, right_sort_description, max_rows_in_right_block, max_files_to_merge,
                    table_join->temporaryFilesCodec());
    disk_writer->addBlocks(right_blocks);
    right_blocks.clear();
}

/// Stream from not joined earlier rows of the right table.
class NonMergeJoinedBlockInputStream : private NotJoined, public IBlockInputStream
{
public:
    NonMergeJoinedBlockInputStream(const MergeJoin & parent_, const Block & result_sample_block_, UInt64 max_block_size_)
        : NotJoined(*parent_.table_join,
                    parent_.modifyRightBlock(parent_.right_sample_block),
                    parent_.right_sample_block,
                    result_sample_block_)
        , parent(parent_)
        , max_block_size(max_block_size_)
    {}

    String getName() const override { return "NonMergeJoined"; }
    Block getHeader() const override { return result_sample_block; }

protected:
    Block readImpl() override
    {
        if (parent.getRightBlocksCount())
            return createBlock();
        return {};
    }

private:
    const MergeJoin & parent;
    size_t max_block_size;
    size_t block_number = 0;

    Block createBlock()
    {
        MutableColumns columns_right = saved_block_sample.cloneEmptyColumns();

        size_t rows_added = fillColumns(columns_right);
        if (!rows_added)
            return {};

        correctLowcardAndNullability(columns_right);

        Block res = result_sample_block.cloneEmpty();
        addLeftColumns(res, rows_added);
        addRightColumns(res, columns_right);
        copySameKeys(res);
        return res;
    }

    size_t fillColumns(MutableColumns & columns_right)
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
                break;
        }

        return rows_added;
    }
};


BlockInputStreamPtr MergeJoin::createStreamWithNonJoinedRows(const Block & result_sample_block, UInt64 max_block_size) const
{
    if (table_join->strictness() == ASTTableJoin::Strictness::All && (is_right || is_full))
        return std::make_shared<NonMergeJoinedBlockInputStream>(*this, result_sample_block, max_block_size);
    return {};
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
