#include <Core/NamesAndTypes.h>
#include <Core/SortCursor.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/AnalyzedJoin.h>
#include <Interpreters/sortBlock.h>
#include <DataStreams/materializeBlock.h>
#include <DataStreams/MergeSortingBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int NOT_IMPLEMENTED;
}

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
    {}

    size_t position() const { return impl.pos; }
    bool atEnd() const { return impl.pos >= impl.rows; }
    void nextN(size_t num) { impl.pos += num; }

    int compareAt(const MergeJoinCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        int res = 0;
        for (size_t i = 0; i < impl.sort_columns_size; ++i)
        {
            res = impl.sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl.sort_columns[i]), 1);
            if (res)
                break;
        }
        return res;
    }

    bool sameNext(size_t lhs_pos) const
    {
        if (impl.isLast())
            return false;

        for (size_t i = 0; i < impl.sort_columns_size; ++i)
            if (impl.sort_columns[i]->compareAt(lhs_pos, lhs_pos + 1, *(impl.sort_columns[i]), 1) != 0)
                return false;
        return true;
    }

    size_t getEqualLength()
    {
        if (atEnd())
            return 0;

        size_t pos = impl.pos;
        for (; pos < impl.rows; ++pos)
            if (!sameNext(pos))
                break;

        return pos - impl.pos + 1;
    }

    Range getNextEqualRange(MergeJoinCursor & rhs)
    {
        while (!atEnd() && !rhs.atEnd())
        {
            int cmp = compareAt(rhs, impl.pos, rhs.impl.pos);
            if (cmp < 0)
                impl.next();
            if (cmp > 0)
                rhs.impl.next();
            if (!cmp)
            {
                Range range{impl.pos, rhs.impl.pos, 0, 0};
                range.left_length = getEqualLength();
                range.right_length = rhs.getEqualLength();
                return range;
            }
        }

        return Range{impl.pos, rhs.impl.pos, 0, 0};
    }

private:
    SortCursorImpl impl;
};

static void makeSortAndMerge(const Names & keys, SortDescription & sort, SortDescription & merge)
{
    NameSet unique_keys;
    for (auto & key_name : keys)
    {
        merge.emplace_back(SortColumnDescription(key_name, 1, 1));

        if (!unique_keys.count(key_name))
        {
            unique_keys.insert(key_name);
            sort.emplace_back(SortColumnDescription(key_name, 1, 1));
        }
    }
}


MergeJoin::MergeJoin(std::shared_ptr<AnalyzedJoin> table_join_, const Block & right_sample_block)
    : table_join(table_join_)
    , nullable_right_side(table_join->forceNullabelRight())
{
    if (!isLeft(table_join->kind()) && !isInner(table_join->kind()))
        throw Exception("Partial merge supported for LEFT and INNER JOINs only", ErrorCodes::NOT_IMPLEMENTED);

    JoinCommon::extractKeysForJoin(table_join->keyNamesRight(), right_sample_block, right_table_keys, right_columns_to_add);

    const NameSet required_right_keys = table_join->requiredRightKeys();
    for (const auto & column : right_table_keys)
        if (required_right_keys.count(column.name))
            right_columns_to_add.insert(ColumnWithTypeAndName{nullptr, column.type, column.name});

    JoinCommon::createMissedColumns(right_columns_to_add);

    if (nullable_right_side)
        JoinCommon::convertColumnsToNullable(right_columns_to_add);

    makeSortAndMerge(table_join->keyNamesLeft(), left_sort_description, left_merge_description);
    makeSortAndMerge(table_join->keyNamesRight(), right_sort_description, right_merge_description);
}

void MergeJoin::setTotals(const Block & totals_block)
{
    totals = totals_block;
    mergeRightBlocks();
}

void MergeJoin::mergeRightBlocks()
{
    const size_t max_merged_block_size = 128 * 1024 * 1024;

    Blocks unsorted_blocks;
    unsorted_blocks.reserve(right_blocks.size());
    for (const auto & block : right_blocks)
        unsorted_blocks.push_back(block);

    /// TODO: there should be no splitted keys by blocks for RIGHT|FULL JOIN
    MergeSortingBlocksBlockInputStream stream(unsorted_blocks, right_sort_description, max_merged_block_size);

    right_blocks.clear();
    while (Block block = stream.read())
        right_blocks.push_back(block);
}

bool MergeJoin::addJoinedBlock(const Block & src_block)
{
    Block block = src_block;
    sortBlock(block, right_sort_description);

    std::unique_lock lock(rwlock);

    right_blocks.push_back(block);
    right_blocks_row_count += block.rows();
    right_blocks_bytes += block.bytes();

    return table_join->sizeLimits().check(right_blocks_row_count, right_blocks_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void MergeJoin::joinBlock(Block & block)
{
    JoinCommon::checkTypesOfKeys(block, table_join->keyNamesLeft(), right_table_keys, table_join->keyNamesRight());
    sortBlock(block, left_sort_description);

    std::shared_lock lock(rwlock);

    if (isLeft(table_join->kind()))
    {
        MutableColumns right_columns = makeMutableColumns(right_columns_to_add);

        MergeJoinCursor left_cursor(block, left_merge_description);
        for (auto it = right_blocks.begin(); it != right_blocks.end(); ++it)
        {
            if (left_cursor.atEnd())
                break;
            leftJoin(left_cursor, *it, right_columns);
        }

        appendRightColumns(block, std::move(right_columns));
    }
    else if (isInner(table_join->kind()))
    {
        MutableColumns left_columns = makeMutableColumns(block);
        MutableColumns right_columns = makeMutableColumns(right_columns_to_add);

        MergeJoinCursor left_cursor(block, left_merge_description);
        for (auto it = right_blocks.begin(); it != right_blocks.end(); ++it)
        {
            if (left_cursor.atEnd())
                break;
            innerJoin(left_cursor, block, *it, left_columns, right_columns);
        }

        block.clear();
        appendRightColumns(block, std::move(left_columns));
        appendRightColumns(block, std::move(right_columns));
    }
}

void MergeJoin::leftJoin(MergeJoinCursor & left_cursor, const Block & right_block, MutableColumns & right_columns)
{
    MergeJoinCursor right_cursor(right_block, right_merge_description);

    while (!left_cursor.atEnd() && !right_cursor.atEnd())
    {
        size_t left_position = left_cursor.position();
        Range range = left_cursor.getNextEqualRange(right_cursor);

        if (left_position < range.left_start)
            appendRightNulls(right_columns, range.left_start - left_position);

        if (range.empty())
            break;

        leftJoinEquals(right_block, right_columns, range);
        right_cursor.nextN(range.right_length);

        /// TODO: Do not run over last left keys for ALL JOIN (cause of possible duplicates in next right block)
        //if (!right_cursor.atEnd())
        left_cursor.nextN(range.left_length);
    }
}

void MergeJoin::innerJoin(MergeJoinCursor & left_cursor, const Block & left_block, const Block & right_block,
                          MutableColumns & left_columns, MutableColumns & right_columns)
{
    MergeJoinCursor right_cursor(right_block, right_merge_description);

    while (!left_cursor.atEnd() && !right_cursor.atEnd())
    {
        Range range = left_cursor.getNextEqualRange(right_cursor);
        if (range.empty())
            break;

        innerJoinEquals(left_block, right_block, left_columns, right_columns, range);
        right_cursor.nextN(range.right_length);

        /// TODO: Do not run over last left keys for ALL JOIN (cause of possible duplicates in next right block)
        //if (!right_cursor.atEnd())
        left_cursor.nextN(range.left_length);
    }
}

MutableColumns MergeJoin::makeMutableColumns(const Block & block)
{
    MutableColumns columns;
    columns.reserve(block.columns());

    for (const auto & src_column : block)
        columns.push_back(src_column.column->cloneEmpty());
    return columns;
}

void MergeJoin::appendRightColumns(Block & block, MutableColumns && right_columns)
{
    for (size_t i = 0; i < right_columns_to_add.columns(); ++i)
    {
        const auto & column = right_columns_to_add.getByPosition(i);
        block.insert(ColumnWithTypeAndName{std::move(right_columns[i]), column.type, column.name});
    }
}

void MergeJoin::appendRightNulls(MutableColumns & right_columns, size_t rows_to_add)
{
    for (auto & column : right_columns)
        for (size_t i = 0; i < rows_to_add; ++i)
            column->insertDefault();
}

void MergeJoin::leftJoinEquals(const Block & right_block, MutableColumns & right_columns, const Range & range)
{
    bool any = table_join->strictness() == ASTTableJoin::Strictness::Any;

    size_t left_rows_to_insert = range.left_length;
    size_t right_rows_to_insert = any ? 1 : range.right_length;

    size_t row_position = range.right_start;
    for (size_t right_row = 0; right_row < right_rows_to_insert; ++right_row, ++row_position)
    {
        for (size_t i = 0; i < right_columns_to_add.columns(); ++i)
        {
            const auto & src_column = right_block.getByName(right_columns_to_add.getByPosition(i).name);
            auto & dst_column = right_columns[i];

            for (size_t left_row = 0; left_row < left_rows_to_insert; ++left_row)
                dst_column->insertFrom(*src_column.column, row_position);
        }
    }
}

void MergeJoin::innerJoinEquals(const Block & left_block, const Block & right_block,
                                MutableColumns & left_columns, MutableColumns & right_columns, const Range & range)
{
    bool any = table_join->strictness() == ASTTableJoin::Strictness::Any;

    size_t left_rows_to_insert = range.left_length;
    size_t right_rows_to_insert = any ? 1 : range.right_length;

    size_t row_position = range.right_start;
    for (size_t right_row = 0; right_row < right_rows_to_insert; ++right_row, ++row_position)
    {
        for (size_t i = 0; i < left_block.columns(); ++i)
        {
            const auto & src_column = left_block.getByPosition(i);
            auto & dst_column = left_columns[i];

            size_t row_pos = range.left_start;
            for (size_t row = 0; row < left_rows_to_insert; ++row, ++row_pos)
                dst_column->insertFrom(*src_column.column, row_pos);
        }

        for (size_t i = 0; i < right_columns_to_add.columns(); ++i)
        {
            const auto & src_column = right_block.getByName(right_columns_to_add.getByPosition(i).name);
            auto & dst_column = right_columns[i];

            for (size_t row = 0; row < left_rows_to_insert; ++row)
                dst_column->insertFrom(*src_column.column, row_position);
        }
    }
}

}
