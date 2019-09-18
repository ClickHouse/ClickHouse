#include <Core/NamesAndTypes.h>
#include <Core/SortCursor.h>
#include <Columns/ColumnNullable.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/AnalyzedJoin.h>
#include <Interpreters/sortBlock.h>
#include <Interpreters/join_common.h>
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
    size_t end() const { return impl.rows; }
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
        if (lhs_pos + 1 >= impl.rows)
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
        while (sameNext(pos))
            ++pos;
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

void joinEquals(const Block & left_block, const Block & right_block, const Block & right_columns_to_add,
                MutableColumns & left_columns, MutableColumns & right_columns, const Range & range, bool is_all)
{
    size_t left_rows_to_add = range.left_length;
    size_t right_rows_to_add = is_all ? range.right_length : 1;

    size_t row_position = range.right_start;
    for (size_t right_row = 0; right_row < right_rows_to_add; ++right_row, ++row_position)
    {
        copyLeftRange(left_block, left_columns, range.left_start, left_rows_to_add);
        copyRightRange(right_block, right_columns_to_add, right_columns, row_position, left_rows_to_add);
    }
}

void appendNulls(MutableColumns & right_columns, size_t rows_to_add)
{
    for (auto & column : right_columns)
        column->insertManyDefaults(rows_to_add);
}

void joinInequalsLeft(const Block & left_block, MutableColumns & left_columns, MutableColumns & right_columns,
                      size_t start, size_t end, bool copy_left)
{
    if (end <= start)
        return;

    size_t rows_to_add = end - start;
    if (copy_left)
        copyLeftRange(left_block, left_columns, start, rows_to_add);
    appendNulls(right_columns, rows_to_add);
}

}



MergeJoin::MergeJoin(std::shared_ptr<AnalyzedJoin> table_join_, const Block & right_sample_block)
    : table_join(table_join_)
    , nullable_right_side(table_join->forceNullableRight())
    , is_all(table_join->strictness() == ASTTableJoin::Strictness::All)
    , is_inner(isInner(table_join->kind()))
    , is_left(isLeft(table_join->kind()))
{
    if (!isLeft(table_join->kind()) && !isInner(table_join->kind()))
        throw Exception("Partial merge supported for LEFT and INNER JOINs only", ErrorCodes::NOT_IMPLEMENTED);

    JoinCommon::extractKeysForJoin(table_join->keyNamesRight(), right_sample_block, right_table_keys, right_columns_to_add);

    const NameSet required_right_keys = table_join->requiredRightKeys();
    for (const auto & column : right_table_keys)
        if (required_right_keys.count(column.name))
            right_columns_to_add.insert(ColumnWithTypeAndName{nullptr, column.type, column.name});

    JoinCommon::removeLowCardinalityInplace(right_columns_to_add);
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

    if (right_blocks.empty())
        return;

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
    Block block = materializeBlock(src_block);
    JoinCommon::removeLowCardinalityInplace(block);

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
    materializeBlockInplace(block);
    JoinCommon::removeLowCardinalityInplace(block);

    sortBlock(block, left_sort_description);

    std::shared_lock lock(rwlock);

    size_t rows_to_reserve = is_left ? block.rows() : 0;
    MutableColumns left_columns = makeMutableColumns(block, (is_all ? rows_to_reserve : 0));
    MutableColumns right_columns = makeMutableColumns(right_columns_to_add, rows_to_reserve);
    MergeJoinCursor left_cursor(block, left_merge_description);
    size_t left_key_tail = 0;

    if (is_left)
    {
        for (auto it = right_blocks.begin(); it != right_blocks.end(); ++it)
        {
            if (left_cursor.atEnd())
                break;
            leftJoin(left_cursor, block, *it, left_columns, right_columns, left_key_tail);
        }

        left_cursor.nextN(left_key_tail);
        joinInequalsLeft(block, left_columns, right_columns, left_cursor.position(), left_cursor.end(), is_all);
        //left_cursor.nextN(left_cursor.end() - left_cursor.position());

        changeLeftColumns(block, std::move(left_columns));
        addRightColumns(block, std::move(right_columns));
    }
    else if (is_inner)
    {
        for (auto it = right_blocks.begin(); it != right_blocks.end(); ++it)
        {
            if (left_cursor.atEnd())
                break;
            innerJoin(left_cursor, block, *it, left_columns, right_columns, left_key_tail);
        }

        left_cursor.nextN(left_key_tail);
        changeLeftColumns(block, std::move(left_columns));
        addRightColumns(block, std::move(right_columns));
    }
}

void MergeJoin::leftJoin(MergeJoinCursor & left_cursor, const Block & left_block, const Block & right_block,
                         MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail)
{
    MergeJoinCursor right_cursor(right_block, right_merge_description);

    while (!left_cursor.atEnd() && !right_cursor.atEnd())
    {
        size_t left_position = left_cursor.position(); /// save inequal position
        Range range = left_cursor.getNextEqualRange(right_cursor);

        joinInequalsLeft(left_block, left_columns, right_columns, left_position, range.left_start, is_all);

        if (range.empty())
            break;

        if (is_all)
            joinEquals(left_block, right_block, right_columns_to_add, left_columns, right_columns, range, is_all);
        else
            joinEqualsAnyLeft(right_block, right_columns_to_add, right_columns, range);

        right_cursor.nextN(range.right_length);

        /// Do not run over last left keys for ALL JOIN (cause of possible duplicates in next right block)
        if (is_all && right_cursor.atEnd())
        {
            left_key_tail = range.left_length;
            break;
        }
        left_cursor.nextN(range.left_length);
    }
}

void MergeJoin::innerJoin(MergeJoinCursor & left_cursor, const Block & left_block, const Block & right_block,
                          MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail)
{
    MergeJoinCursor right_cursor(right_block, right_merge_description);

    while (!left_cursor.atEnd() && !right_cursor.atEnd())
    {
        Range range = left_cursor.getNextEqualRange(right_cursor);
        if (range.empty())
            break;

        joinEquals(left_block, right_block, right_columns_to_add, left_columns, right_columns, range, is_all);
        right_cursor.nextN(range.right_length);

        /// Do not run over last left keys for ALL JOIN (cause of possible duplicates in next right block)
        if (is_all && right_cursor.atEnd())
        {
            left_key_tail = range.left_length;
            break;
        }
        left_cursor.nextN(range.left_length);
    }
}

void MergeJoin::changeLeftColumns(Block & block, MutableColumns && columns)
{
    if (is_left && !is_all)
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

}
