#include <Core/NamesAndTypes.h>
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
}


MergeJoin::MergeJoin(const AnalyzedJoin & table_join_, const Block & right_sample_block)
    : table_join(table_join_)
    , nullable_right_side(table_join_.forceNullabelRight())
{
    JoinCommon::extractKeysForJoin(table_join.keyNamesRight(), right_sample_block, right_table_keys, right_columns_to_add);

    const NameSet required_right_keys = table_join.requiredRightKeys();
    for (const auto & column : right_table_keys)
    {
        if (required_right_keys.count(column.name))
            right_columns_to_add.insert(ColumnWithTypeAndName{nullptr, column.type, column.name});

        right_sort_description.emplace_back(SortColumnDescription(column.name, 1, 1));
    }

    JoinCommon::createMissedColumns(right_columns_to_add);

    if (nullable_right_side)
        JoinCommon::convertColumnsToNullable(right_columns_to_add);

    NameSet unique_left_keys;
    for (auto & key_name : table_join.keyNamesLeft())
    {
        if (!unique_left_keys.count(key_name))
        {
            unique_left_keys.insert(key_name);
            left_sort_description.emplace_back(SortColumnDescription(key_name, 1, 1));
        }
    }
}

void MergeJoin::setTotals(const Block & totals_block)
{
    totals = totals_block;
    mergeRightBlocks();
}

bool MergeJoin::addJoinedBlock(const Block & src_block)
{
    Block block = src_block;
    sortBlock(block, right_sort_description);

    std::unique_lock lock(rwlock);

    right_blocks.push_back(block);
    right_blocks_row_count += block.rows();
    right_blocks_bytes += block.bytes();

    return table_join.sizeLimits().check(right_blocks_row_count, right_blocks_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void MergeJoin::joinBlock(Block & block)
{
    JoinCommon::checkTypesOfKeys(block, table_join.keyNamesLeft(), right_table_keys, table_join.keyNamesRight());
    sortBlock(block, left_sort_description);

    std::shared_lock lock(rwlock);

    addRightColumns(block);

    for (auto it = right_blocks.begin(); it != right_blocks.end(); ++it)
        mergeJoin(block, *it);
}

void MergeJoin::addRightColumns(Block & block)
{
    size_t rows = block.rows();
    for (const auto & column : right_columns_to_add)
        block.insert(ColumnWithTypeAndName{column.column->cloneResized(rows), column.type, column.name});
}

void MergeJoin::mergeRightBlocks()
{
    const size_t max_merged_block_size = 128 * 1024 * 1024;

    Blocks unsorted_blocks;
    unsorted_blocks.reserve(right_blocks.size());
    for (const auto & block : right_blocks)
        unsorted_blocks.push_back(block);

    /// FIXME: there should be no splitted keys by blocks
    MergeSortingBlocksBlockInputStream stream(unsorted_blocks, right_sort_description, max_merged_block_size);

    right_blocks.clear();
    while (Block block = stream.read())
        right_blocks.push_back(block);
}

void MergeJoin::mergeJoin(Block & /*block*/, const Block & /*right_block*/)
{
    /// TODO
}

}
