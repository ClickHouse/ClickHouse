#include <Core/NamesAndTypes.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/AnalyzedJoin.h>
#include <DataStreams/materializeBlock.h>

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
        if (required_right_keys.count(column.name))
            right_columns_to_add.insert(ColumnWithTypeAndName{nullptr, column.type, column.name});

    JoinCommon::createMissedColumns(right_columns_to_add);

    if (nullable_right_side)
        JoinCommon::convertColumnsToNullable(right_columns_to_add);
}

/// TODO: sort
bool MergeJoin::addJoinedBlock(const Block & block)
{
    std::unique_lock lock(rwlock);

    right_blocks.push_back(block);
    right_blocks_row_count += block.rows();
    right_blocks_bytes += block.bytes();

    return table_join.sizeLimits().check(right_blocks_row_count, right_blocks_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void MergeJoin::joinBlock(Block & block)
{
    std::shared_lock lock(rwlock);

    JoinCommon::checkTypesOfKeys(block, table_join.keyNamesLeft(), right_table_keys, table_join.keyNamesRight());

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

void MergeJoin::mergeJoin(Block & /*block*/, const Block & /*right_block*/)
{
    /// TODO
}

}
