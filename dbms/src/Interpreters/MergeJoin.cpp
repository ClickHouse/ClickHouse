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
{
    extractKeysForJoin(table_join.keyNamesRight(), right_sample_block, sample_block_with_keys, sample_block_with_columns_to_add);
    createMissedColumns(sample_block_with_columns_to_add);
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
    addRightColumns(block);

    std::shared_lock lock(rwlock);

    for (auto it = right_blocks.begin(); it != right_blocks.end(); ++it)
        mergeJoin(block, *it);
}

void MergeJoin::addRightColumns(Block & block)
{
    size_t rows = block.rows();

    for (const auto & column : sample_block_with_columns_to_add)
        block.insert(ColumnWithTypeAndName{column.column->cloneResized(rows), column.type, column.name});
}

void MergeJoin::mergeJoin(Block & /*block*/, const Block & /*right_block*/)
{
    /// TODO
}

}
