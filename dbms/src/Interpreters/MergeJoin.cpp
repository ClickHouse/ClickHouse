#include <Core/NamesAndTypes.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/AnalyzedJoin.h>
#include <DataStreams/materializeBlock.h>

namespace DB
{

MergeJoin::MergeJoin(const AnalyzedJoin & table_join_, const Block & right_sample_block)
    : table_join(table_join_)
    , sample_block_with_columns_to_add(materializeBlock(right_sample_block))
{
    for (auto & column : table_join.columnsAddedByJoin())
        sample_block_with_columns_to_add.getByName(column.name);
}

void MergeJoin::joinBlocks(const Block & src_block, Block & dst_block, size_t & src_row)
{
    for (auto it = right_blocks.begin(); it != right_blocks.end();)
    {
        join(src_block, *it, dst_block, src_row);
        if (src_row == src_block.rows())
            return;

        it = right_blocks.erase(it);
    }
}

void MergeJoin::join(const Block & left_block, const Block & /*right_block*/, Block & dst_block, size_t & src_row)
{
    for (auto & column : left_block)
        dst_block.insert(column);

    for (const auto & column : sample_block_with_columns_to_add)
        dst_block.insert(ColumnWithTypeAndName{column.column->cloneResized(src_row), column.type, column.name});

    src_row = left_block.rows();
}

}
