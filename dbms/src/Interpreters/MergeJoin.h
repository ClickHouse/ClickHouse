#pragma once

#include <memory>
#include <shared_mutex>

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/IJoin.h>


namespace DB
{

class AnalyzedJoin;
class MergeJoinCursor;
struct MergeJoinEqualRange;

class MergeJoin : public IJoin
{
public:
    MergeJoin(const AnalyzedJoin & table_join_, const Block & right_sample_block);

    bool addJoinedBlock(const Block & block) override;
    void joinBlock(Block &) override;
    void joinTotals(Block &) const override {}
    void setTotals(const Block &) override;
    size_t getTotalRowCount() const override { return right_blocks_row_count; }

private:
    mutable std::shared_mutex rwlock;
    const AnalyzedJoin & table_join;
    SortDescription left_sort_description;
    SortDescription right_sort_description;
    SortDescription left_merge_description;
    SortDescription right_merge_description;
    Block right_table_keys;
    Block right_columns_to_add;
    BlocksList right_blocks;
    Block totals;
    bool nullable_right_side;
    size_t right_blocks_row_count = 0;
    size_t right_blocks_bytes = 0;

    MutableColumns makeRightColumns(size_t rows);
    void appendRightColumns(Block & block, MutableColumns && right_columns);

    void mergeRightBlocks();
    void leftJoin(MergeJoinCursor & left_cursor, const Block & right_block, MutableColumns & right_columns);

    void appendRightNulls(MutableColumns & right_columns, size_t rows_to_add);
    void anyLeftJoinEquals(const Block & right_block, MutableColumns & right_columns, const MergeJoinEqualRange & range);
};

}
