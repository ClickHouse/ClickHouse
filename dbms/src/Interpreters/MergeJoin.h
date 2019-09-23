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
    MergeJoin(std::shared_ptr<AnalyzedJoin> table_join_, const Block & right_sample_block);

    bool addJoinedBlock(const Block & block) override;
    void joinBlock(Block &) override;
    void joinTotals(Block &) const override;
    void setTotals(const Block &) override;
    bool hasTotals() const override { return totals; }
    size_t getTotalRowCount() const override { return right_blocks_row_count; }

private:
    mutable std::shared_mutex rwlock;
    std::shared_ptr<AnalyzedJoin> table_join;
    SortDescription left_sort_description;
    SortDescription right_sort_description;
    SortDescription left_merge_description;
    SortDescription right_merge_description;
    Block right_table_keys;
    Block right_columns_to_add;
    BlocksList right_blocks;
    Block totals;
    size_t right_blocks_row_count = 0;
    size_t right_blocks_bytes = 0;
    const bool nullable_right_side;
    const bool is_all;
    const bool is_inner;
    const bool is_left;

    void changeLeftColumns(Block & block, MutableColumns && columns);
    void addRightColumns(Block & block, MutableColumns && columns);

    void mergeRightBlocks();
    void leftJoin(MergeJoinCursor & left_cursor, const Block & left_block, const Block & right_block,
                  MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail);
    void innerJoin(MergeJoinCursor & left_cursor, const Block & left_block, const Block & right_block,
                   MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail);
};

}
