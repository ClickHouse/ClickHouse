#pragma once

#include <shared_mutex>

#include <Common/LRUCache.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/SortedBlocksWriter.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

class TableJoin;
class MergeJoinCursor;
struct MergeJoinEqualRange;

class MergeJoin : public IJoin
{
public:
    MergeJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block);

    bool addJoinedBlock(const Block & block, bool check_limits) override;
    void joinBlock(Block &, ExtraBlockPtr & not_processed) override;
    void joinTotals(Block &) const override;
    void setTotals(const Block &) override;
    bool hasTotals() const override { return totals; }
    size_t getTotalRowCount() const override { return right_blocks.row_count; }
    size_t getTotalByteCount() const override { return right_blocks.bytes; }

private:
    struct NotProcessed : public ExtraBlock
    {
        size_t left_position;
        size_t right_position;
        size_t right_block;
    };

    /// There're two size limits for right-hand table: max_rows_in_join, max_bytes_in_join.
    /// max_bytes is prefered. If it isn't set we approximate it as (max_rows * bytes/row).
    struct BlockByteWeight
    {
        size_t operator()(const Block & block) const { return block.bytes(); }
    };

    using Cache = LRUCache<size_t, Block, std::hash<size_t>, BlockByteWeight>;

    mutable std::shared_mutex rwlock;
    std::shared_ptr<TableJoin> table_join;
    SizeLimits size_limits;
    SortDescription left_sort_description;
    SortDescription right_sort_description;
    SortDescription left_merge_description;
    SortDescription right_merge_description;
    Block right_sample_block;
    Block right_table_keys;
    Block right_columns_to_add;
    SortedBlocksWriter::Blocks right_blocks;
    Blocks min_max_right_blocks;
    std::unique_ptr<Cache> cached_right_blocks;
    std::vector<std::shared_ptr<Block>> loaded_right_blocks;
    std::unique_ptr<SortedBlocksWriter> disk_writer;
    SortedBlocksWriter::SortedFiles flushed_right_blocks;
    Block totals;
    std::atomic<bool> is_in_memory{true};
    const bool nullable_right_side;
    const bool is_any_join;
    const bool is_all_join;
    const bool is_semi_join;
    const bool is_inner;
    const bool is_left;
    const bool skip_not_intersected;
    const size_t max_joined_block_rows;
    const size_t max_rows_in_right_block;
    const size_t max_files_to_merge;

    void changeLeftColumns(Block & block, MutableColumns && columns) const;
    void addRightColumns(Block & block, MutableColumns && columns);

    template <bool is_all>
    ExtraBlockPtr extraBlock(Block & processed, MutableColumns && left_columns, MutableColumns && right_columns,
                             size_t left_position, size_t right_position, size_t right_block_number);

    void mergeRightBlocks();

    template <bool in_memory>
    size_t rightBlocksCount();
    template <bool in_memory, bool is_all>
    void joinSortedBlock(Block & block, ExtraBlockPtr & not_processed);
    template <bool in_memory>
    std::shared_ptr<Block> loadRightBlock(size_t pos);

    template <bool is_all> /// ALL or ANY
    bool leftJoin(MergeJoinCursor & left_cursor, const Block & left_block, const Block & right_block,
                  MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail, size_t & skip_right);
    bool semiLeftJoin(MergeJoinCursor & left_cursor, const Block & left_block, const Block & right_block,
                  MutableColumns & left_columns, MutableColumns & right_columns);
    bool allInnerJoin(MergeJoinCursor & left_cursor, const Block & left_block, const Block & right_block,
                  MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail, size_t & skip_right);

    bool saveRightBlock(Block && block);

    void mergeInMemoryRightBlocks();
    void mergeFlushedRightBlocks();
};

}
