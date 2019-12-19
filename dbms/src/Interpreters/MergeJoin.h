#pragma once

#include <memory>
#include <shared_mutex>

#include <Common/LRUCache.h>
#include <Common/filesystemHelpers.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/IJoin.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

class AnalyzedJoin;
class MergeJoinCursor;
struct MergeJoinEqualRange;

struct MiniLSM
{
    using SortedFiles = std::vector<std::unique_ptr<TemporaryFile>>;

    const String & path;
    const Block & sample_block;
    const SortDescription & sort_description;
    const size_t rows_in_block;
    const size_t max_size;
    std::vector<SortedFiles> sorted_files;

    MiniLSM(const String & path_, const Block & sample_block_, const SortDescription & description,
            size_t rows_in_block_, size_t max_size_ = 16)
        : path(path_)
        , sample_block(sample_block_)
        , sort_description(description)
        , rows_in_block(rows_in_block_)
        , max_size(max_size_)
    {}

    void insert(const BlocksList & blocks);
    void merge(std::function<void(const Block &)> callback = [](const Block &){});
};


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
    /// There're two size limits for right-hand table: max_rows_in_join, max_bytes_in_join.
    /// max_bytes is prefered. If it isn't set we aproximate it as (max_rows * bytes/row).
    struct BlockByteWeight
    {
        size_t operator()(const Block & block) const { return block.bytes(); }
    };

    using Cache = LRUCache<size_t, Block, std::hash<size_t>, BlockByteWeight>;

    mutable std::shared_mutex rwlock;
    std::shared_ptr<AnalyzedJoin> table_join;
    SizeLimits size_limits;
    SortDescription left_sort_description;
    SortDescription right_sort_description;
    SortDescription left_merge_description;
    SortDescription right_merge_description;
    Block right_sample_block;
    Block right_table_keys;
    Block right_columns_to_add;
    BlocksList right_blocks;
    Blocks min_max_right_blocks;
    std::unique_ptr<Cache> cached_right_blocks;
    std::vector<std::shared_ptr<Block>> loaded_right_blocks;
    std::unique_ptr<MiniLSM> lsm;
    MiniLSM::SortedFiles flushed_right_blocks;
    Block totals;
    size_t right_blocks_row_count = 0;
    size_t right_blocks_bytes = 0;
    bool is_in_memory = true;
    const bool nullable_right_side;
    const bool is_all;
    const bool is_inner;
    const bool is_left;
    const bool skip_not_intersected;
    const size_t max_rows_in_right_block;

    void changeLeftColumns(Block & block, MutableColumns && columns);
    void addRightColumns(Block & block, MutableColumns && columns);

    void mergeRightBlocks();

    template <bool in_memory>
    size_t rightBlocksCount();
    template <bool in_memory>
    void joinSortedBlock(Block & block);
    template <bool in_memory>
    std::shared_ptr<Block> loadRightBlock(size_t pos);

    void leftJoin(MergeJoinCursor & left_cursor, const Block & left_block, const Block & right_block,
                  MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail);
    void innerJoin(MergeJoinCursor & left_cursor, const Block & left_block, const Block & right_block,
                   MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail);

    bool saveRightBlock(Block && block);
    void flushRightBlocks();

    void mergeInMemoryRightBlocks();
    void mergeFlushedRightBlocks();

    void clearRightBlocksList()
    {
        right_blocks.clear();
        right_blocks_row_count = 0;
        right_blocks_bytes = 0;
    }

    void countBlockSize(const Block & block)
    {
        right_blocks_row_count += block.rows();
        right_blocks_bytes += block.bytes();
    }
};

}
