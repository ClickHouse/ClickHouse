#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include "Interpreters/TemporaryDataOnDisk.h"

namespace DB
{

class CrossJoin : public IJoin
{
public:
    CrossJoin(ContextPtr context_, std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_);
    bool addJoinedBlock(const Block & block, bool check_limits) override;
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) override;
    const TableJoin & getTableJoin() const override;
    void checkTypesOfKeys(const Block & block) const override;
    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;
    IBlocksStreamPtr getNonJoinedBlocks(
        const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;
    
private:
    void moveBlocksToDisk();

    std::shared_ptr<TableJoin> table_join;
    std::atomic<int> join_block_count_in_progress {0};
    Block right_sample_block;
    BlocksList right_blocks;
    ContextPtr context;
    std::unique_ptr<TemporaryDataOnDisk> temp_data;
    TemporaryFileStream & block_stream;
    size_t right_rows = 0;
    size_t right_bytes = 0;
    size_t right_rows_on_disk = 0;
    size_t right_bytes_on_disk = 0;
    size_t right_blocks_count = 0;
    Poco::Logger * log;
    std::mutex finish_writing;
};

}
