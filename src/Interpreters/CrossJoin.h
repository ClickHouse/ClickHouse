#pragma once

#include <list>
#include <memory>
#include <optional>

#include <Core/Block.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/RowRefs.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/logger_useful.h>

namespace DB
{

class TableJoin;

/** Implements `CROSS JOIN` and comma joins.
  * It stores right-side blocks without hash keys and emits the cartesian product for each left-side input block.
  * Right-side blocks may be compressed or spilled to a temporary block stream when join limits are exceeded.
  */
class CrossJoin final : public IJoin
{
public:
    CrossJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block_);

    std::string getName() const override { return "CrossJoin"; }
    const TableJoin & getTableJoin() const override { return *table_join; }

    bool isCloneSupported() const override
    {
        return getTotals().empty() && total_rows_to_join == 0;
    }

    std::shared_ptr<IJoin> clone(
        const std::shared_ptr<TableJoin> & table_join_,
        SharedHeader,
        SharedHeader right_sample_block_) const override
    {
        return std::make_shared<CrossJoin>(table_join_, right_sample_block_);
    }

    bool addBlockToJoin(const Block & source_block, bool check_limits) override;
    bool addBlockToJoin(const Block & source_block, size_t num_rows, bool check_limits) override;

    void checkTypesOfKeys(const Block &) const override {}

    JoinResultPtr joinBlock(Block block) override;

    size_t getTotalRowCount() const override { return in_memory_rows; }
    size_t getTotalByteCount() const override;

    bool alwaysReturnsEmptySet() const override { return total_rows_to_join == 0; }

    IBlocksStreamPtr getNonJoinedBlocks(const Block &, const Block &, UInt64) const override { return {}; }

private:
    friend class CrossJoinResult;

    struct StoredBlock
    {
        ColumnsInfo columns_info;
        size_t rows = 0;

        StoredBlock(ColumnsInfo columns_info_, size_t rows_)
            : columns_info(std::move(columns_info_))
            , rows(rows_)
        {
        }

        size_t allocatedBytes() const;
    };

    using StoredBlocks = std::list<StoredBlock>;

    std::shared_ptr<TableJoin> table_join;

    Block right_sample_block;
    Block sample_block_with_columns_to_add;
    Block saved_block_sample;

    StoredBlocks right_blocks;

    TemporaryDataOnDiskScopePtr tmp_data;
    std::optional<TemporaryBlockStreamHolder> tmp_stream;

    size_t total_rows_to_join = 0;
    size_t in_memory_rows = 0;
    size_t allocated_size = 0;
    bool have_compressed = false;

    size_t max_joined_block_rows = 0;
    size_t max_joined_block_bytes = 0;

    bool shrink_blocks = false;
    Int64 memory_usage_before_adding_blocks = 0;

    LoggerPtr log;

    Block materializeColumnsFromRightBlock(Block block) const;
    void shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize = false);
    void doDebugAsserts() const;
};

}
