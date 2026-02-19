#pragma once

#include <mutex>

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <QueryPipeline/SizeLimits.h>


namespace DB
{

class HashJoin;
class GraceHashJoin;

/// An IJoin wrapper that automatically spills to disk when memory limits are exceeded.
///
/// During the build phase (right table), blocks are buffered in memory without hashing.
/// If the buffered data exceeds `max_bytes_in_join`, a `GraceHashJoin` is created and the
/// buffered blocks are drained into it (most will be spilled to disk).
/// If all blocks fit in memory, a regular `HashJoin` is created at `onBuildPhaseFinish` time.
///
/// `hasDelayedBlocks()` always returns true so that the pipeline includes the delayed block
/// transforms needed by `GraceHashJoin`. When `HashJoin` is used, `getDelayedBlocks()` returns
/// nullptr and the delayed transforms finish instantly.
class SpillingHashJoin final : public IJoin
{
public:
    SpillingHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader left_sample_block_,
        SharedHeader right_sample_block_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t max_num_buckets_);

    ~SpillingHashJoin() override;

    std::string getName() const override { return "SpillingHashJoin"; }
    const TableJoin & getTableJoin() const override { return *table_join; }

    bool addBlockToJoin(const Block & block, bool check_limits) override;
    void checkTypesOfKeys(const Block & block) const override;
    void initialize(const Block & sample_block) override;
    JoinResultPtr joinBlock(Block block) override;

    void setTotals(const Block & block) override;
    const Block & getTotals() const override;

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;

    bool supportParallelJoin() const override { return false; }
    bool supportTotals() const override { return false; }

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    IBlocksStreamPtr getDelayedBlocks() override;
    bool hasDelayedBlocks() const override { return true; }

    void onBuildPhaseFinish() override;

private:
    enum class State
    {
        COLLECTING,
        HASH_JOIN,
        GRACE_HASH_JOIN,
    };

    void switchToGraceHashJoin();

    LoggerPtr log;
    std::shared_ptr<TableJoin> table_join;
    SharedHeader left_sample_block;
    Block right_sample_block;
    TemporaryDataOnDiskScopePtr tmp_data;
    size_t max_num_buckets;
    SizeLimits limits;

    State state = State::COLLECTING;
    BlocksList buffered_blocks;
    size_t buffered_rows = 0;
    size_t buffered_bytes = 0;

    /// Lightweight HashJoin created in the constructor for metadata operations
    /// (checkTypesOfKeys, initialize, header computation). NOT used for data storage.
    JoinPtr hash_join;

    /// The real join, created when switching out of COLLECTING state.
    JoinPtr inner_join;
};

}
