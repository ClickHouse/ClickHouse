#pragma once

#include <atomic>
#include <mutex>

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/SharedMutex.h>


namespace DB
{

class HashJoin;
class GraceHashJoin;
class ConcurrentHashJoin;

/// An IJoin wrapper that automatically switches to GraceHashJoin to spill to disk when memory limits are exceeded.
///
/// Operates in two modes depending on the constructor parameters:
///
/// Single-thread mode:
/// Blocks are fed directly into a HashJoin instance during the build phase.
/// If the data exceeds max_bytes_before_external_join, the blocks are extracted via releaseJoinedBlocks and drained into a new
/// GraceHashJoin.
/// If all blocks fit in memory, the HashJoin is promoted to chosen_join with zero rework.
///
/// Concurrent mode:
/// Blocks are fed into a ConcurrentHashJoin from multiple threads concurrently.
/// A SharedMutex protects the COLLECTING -> GRACE_HASH_JOIN transition: addBlockToJoin takes a shared lock, while
/// switchToGraceHashJoin takes an exclusive lock.
/// If the data exceeds max_bytes_before_external_join, a GraceHashJoin is created and ConcurrentHashJoin slots are converted via
/// addBlockToJoin calls possibly from multiple threads.
/// If all blocks fit in memory, the ConcurrentHashJoin is promoted to chosen_join with zero rework.
///
/// hasDelayedBlocks always returns true so that the pipeline includes the delayed-block
/// transforms needed by GraceHashJoin. When HashJoin / ConcurrentHashJoin is used,
/// getDelayedBlocks returns nullptr and the delayed transforms finish instantly.
/// Because hasDelayedBlocks returns true, the read-in-order-through-join optimisation
/// in optimizeReadInOrder.cpp will NOT propagate through SpillingHashJoin (same as
/// GraceHashJoin), since spilling may reorder rows.
class SpillingHashJoin final : public IJoin
{
public:
    /// Single-thread mode: wraps a HashJoin.
    SpillingHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader left_sample_block_,
        SharedHeader right_sample_block_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t initial_num_buckets_,
        size_t max_num_buckets_);

    /// Concurrent mode: wraps a ConcurrentHashJoin.
    SpillingHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader left_sample_block_,
        SharedHeader right_sample_block_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t initial_num_buckets_,
        size_t max_num_buckets_,
        size_t concurrent_slots_,
        const StatsCollectingParams & stats_collecting_params_);

    ~SpillingHashJoin() override;

    std::string getName() const override;
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

    bool supportParallelJoin() const override { return concurrent_join != nullptr; }
    bool supportParallelNonJoinedBlocksProcessing() const override;
    bool isParallelNonJoinedProcessingEnabled() const override;

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    IBlocksStreamPtr getNonJoinedBlocks(
        const Block & left_sample_block,
        const Block & result_sample_block,
        UInt64 max_block_size,
        size_t stream_idx,
        size_t num_streams) const override;

    IBlocksStreamPtr getDelayedBlocks() override;
    bool hasDelayedBlocks() const override { return true; }

    void onBuildPhaseFinish() override;

private:
    enum class State
    {
        COLLECTING, // Right-side blocks are being collected in HashJoin / ConcurrentHashJoin, no spilling yet.
        GRACE_HASH_JOIN, // Spilled to disk and switched to GraceHashJoin, but some concurrent slots may still be unconverted.
        IN_MEMORY_JOIN // All blocks fit in memory, using HashJoin / ConcurrentHashJoin directly without switching.
    };

    void switchToGraceHashJoin();
    void tryConvertSlots();

    LoggerPtr log;
    std::shared_ptr<TableJoin> table_join;
    SharedHeader left_sample_block;
    Block right_sample_block;
    TemporaryDataOnDiskScopePtr tmp_data;
    size_t initial_num_buckets;
    size_t max_num_buckets;
    size_t max_bytes_before_external_join;

    SharedMutex switch_mutex;
    std::atomic<size_t> next_slot_to_convert{0};
    mutable std::mutex totals_mutex;
    bool supports_parallel_non_joined_blocks_processing{false};

    std::atomic<State> state{State::COLLECTING};

    /// HashJoin that stores right-side blocks during COLLECTING phase (single-thread mode).
    std::shared_ptr<HashJoin> hash_join;

    /// ConcurrentHashJoin for multi-thread path (mutually exclusive with hash_join).
    std::shared_ptr<ConcurrentHashJoin> concurrent_join;

    /// GraceHashJoin created during overflow. Also assigned to chosen_join.
    std::shared_ptr<GraceHashJoin> grace_join;

    /// The real join, created when switching out of COLLECTING state.
    JoinPtr chosen_join;
};

}
