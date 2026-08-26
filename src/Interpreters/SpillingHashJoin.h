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
class PartitionedHashJoin;

/// Disambiguates the partitioned collecting mode from the concurrent one, whose constructor takes
/// the same trailing `size_t`.
struct PartitionedCollectingTag
{
};

/// An IJoin wrapper that automatically switches to GraceHashJoin to spill to disk when memory limits are exceeded.
///
/// Operates in three modes depending on the constructor parameters:
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
/// Partitioned mode:
/// Blocks are fed into a PartitionedHashJoin from multiple threads concurrently, under the same
/// shared/exclusive lock as concurrent mode. Accumulation overflow is judged by
/// `predictedResidentBytes` (the post-build peak the rows already held are heading for), not
/// `getTotalByteCount`, because the leaf tables do not exist yet during the fill. Overflow drops
/// fill transients, then drains one stored block at a time into GraceHashJoin via
/// `tryConvertFillLanes`. After the partitioned
/// barrier a three-way gate may still switch (the resident data itself does not fit) or promote
/// the in-memory join, possibly with a grouped post-build scatter that bounds the transient
/// without disk.
///
/// hasDelayedBlocks always returns true so that the pipeline includes the delayed-block
/// transforms needed by GraceHashJoin. When HashJoin / ConcurrentHashJoin /
/// PartitionedHashJoin is used, getDelayedBlocks returns nullptr and the delayed transforms finish instantly.
/// Because hasDelayedBlocks returns true, the read-in-order-through-join optimisation
/// in optimizeReadInOrder.cpp will NOT propagate through SpillingHashJoin (same as
/// GraceHashJoin), since spilling may reorder rows.
class SpillingHashJoin final : public IJoin
{
public:
    using IJoin::addBlockToJoin;
    using IJoin::joinBlock;
    /// Single-thread mode: wraps a HashJoin.
    SpillingHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader left_sample_block_,
        SharedHeader right_sample_block_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t initial_num_buckets_,
        size_t max_num_buckets_,
        const StatsCollectingParams & stats_collecting_params_ = {},
        bool any_take_last_row_ = false);

    /// Concurrent mode: wraps a ConcurrentHashJoin.
    SpillingHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader left_sample_block_,
        SharedHeader right_sample_block_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t initial_num_buckets_,
        size_t max_num_buckets_,
        size_t concurrent_slots_,
        const StatsCollectingParams & stats_collecting_params_ = {},
        bool any_take_last_row_ = false);

    /// Partitioned mode: wraps a PartitionedHashJoin.
    SpillingHashJoin(
        PartitionedCollectingTag,
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader left_sample_block_,
        SharedHeader right_sample_block_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t initial_num_buckets_,
        size_t max_num_buckets_,
        size_t num_threads_,
        const StatsCollectingParams & stats_collecting_params_ = {},
        bool any_take_last_row_ = false);

    ~SpillingHashJoin() override;

    std::string getName() const override;
    const TableJoin & getTableJoin() const override { return *table_join; }
    bool anyTakeLastRow() const override { return any_take_last_row; }

    bool addBlockToJoin(const Block & block, bool check_limits) override;
    bool addBlockToJoin(const Block & block, size_t num_rows, bool check_limits, size_t build_lane) override;
    void checkTypesOfKeys(const Block & block) const override;
    void initialize(const Block & sample_block) override;
    JoinResultPtr joinBlock(Block block) override;
    JoinResultPtr joinBlock(Block block, size_t lane) override;

    void setTotals(const Block & block) override;
    const Block & getTotals() const override;

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;

    StepAnalysisReport getAnalysisReport() const override;

    bool supportParallelJoin() const override;
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

    /// Forwarded to the join actually chosen in `onBuildPhaseFinish`, so that an in-memory
    /// `HashJoin` still gets its post-build optimizations (right-table reranging, conversion to a
    /// fixed hash map, publishing the shared runtime filter).
    /// After a spill `chosen_join` is a `GraceHashJoin`, which does not override these methods, so
    /// forwarding keeps the spilled path exactly as it is today: `GraceHashJoin` itself runs the
    /// post-build phase only when the right table ended up in a single bucket. Multi-bucket spills
    /// skip it, because a hash table holding one bucket cannot produce a runtime filter valid for
    /// the whole right table.
    bool hasPostBuildPhase() const override;
    void runPostBuildPhase() override;

    void setEnableLazyColumnsIndexing(bool value) override;

private:
    enum class State
    {
        COLLECTING, // Right-side blocks are being collected in HashJoin / ConcurrentHashJoin / PartitionedHashJoin, no spilling yet.
        GRACE_HASH_JOIN, // Spilled to disk and switched to GraceHashJoin, but some concurrent slots may still be unconverted.
        IN_MEMORY_JOIN // All blocks fit in memory, using HashJoin / ConcurrentHashJoin / PartitionedHashJoin directly without switching.
    };

    void switchToGraceHashJoin();
    /// Shared by the fill-path switch and the post-barrier `MustSpill` arm. The latter must not call
    /// `switchToGraceHashJoin`, which drains fill lanes the barrier has already consumed.
    void createGraceJoin();
    void tryConvertSlots();
    void tryConvertFillLanes();

    /// The join that owns the data while the state is COLLECTING.
    IJoin & collectingJoin() const;

    /// Shared by the two `addBlockToJoin` overloads. `forward_lane` is true only for the partitioned
    /// collecting mode, which resolves fill lanes through lock-free slot tables.
    bool addCollectedBlock(const Block & block, bool check_limits, bool forward_lane, size_t build_lane);

    LoggerPtr log;
    std::shared_ptr<TableJoin> table_join;
    SharedHeader left_sample_block;
    Block right_sample_block;
    TemporaryDataOnDiskScopePtr tmp_data;
    size_t initial_num_buckets;
    size_t max_num_buckets;
    bool any_take_last_row;
    size_t max_bytes_before_external_join;

    SharedMutex switch_mutex;
    std::atomic<size_t> next_slot_to_convert{0};
    std::atomic<size_t> next_fill_lane_to_convert{0};
    mutable std::mutex totals_mutex;
    bool supports_parallel_non_joined_blocks_processing{false};

    std::atomic<State> state{State::COLLECTING};

    /// HashJoin that stores right-side blocks during COLLECTING phase (single-thread mode).
    std::shared_ptr<HashJoin> hash_join;

    /// ConcurrentHashJoin for multi-thread path (mutually exclusive with hash_join).
    std::shared_ptr<ConcurrentHashJoin> concurrent_join;

    /// PartitionedHashJoin for the partitioned_hash collecting path (mutually exclusive with the other two).
    std::shared_ptr<PartitionedHashJoin> partitioned_join;

    /// GraceHashJoin created during overflow. Also assigned to chosen_join.
    std::shared_ptr<GraceHashJoin> grace_join;

    /// The real join, created when switching out of COLLECTING state.
    JoinPtr chosen_join;
};

}
