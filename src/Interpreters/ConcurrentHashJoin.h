#pragma once

#include <memory>
#include <optional>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/HyperLogLogWithSmallSetOptimization.h>
#include <Interpreters/TableJoin.h>
#include <atomic>

namespace DB
{

/**
 * The default `HashJoin` is not thread-safe for inserting the right table's rows; thus, it is done on a single thread.
 * When the right table is large, the join process is too slow.
 *
 * `ConcurrentHashJoin` can run `addBlockToJoin()` concurrently to speed up the join process. On the test, it scales almost linearly.
 * For that, we create multiple `HashJoin` instances. In `addBlockToJoin()`, one input block is split into multiple blocks
 * corresponding to the `HashJoin` instances by hashing every row on the join keys. In particular, each `HashJoin` instance has its own hash map
 * that stores a unique set of keys. Also, `addBlockToJoin()` calls are done under mutex to guarantee
 * that every `HashJoin` instance is written only from one thread at a time.
 *
 * When matching the left table, the input blocks are also split by hash and routed to corresponding `HashJoin` instances.
 * This introduces some noticeable overhead compared to the `hash` join algorithm that doesn't have to split. Then,
 * we introduced the following optimization. On the probe stage, we want to have the same execution as for the `hash` join algorithm,
 * i.e., we want to have a single shared hash map that we will read from each thread. No splitting of blocks is required.
 * We should somehow divide this shared hash map between threads so that we can still execute the build stage concurrently.
 * The idea is to use a two-level hash map and distribute its buckets between threads. Namely, we will calculate the same hash
 * that the hash map calculates, map it to the bucket number, and then take this number modulo the number of threads. This way,
 * upon build phase completion, we will have thread #0 having a hash map with only buckets {#0, #threads_num, #threads_num*2, ...},
 * thread #1 with only buckets {#1, #threads_num+1, #threads_num*2+1, ...} and so on. To form the resulting hash map,
 * we will merge all these sub-maps in the method `onBuildPhaseFinish`. Please note that this merge could be done in constant time because,
 * for each bucket, only one `HashJoin` instance has it non-empty.
 */
class ConcurrentHashJoin : public IJoin
{

public:
    /// `external_join_threshold_` is the auto-spill memory cap supplied by `SpillingHashJoin`
    /// when this instance is wrapped. It bounds statistics-driven preallocation so the
    /// reserve cannot blow past the wrapper's spill threshold. Pass 0 for standalone use
    /// (`join_algorithm = 'parallel_hash'`); the user-visible `max_bytes_before_external_join`
    /// setting deliberately does NOT apply to standalone instances.
    /// `plan_key_ndv_` is the planner's trustworthy (`uniq`-backed) distinct-key estimate of the right
    /// join key, when available. When present (and there is no cross-run statistics size hint), the
    /// deferred build is skipped and the streaming build preallocates the maps to this distinct-key
    /// count - exactly like the warm `ht_size` path - so the buffering of the deferred build is avoided.
    explicit ConcurrentHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        size_t slots_,
        SharedHeader right_sample_block,
        const StatsCollectingParams & stats_collecting_params_,
        bool any_take_last_row_ = false,
        size_t external_join_threshold_ = 0,
        std::optional<size_t> plan_key_ndv_ = std::nullopt);

    ~ConcurrentHashJoin() override;

    std::string getName() const override { return "ConcurrentHashJoin"; }
    const TableJoin & getTableJoin() const override { return *table_join; }
    bool addBlockToJoin(const Block & right_block_, bool check_limits) override;
    void checkTypesOfKeys(const Block & block) const override;
    JoinResultPtr joinBlock(Block block) override;
    void setTotals(const Block & block) override;
    const Block & getTotals() const override;
    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;

    /// `getTotalByteCount` plus a conservative upper bound on the memory the pending deferred build
    /// will allocate once it is replayed at `onBuildPhaseFinish`: the hash-table buffers sized for the
    /// estimated distinct-key count, the arena copies of string keys, and the `RowRefList` arena nodes
    /// for duplicate rows. During a deferred build the maps are still empty, so `getTotalByteCount`
    /// alone would let the wrapping `SpillingHashJoin` pass its spill threshold and the replay would
    /// then overshoot the cap with no spill opportunity left. Equals `getTotalByteCount` when nothing
    /// is buffered (non-deferred builds). The distinct-key count it uses is the same one that sizes
    /// the reserve in `onBuildPhaseFinish`, so the spill decision matches the build.
    size_t getProjectedTotalByteCount() const;

    bool alwaysReturnsEmptySet() const override;
    bool supportParallelJoin() const override { return true; }

    /// Number of internal hash join slots.
    size_t getNumSlots() const { return slots; }

    /// Extract all stored blocks from a specific slot.
    /// The slot's HashJoin data is reset afterwards.
    BlocksList releaseSlotBlocks(size_t slot_idx);

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    bool supportParallelNonJoinedBlocksProcessing() const override;

    IBlocksStreamPtr getNonJoinedBlocks(
        const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size,
        size_t stream_idx, size_t num_streams) const override;

    bool isCloneSupported() const override
    {
        return getTotals().empty() && getTotalRowCount() == 0;
    }

    std::shared_ptr<IJoin> clone(const std::shared_ptr<TableJoin> & table_join_, SharedHeader, SharedHeader right_sample_block_) const override
    {
        return std::make_shared<ConcurrentHashJoin>(
            table_join_, slots, right_sample_block_, stats_collecting_params, any_take_last_row, external_join_threshold, plan_key_ndv);
    }

    std::shared_ptr<IJoin> cloneNoParallel(const std::shared_ptr<TableJoin> & table_join_, SharedHeader, SharedHeader right_sample_block_) const override
    {
        return std::make_shared<HashJoin>(table_join_, right_sample_block_, any_take_last_row);
    }

    void onBuildPhaseFinish() override;

    void setEnableLazyColumnsIndexing(bool value) override
    {
        std::ranges::for_each(hash_joins, [value](auto & hash_join) { hash_join->data->setEnableLazyColumnsIndexing(value); });
    }

    struct InternalHashJoin
    {
        std::mutex mutex;
        std::unique_ptr<HashJoin> data;
        bool space_was_preallocated = false;

        /// Deferred (exact-size) build: scattered right blocks routed to this slot are buffered here
        /// during the build phase instead of being inserted immediately. At `onBuildPhaseFinish` the
        /// slot's hash map is reserved to the estimated distinct-key count (see `NdvShard`) and the
        /// blocks are replayed, so the map is filled with few or no rehashes. Only used when
        /// `deferred_build` is set (no statistics hint).
        /// `buffered_rows`/`buffered_bytes` keep `getTotalRowCount`/`getTotalByteCount` accurate while
        /// the data is parked here, and `getProjectedTotalByteCount` projects the size of the maps the
        /// replay would build on top of them, so the wrapping `SpillingHashJoin` can still decide to
        /// spill. On a spill the buffered blocks are handed to `GraceHashJoin` directly (see
        /// `releaseSlotBlocks`), without ever building the in-memory map.
        VectorWithMemoryTracking<ScatteredBlock> buffered_blocks;
        size_t buffered_rows = 0;
        size_t buffered_bytes = 0;

        /// Empty the parked deferred build as one unit, once its blocks have left the buffer - either
        /// replayed into the map (`onBuildPhaseFinish`) or handed to `GraceHashJoin` (`releaseSlotBlocks`).
        void clearBuffers()
        {
            buffered_blocks.clear();
            buffered_blocks.shrink_to_fit();
            buffered_rows = 0;
            buffered_bytes = 0;
        }
    };

    friend class NotJoinedHash;

private:
    std::shared_ptr<TableJoin> table_join;
    size_t slots;
    bool any_take_last_row;
    std::unique_ptr<ThreadPool> pool;
    std::vector<std::shared_ptr<InternalHashJoin>> hash_joins;
    bool build_phase_finished = false;

    StatsCollectingParams stats_collecting_params;
    const size_t external_join_threshold;

    /// Trustworthy (`uniq`-backed) distinct-key estimate of the right join key (see the constructor);
    /// `nullopt` when unavailable. Drives streaming-path preallocation and skips the deferred build.
    const std::optional<size_t> plan_key_ndv;

    /// When true, slots buffer their right blocks during the build phase and the hash maps are filled
    /// at `onBuildPhaseFinish` after being reserved to the estimated distinct-key count (few or no
    /// rehashes during build).
    /// Enabled only when there is no statistics-driven preallocation to fall back on (see the ctor),
    /// and never under `join_overflow_mode = 'break'` (which cannot be honored after a full replay).
    bool deferred_build = false;

    /// Set when a deferred build buffered a block with `check_limits` and active size limits. The
    /// limits cannot be enforced at buffering time (the distinct-key count and `RowRefList` arena
    /// bytes are unknown until the replay), so they are re-checked against the real maps in
    /// `onBuildPhaseFinish`. Only `throw` reaches this path; `break` disables the deferral.
    std::atomic<bool> deferred_limits_check_requested = false;

    /// Deferred build with a string-key map: the replay will copy every key into the arena, so the
    /// key bytes of the buffered blocks are tracked here (accumulated once per source block, before
    /// dispatch) and included in `getProjectedTotalByteCount`. Monotone; meaningful only during the
    /// build phase, which is the only time the projection is consulted.
    bool track_buffered_key_bytes = false;
    std::atomic<size_t> buffered_key_bytes = 0;

    std::mutex totals_mutex;
    Block totals;

    /// Distinct-key estimation for the deferred build. Each `addBlockToJoin` caller feeds the scatter
    /// hashes of its whole source block into ONE shard, chosen round-robin, taking that shard's lock
    /// once per block (not per row) - this keeps the HLL updates off the per-slot buffering mutex and
    /// avoids the per-row contention/allocation of the previous per-slot design. The shards are merged
    /// (HLL union, so cross-shard duplicate keys are not double-counted) once the build finishes to get
    /// the global distinct-key estimate. The number of shards equals `slots` (>= the build concurrency,
    /// so round-robin rarely makes two concurrent callers collide). Only allocated for deferred builds.
    struct NdvShard
    {
        std::mutex mutex;
        HyperLogLogWithSmallSetOptimization<UInt64, 16, 12> hll;
    };
    std::vector<std::unique_ptr<NdvShard>> ndv_shards;
    std::atomic<size_t> ndv_shard_round_robin = 0;

    /// Feed a source block's scatter hashes into a round-robin distinct-key shard (deferred build).
    /// The raw scatter hash is reused directly: the HLL's own `intHash32` mixes it, and because the
    /// estimate is now global (shards are merged, not partitioned per slot) there is no per-slot bit
    /// correlation that would require an extra finalizer.
    void feedDistinctKeyEstimator(const std::vector<UInt64> & block_key_hashes);

    /// Merged global distinct-key estimate over all shards (raw, unbiased), plus the buffered
    /// source-row count via `total_buffered_rows`. Valid after the build phase (shards are frozen).
    size_t sumBufferedDistinctKeyEstimates(size_t & total_buffered_rows) const;

    /// Up-biased, source-row-clamped distinct-key estimate (see `ndvReserveEstimate`). Returns 0 when
    /// nothing is buffered. Used both to size the reserve (`onBuildPhaseFinish`) and as the hash-table
    /// buffer term of `getProjectedTotalByteCount`; the two MUST agree, so they derive it identically
    /// from the same frozen shards.
    size_t estimateBufferedDistinctKeys() const;

    /// `out_block_key_hashes`, when non-null, is filled with the raw scatter hashes of the source block's
    /// insertable rows (those a NULL key or the right-side ON condition would not exclude at insert time;
    /// see `selectDispatchBlock`), for the deferred build's distinct-key estimate. Not split per slot.
    /// `out_has_kept_row`, when non-null, is filled with one flag per slot: whether any row routed to that
    /// slot would be kept by the streaming build (NULL key or passing the right-side ON mask). The
    /// deferred build uses it to skip buffering a per-slot block the streaming build would pop. An empty
    /// returned vector means "every slot keeps a row" (skip nothing) - see `selectDispatchBlock`.
    ScatteredBlocks dispatchBlock(
        const Strings & key_columns_names,
        Block && from_block,
        std::vector<UInt64> * out_block_key_hashes = nullptr,
        std::vector<UInt8> * out_has_kept_row = nullptr);
};

}
