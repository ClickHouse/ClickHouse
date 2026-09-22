#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>

#include <Core/Block.h>
#include <Core/Block_fwd.h>

#include <Common/MultiVersion.h>
#include <Common/SharedMutex.h>

#include <mutex>

namespace DB
{
class TableJoin;
class HashJoin;
class PartitionedHashJoin;
class MatchedRowsStats;

/**
 * Efficient and highly parallel implementation of external memory JOIN based on an in-memory hash join
 * (`HashJoin`, or `PartitionedHashJoin` when the query runs `partitioned_hash`).
 * Supports most of the JOIN modes, except CROSS and ASOF.
 *
 * The joining algorithm consists of three stages:
 *
 * 1) During the first stage we accumulate blocks of the right table via @addBlockToJoin.
 * Each input block is split into multiple buckets based on the hash of the row join keys.
 * The first bucket is added to the in-memory join, and the remaining buckets are written to disk for further processing.
 * When the size of the in-memory join exceeds the limits, we double the number of buckets.
 * There can be multiple threads calling addBlockToJoin, just like HashJoin.
 *
 * 2) At the second stage we process left table blocks via @joinBlock.
 * Again, each input block is split into multiple buckets by hash.
 * The first bucket is joined in-memory via the in-memory join's `joinBlock`, and the remaining buckets are written to the disk.
 *
 * 3) When the last thread reading left table block finishes, the last stage begins.
 * Each @DelayedJoinedBlocksTransform calls repeatedly @getDelayedBlocks until there are no more unfinished buckets left.
 * Inside @getDelayedBlocks we select the next unprocessed bucket, load right table blocks from disk into the in-memory join,
 * And then join them with left table blocks.
 *
 * After joining the left table blocks, we can load non-joined rows from the right table for RIGHT/FULL JOINs.
 * Note that non-joined rows are processed in multiple threads, unlike MergeJoin.
 */
class GraceHashJoin final : public IJoin
{
    class FileBucket;
    class DelayedBlocks;

    /// The join of one bucket, see `partitioned_buckets`.
    using InMemoryJoinPtr = std::shared_ptr<IJoin>;

    struct GraceHashJoinStats
    {
        size_t right_rows = 0;
        size_t unique_keys = 0;
        size_t peak_in_memory_bytes = 0;
        size_t num_rehashes = 0;
        size_t num_buckets = 0;
        size_t left_spilled_compressed_bytes = 0;
        size_t right_spilled_compressed_bytes = 0;

        UInt64 left_rows_total = 0;
        MatchedRowsAccumulator matched_left;
        MatchedRowsAccumulator matched_right;

        void foldIn(UInt64 right_table_rows, UInt64 keys, size_t peak_bytes, const MatchedRowsStats * match_stats);
    };

public:
    using BucketPtr = std::shared_ptr<FileBucket>;
    using Buckets = std::vector<BucketPtr>;

    /// `external_join_threshold_` comes from `max_bytes_before_external_join` (or the ratio): we rehash the
    /// buckets once the in-memory table reaches half of it. Only legacy mode passes 0.
    GraceHashJoin(
        size_t initial_num_buckets_,
        size_t max_num_buckets_,
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader left_sample_block_, SharedHeader right_sample_block_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        bool any_take_last_row_,
        size_t external_join_threshold_,
        size_t max_threads_,
        bool partitioned_buckets_ = false);

    ~GraceHashJoin() override;

    std::string getName() const override { return "GraceHashJoin"; }
    const TableJoin & getTableJoin() const override { return *table_join; }
    bool anyTakeLastRow() const override { return any_take_last_row; }

    void initialize(const Block & sample_block) override;

    bool addBlockToJoin(const Block & block, size_t num_rows, size_t worker_id, bool check_limits) override;
    void checkTypesOfKeys(const Block & block) const override;
    JoinResultPtr joinBlock(Block block) override;

    void setTotals(const Block & block) override;
    const Block & getTotals() const override;

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    StepAnalysisReport getAnalysisReport() const override;
    bool alwaysReturnsEmptySet() const override;

    bool supportParallelJoin() const override { return true; }
    size_t getMaxBuildThreads() const override { return max_threads; }

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block_, const Block & result_sample_block_, UInt64 max_block_size) const override;

    /// Open iterator over joined blocks.
    /// Must be called after all @joinBlock calls.
    IBlocksStreamPtr getDelayedBlocks() override;
    bool hasDelayedBlocks() const override { return true; }

    void onBuildPhaseFinish() override;

    static bool isSupported(const std::shared_ptr<TableJoin> & table_join);

    bool canSpillToDisk() const override { return true; }
    size_t getSpillableBytes() const override;
    void requestSpill() override { force_spill = true; }

private:
    void initBuckets();
    /// Create empty join for in-memory processing.
    InMemoryJoinPtr makeInMemoryJoin(const String & bucket_id, size_t reserve_num = 0);

    /// The calls to the bucket's join beyond `IJoin`, each with a `HashJoin` and a `PartitionedHashJoin` arm.
    /// The bytes the overflow checks compare with the limits: what `HashJoin` holds now, or what the
    /// partitioned join predicts it will hold once it builds its table at the barrier.
    size_t inMemoryBytes(const IJoin & join) const;
    size_t inMemoryPeakBytes(const IJoin & join) const;
    BlocksList releaseInMemoryBlocks(IJoin & join) const;
    /// The partitioned join builds its table in its post-build phase, so that phase runs here for every
    /// bucket; a `HashJoin` bucket keeps its post-build optimizations for the single-bucket case.
    void finishInMemoryBuild(IJoin & join) const;
    void foldInMemoryJoin(GraceHashJoinStats & into, const IJoin & join) const;

    /// Add right table block to the @join. Calls @rehash on overflow.
    void addBlockToJoinImpl(Block block, size_t worker_id);

    /// Split the bucket held in memory in two, half of it onto disk. Caller holds `hash_join_mutex`.
    void repartitionCurrentBucket(size_t prev_keys_num, Block leftover);
    bool canForceRepartition() const;
    bool forcedSpillPending() const;

    /// Check that join satisfies limits on rows/bytes in table_join.
    bool hasMemoryOverflow(size_t total_rows, size_t total_bytes) const;
    bool hasMemoryOverflow(const InMemoryJoinPtr & hash_join_) const;

    /// Add bucket_count new buckets
    /// Throws if a bucket creation fails
    void addBuckets(size_t bucket_count);

    /// Increase number of buckets to match desired_size.
    /// Called when HashJoin in-memory table for one bucket exceeds the limits.
    ///
    /// NB: after @rehashBuckets there may be rows that are written to the buckets that they do not belong to.
    /// It is fine; these rows will be written to the corresponding buckets during the third stage.
    Buckets rehashBuckets();

    /// Perform some bookkeeping after all calls to @joinBlock.
    void startReadingDelayedBlocks();

    /// `max_rows_in_join` / `max_bytes_in_join` as a hard cap on the whole right side. False means
    /// `join_overflow_mode = 'break'`; it throws for `'throw'`.
    bool checkSizeLimits() const;

    size_t getNumBuckets() const;
    Buckets getCurrentBuckets() const;

    GraceHashJoinStats collectStats() const;

    /// Structure block to store in the HashJoin according to sample_block.
    Block prepareRightBlock(const Block & block);

    LoggerPtr log;
    std::shared_ptr<TableJoin> table_join;
    SharedHeader left_sample_block;
    SharedHeader right_sample_block;
    Block output_sample_block;
    bool any_take_last_row;
    const size_t initial_num_buckets;
    const size_t max_num_buckets;
    const size_t external_join_threshold;
    const size_t max_threads;
    const bool partitioned_buckets;

    Names left_key_names;
    Names right_key_names;

    TemporaryDataOnDiskScopePtr tmp_data;

    Buckets buckets;
    mutable SharedMutex rehash_mutex;

    FileBucket * current_bucket = nullptr;
    /// A bucket crossed the hard cap under `join_overflow_mode = 'break'`: emit it, then stop.
    /// Set from the concurrent build phase (`addBlockToJoin`) as well as from `getDelayedBlocks`.
    std::atomic<bool> stop_after_current_bucket = false;

    mutable std::mutex current_bucket_mutex;

    InMemoryJoinPtr hash_join;
    Block hash_join_sample_block;
    mutable std::mutex hash_join_mutex;
    std::atomic<bool> force_spill = false;

    /// What the buckets built and already released held, for the `max_rows_in_join` /
    /// `max_bytes_in_join` check. The bucket in memory right now is added on top, see `checkSizeLimits`.
    std::atomic<size_t> accounted_right_rows = 0;
    std::atomic<size_t> accounted_right_bytes = 0;

    /// Without it every probe thread would take `hash_join_mutex` once per block to learn the phase already ran.
    std::atomic<bool> post_build_phase_ran = false;

    GraceHashJoinStats stats;

    mutable std::mutex totals_mutex;
};

}
