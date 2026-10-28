#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/QueryPlan/Profiling/Metrics/StepAnalyzeInfo.h>

#include <Core/Block.h>
#include <Core/Block_fwd.h>

#include <Common/MultiVersion.h>
#include <Common/SharedMutex.h>

#include <mutex>

namespace DB
{
class TableJoin;
class HashJoin;

/**
 * External-memory `JOIN` based on `HashJoin`, with parallel build and probe streams.
 * Supports most `JOIN` modes, except `CROSS` and `ASOF`.
 *
 * The joining algorithm consists of three stages:
 *
 * 1) During the first stage we accumulate blocks of the right table via `addBlockToJoin`.
 * Each input block is split into multiple buckets based on the hash of the row join keys.
 * The first bucket is added to the in-memory `HashJoin`, and the remaining buckets are written to disk.
 * A bucket that reaches the spill threshold splits locally using its next hash bit.
 * The in-memory bucket is rebuilt immediately. A spilled bucket retains its old files and routes new rows to its children.
 * A spilled bucket has no hash table yet, so its size is estimated from its logical bytes, scaled by the ratio of
 * hash-table memory to logical bytes measured on the in-memory bucket.
 * There can be multiple threads calling `addBlockToJoin`, just like `ConcurrentHashJoin`.
 *
 * 2) At the second stage we process left table blocks via `joinBlock`.
 * Again, each input block is split into multiple buckets by hash.
 * The first bucket is joined in-memory via `HashJoin::joinBlock`, and the remaining buckets are written to disk.
 *
 * 3) When the last thread reading left table block finishes, the last stage begins.
 * `DelayedJoinedBlocksTransform` calls `getDelayedBlocks` until there are no unfinished buckets left.
 * Historical files are redistributed through the partition tree as they are read.
 * Right-side files inherited from ancestors are read before newer files, then the leaf is joined with its left-side blocks.
 *
 * After joining the left table blocks, we emit unmatched right-side rows for `RIGHT` and `FULL JOIN`.
 */
class GraceHashJoin final : public IJoin
{
    class FileBucket;
    class DelayedBlocks;
    class BucketRouting;

    using InMemoryJoinPtr = std::shared_ptr<HashJoin>;

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

        void foldIn(const HashJoin & in_memory_join);
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
        size_t external_join_threshold_);

    ~GraceHashJoin() override;

    std::string getName() const override { return "GraceHashJoin"; }

    std::string getAlgorithm() const override { return toString(JoinAlgorithm::GRACE_HASH); }
    const TableJoin & getTableJoin() const override { return *table_join; }
    bool anyTakeLastRow() const override { return any_take_last_row; }

    void initialize(const Block & sample_block) override;

    bool addBlockToJoin(const Block & block, size_t num_rows, JoinBuildContext context) override;
    void checkTypesOfKeys(const Block & block) const override;
    JoinResultPtr joinBlock(Block block) override;

    void setTotals(const Block & block) override;
    const Block & getTotals() const override;

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    StepAnalysisReport getAnalysisReport() const override;
    bool alwaysReturnsEmptySet() const override;

    bool supportParallelJoin() const override { return true; }

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
    void requestSpill(JoinBuildContext /*context*/) override { force_spill = true; }

private:
    void initBuckets();
    /// Create empty join for in-memory processing.
    InMemoryJoinPtr makeInMemoryJoin(const String & bucket_id, size_t reserve_num = 0);

    /// Add a right-side block, splitting the active bucket on overflow.
    void addBlockToJoinImpl(Block block, JoinBuildContext context);

    /// Split the bucket held in memory in two, half of it onto disk. Caller holds `hash_join_mutex`.
    void repartitionCurrentBucket(size_t prev_keys_num, Block leftover, JoinBuildContext context);
    bool canForceRepartition() const;
    bool forcedSpillPending() const;

    /// Check that join satisfies limits on rows/bytes in table_join.
    bool hasMemoryOverflow(size_t total_rows, size_t total_bytes) const;

    /// Add bucket_count new buckets
    /// Throws if a bucket creation fails
    void addBuckets(size_t bucket_count);

    /// Caller holds `rehash_mutex` exclusively. Each split adds one leaf.
    BucketPtr splitBucket(size_t bucket_index, bool spilled);
    bool canSplitBucket(size_t bucket_index) const;
    /// After the build (`build_finished`), the halves of each split are planned again until every leaf fits.
    void splitSpilledBuckets(std::vector<size_t> candidates, bool build_finished);
    /// `rows` and `logical_bytes` (`Block::bytes`) are the planned size of a leaf, including its inherited estimate.
    /// The bytes are compared as the estimated hash-table size.
    bool hasSpilledBucketOverflow(size_t rows, size_t logical_bytes) const;
    /// Estimated memory of a hash table built from `logical_bytes`, see `build_amplification`.
    size_t estimateBuildBytes(size_t logical_bytes) const;
    /// Recalibrate `build_amplification` from the in-memory join before it splits. Caller holds `hash_join_mutex`.
    void updateBuildAmplification(size_t build_bytes);

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

    Names left_key_names;
    Names right_key_names;

    TemporaryDataOnDiskScopePtr tmp_data;

    Buckets buckets;
    std::shared_ptr<const BucketRouting> bucket_routing;
    mutable SharedMutex rehash_mutex;
    /// During delayed loading, redistributed right rows precede a child's own input.
    bool reading_delayed_buckets = false;

    FileBucket * current_bucket = nullptr;
    /// A bucket crossed the hard cap under `join_overflow_mode = 'break'`: emit it, then stop.
    /// Set from the concurrent build phase (`addBlockToJoin`) as well as from `getDelayedBlocks`.
    std::atomic<bool> stop_after_current_bucket = false;

    mutable std::mutex current_bucket_mutex;

    InMemoryJoinPtr hash_join;
    Block hash_join_sample_block;
    /// `HashJoin::getTotalByteCount` of an empty unreserved join, measured once in the constructor.
    size_t empty_build_bytes = 0;
    mutable std::mutex hash_join_mutex;
    std::atomic<bool> force_spill = false;

    /// `Block::bytes` of the right-side blocks inserted into `hash_join`. Guarded by `hash_join_mutex`.
    size_t resident_logical_bytes = 0;
    /// Hash-table memory above `empty_build_bytes` per logical byte, measured on the in-memory bucket when it splits
    /// and used to estimate the tables of spilled buckets. 1 until a large enough table splits. Written under
    /// `hash_join_mutex`, read without it.
    std::atomic<double> build_amplification = 1.0;

    /// What the buckets built and already released held, for the `max_rows_in_join` /
    /// `max_bytes_in_join` check. The bucket in memory right now is added on top, see `checkSizeLimits`.
    std::atomic<size_t> accounted_right_rows = 0;
    std::atomic<size_t> accounted_right_bytes = 0;

    GraceHashJoinStats stats;

    mutable std::mutex totals_mutex;
};

}
