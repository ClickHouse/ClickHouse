#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>
#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

class Aggregator;
struct AggregatedDataVariants;
class ReadRangesRegistry;
using ReadRangesRegistryPtr = std::shared_ptr<ReadRangesRegistry>;

struct RuntimeDataflowStatistics
{
    /// The read parallel replicas would coordinate: they split it, so the cost model divides it by the
    /// number of replicas.
    size_t input_bytes = 0;
    /// Every other read of the same subtree. Parallel replicas do not split these - each replica runs the
    /// whole subtree, so each reads all of them. They cost the same wall-clock time either way, which is
    /// why they are kept apart from `input_bytes` rather than added to it, but they cost the cluster
    /// `num_replicas` times as much work, which is what the amplification gate weighs.
    size_t duplicated_bytes = 0;
    size_t output_bytes = 0;
    size_t total_rows_to_read = 0;
    /// Counting the compressed blocks read took more ranges than `automatic_parallel_replicas_max_counted_ranges`,
    /// so the bytes are not known. Remembered, so that the query does not pay for counting them again.
    bool input_not_measured = false;
};

inline RuntimeDataflowStatistics operator+(const RuntimeDataflowStatistics & lhs, const RuntimeDataflowStatistics & rhs)
{
    return RuntimeDataflowStatistics{
        .input_bytes = lhs.input_bytes + rhs.input_bytes,
        .duplicated_bytes = lhs.duplicated_bytes + rhs.duplicated_bytes,
        .output_bytes = lhs.output_bytes + rhs.output_bytes,
        .total_rows_to_read = lhs.total_rows_to_read + rhs.total_rows_to_read,
        .input_not_measured = lhs.input_not_measured || rhs.input_not_measured,
    };
}

class RuntimeDataflowStatisticsCache
{
public:
    using Entry = RuntimeDataflowStatistics;
    using Cache = DB::CacheBase<UInt64, Entry>;
    using CachePtr = std::shared_ptr<Cache>;

    RuntimeDataflowStatisticsCache()
        : stats_cache(std::make_shared<Cache>(CurrentMetrics::end(), CurrentMetrics::end(), 100_MiB, 1'000'000))
        , boundary_cache(std::make_shared<BoundaryCache>(CurrentMetrics::end(), CurrentMetrics::end(), 100_MiB, 1'000'000))
        , undecided_query_cache(std::make_shared<UndecidedQueryCache>(CurrentMetrics::end(), CurrentMetrics::end(), 64_MiB, 100'000))
    {
    }

    [[nodiscard]] std::optional<Entry> getStats(size_t key) const;

    void update(size_t key, RuntimeDataflowStatistics stats);

    /// The keys of the single-replica plan's boundary - the node matched to the top of the replicas plan - and of
    /// the read below it that parallel replicas coordinate, both found by an earlier execution, so that a later one
    /// can find its statistics without building a replicas plan to match against.
    struct Boundary
    {
        UInt64 boundary_hash = 0;
        UInt64 coordinated_read_hash = 0;
    };

    /// `root_key` combines the single-replica plan's root hash with the settings that can move the boundary
    /// without changing that plan.
    [[nodiscard]] std::optional<Boundary> getBoundary(UInt64 root_key) const;

    void setBoundary(UInt64 root_key, Boundary boundary);

    /// The last build of the plan with parallel replicas for a plan ended without a decision, so that the next
    /// executions can skip building it. It is not a verdict: nothing proves the outcome permanent, so a number of
    /// executions skip, and the one after them builds again.
    struct UndecidedQuery
    {
        /// The event the outcome was counted in, which a skipped execution counts again: the plans could not be
        /// compared, the planner used no parallel replicas in the plan built with them, or a step could not collect
        /// statistics (`RuntimeDataflowStatisticsCacheUpdater::markUnsupportedCase`).
        ProfileEvents::Event outcome_event{};
        /// The executions since, counted on the shared entry so that concurrent ones take turns: every one in
        /// `automatic_parallel_replicas_undecided_retry_after_executions + 1` builds the plan, the others skip.
        mutable std::atomic<UInt64> executions{0};
        /// No earlier entry was replaced: this is the first time building the plan for this key ended without a
        /// decision. Then the turn of the first execution after it comes at once. The first execution of a join is
        /// what measures its hash table, and join reordering takes row counts from that, so the build after it often
        /// orders both plans alike.
        bool first = false;
    };

    /// Under the same key as the boundary.
    [[nodiscard]] std::shared_ptr<const UndecidedQuery> getUndecided(UInt64 root_key) const;

    void setUndecided(UInt64 root_key, ProfileEvents::Event outcome_event);

    void forgetUndecided(UInt64 root_key);

private:
    using BoundaryCache = DB::CacheBase<UInt64, Boundary>;

    using UndecidedQueryCache = DB::CacheBase<UInt64, UndecidedQuery>;

    CachePtr stats_cache;
    std::shared_ptr<BoundaryCache> boundary_cache;
    std::shared_ptr<UndecidedQueryCache> undecided_query_cache;
};

RuntimeDataflowStatisticsCache & getRuntimeDataflowStatisticsCache();

/// One execution's dataflow statistics, the single cache entry its updaters fill. Every updater of the
/// execution shares it, and it writes the entry when the last of them is gone.
struct RuntimeDataflowStatisticsBlock
{
    struct Statistics
    {
        std::atomic_size_t counter{0};

        std::mutex mutex;
        size_t bytes TSA_GUARDED_BY(mutex) = 0;
        size_t sample_bytes TSA_GUARDED_BY(mutex) = 0;
        size_t compressed_bytes TSA_GUARDED_BY(mutex) = 0;
        size_t elapsed_microseconds TSA_GUARDED_BY(mutex) = 0;
    };

    enum OutputStatisticsType
    {
        AggregationState = 0,
        AggregationKeys = 1,
        OutputChunk = 2,
        MaxOutputType = 3,
    };

    /// `undecided_query_key`, when not zero, is the key `RuntimeDataflowStatisticsCache::setUndecided` records under when a
    /// step cannot collect, or when the collection measures nothing.
    RuntimeDataflowStatisticsBlock(
        size_t cache_key_, size_t total_rows_to_read_, size_t max_counted_ranges_, UInt64 undecided_query_key_ = 0)
        : cache_key(cache_key_)
        , total_rows_to_read(total_rows_to_read_)
        , max_counted_ranges(max_counted_ranges_)
        , undecided_query_key(undecided_query_key_)
    {
        if (cache_key == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cache key for RuntimeDataflowStatisticsBlock cannot be zero");

        if (total_rows_to_read == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Total rows from storage cannot be zero");
    }

    ~RuntimeDataflowStatisticsBlock();

    const size_t cache_key = 0;
    const size_t total_rows_to_read = 0;
    /// See `ReadRangesRegistry`. 0 is no limit.
    const size_t max_counted_ranges = 0;
    const UInt64 undecided_query_key = 0;

    std::atomic_bool unsupported_case{false};

    std::atomic_size_t read_ranges{0};
    std::atomic_bool counted_ranges_limit_exceeded{false};

    /// The compressed bytes the readers loaded, see `ReadRangesRegistry`.
    std::atomic_size_t input_bytes_read{0};
    std::atomic_size_t duplicated_bytes_read{0};
    std::array<Statistics, MaxOutputType> output_bytes_statistics;
};

class RuntimeDataflowStatisticsCacheUpdater
{
    using Statistics = RuntimeDataflowStatisticsBlock::Statistics;
    using OutputStatisticsType = RuntimeDataflowStatisticsBlock::OutputStatisticsType;

public:
    /// `duplicated` is set on the updater given to the reads parallel replicas would not split: each replica
    /// performs them in full, so their bytes go to `duplicated_bytes` rather than to `input_bytes`.
    explicit RuntimeDataflowStatisticsCacheUpdater(std::shared_ptr<RuntimeDataflowStatisticsBlock> block_, bool duplicated_ = false);

    /// A permutation putting the sample into the order the replicas send it in. Evaluated only for a
    /// block that is actually sampled, since building it costs a sort of the block's key columns.
    using KeyOrderProvider = std::function<IColumn::Permutation()>;

    /// Sort the sample by the group by keys before pricing it. Call this when the replicas' partial
    /// aggregation uses memory-bound merging, and so sorts its output that way before sending it, while
    /// the plan the sample is taken from does not. Left in hash table order such a sample compresses
    /// several times worse than what it is meant to price.
    void setReplicasSendOutputInKeyOrder() { replicas_send_output_in_key_order = true; }

    void recordOutputChunk(const Chunk & chunk, const Block & header);

    void recordAggregationStateSizes(AggregatedDataVariants & variant, ssize_t bucket);

    void recordAggregationKeySizes(const Chunk & chunk, const ColumnNumbers & keys_positions, const DataTypes & key_types);

    /// For a conversion that materialized only some of the groups (the bucket Top-K, the HAVING pre-filter):
    /// `full_key_bytes` is the byte size all keys would occupy materialized, measured on the
    /// hash table, and the chunk provides the compression-ratio sample only. The statistics
    /// must describe the untruncated output because they price the parallel-replicas plan,
    /// whose partial aggregation materializes every group.
    /// `untruncated_sample_columns`, when the conversion kept one, is a bounded copy of the untruncated
    /// keys and is sampled in place of the chunk: the chunk holds the kept groups only, whose keys
    /// compress differently, and holds no row at all when every group was rejected - and without a
    /// compression ratio the accumulated byte count is dropped rather than estimated.
    void recordAggregationKeySizes(
        const Chunk & chunk,
        const ColumnNumbers & keys_positions,
        const DataTypes & key_types,
        size_t full_key_bytes,
        const Columns & untruncated_sample_columns);

    /// Estimates compressed size of aggregate state columns in the output chunk.
    /// Mirrors the logic of Aggregator::estimateSizeOfCompressedState but works on ColumnAggregateFunction columns
    /// rather than a hash table. Used by in-order aggregation where states are already materialized into columns (single-stream case).
    void recordAggregationStateColumnSizes(const Chunk & chunk, const ColumnNumbers & keys_positions, const Block & header);

    /// See `IMergeTreeReader::setReadRangesRegistry`.
    [[nodiscard]] const ReadRangesRegistryPtr & getReadRangesRegistry() const { return read_ranges_registry; }
    /// For the files of the read that every replica would read in full, whatever share of the read it takes: their bytes
    /// go to `duplicated_bytes`. The same as `getReadRangesRegistry` for a duplicated read.
    [[nodiscard]] const ReadRangesRegistryPtr & getDuplicatedReadRangesRegistry() const { return duplicated_read_ranges_registry; }

    /// Ignored for duplicated reads: they only refine the duplicated-read gate, so a read that cannot be
    /// measured there must not drop the statistics that decide whether parallel replicas are considered at all.
    void markUnsupportedCase()
    {
        if (!duplicated)
            block->unsupported_case.store(true, std::memory_order_relaxed);
    }

private:
    static bool shouldSampleBlock(Statistics & statistics, size_t block_rows);

    /// `full_bytes` overrides the byte count taken from the columns, for callers whose columns
    /// are only a sample of the dataflow being accounted.
    static void
    recordColumns(
        Statistics & statistics,
        size_t num_rows,
        const ColumnsWithTypeAndName & cols,
        std::optional<size_t> full_bytes = {},
        const KeyOrderProvider & key_order = {});

    [[nodiscard]] KeyOrderProvider keyOrderProviderFor(
        const Columns & columns, const ColumnNumbers & keys_positions, const DataTypes & key_types) const;

    bool replicas_send_output_in_key_order = false;

    const std::shared_ptr<RuntimeDataflowStatisticsBlock> block;
    const bool duplicated;
    const ReadRangesRegistryPtr read_ranges_registry;
    const ReadRangesRegistryPtr duplicated_read_ranges_registry;
};

using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

class RuntimeDataflowStatisticsCollector : public ISimpleTransform
{
public:
    RuntimeDataflowStatisticsCollector(SharedHeader header_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    String getName() const override { return "RuntimeDataflowStatisticsCollector"; }

protected:
    void transform(Chunk & chunk) override;

private:
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
};
}
