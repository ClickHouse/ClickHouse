#pragma once

#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerUtils.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/IProcessor.h>

#include <atomic>
#include <memory>


namespace DB
{

class IDataType;

/// Computes a single keyless `uniqExact` (and so `count(DISTINCT x)`) without merging per-thread hash sets.
///
/// The keys are buffered in a grid of buffers (lists of blocks of keys). A row of the grid belongs to one stream of the
/// pipeline (the blocks of one output of the previous step, e.g. of one reading thread), and a column to one bucket
/// (all the keys whose hash starts with the same bits). With `S` streams and `P` buckets:
///
///                      bucket 0         bucket 1        ...   bucket P-1
///     stream 0     [ buffer 0,0   ] [ buffer 0,1   ]  ...  [ buffer 0,P-1   ]  <- written by the route transform of stream 0
///     stream 1     [ buffer 1,0   ] [ buffer 1,1   ]  ...  [ buffer 1,P-1   ]  <- written by the route transform of stream 1
///     ...
///     stream S-1   [ buffer S-1,0 ] [ buffer S-1,1 ]  ...  [ buffer S-1,P-1 ]  <- written by the route transform of stream S-1
///                         ^
///                         read by the build thread of bucket 0, into one hash set
///
/// Per-stream state is named `local_*` (`local_set`, `local_buffers`), per-bucket state built once for the pipeline
/// `bucket_*` (`bucket_set`).
///
/// 1. Routing fills the rows. The route transform of stream `s` reads the blocks of the stream, hashes each key, and
///    appends it to buffer `(s, bucket of the hash)`. A stream writes only its own row, so the streams need no locks.
/// 2. Building reads the columns. Once every stream is finished, each build thread takes a bucket `b` that nobody
///    has taken yet, inserts the buffers `(0, b)`, `(1, b)`, ..., `(S-1, b)` into one hash set, and adds its size to its
///    count. The buckets are disjoint, so the result is the sum of the counts.
///
/// A bucket holds `1/P` of the distinct keys, so its set stays in the CPU caches while it is built. The per-thread
/// sets of the ordinary aggregation outgrow the caches, and are then merged into one big set on top of that.
///
/// The pipeline is:
///     `RadixUniqExactRouteTransform` x S   (one per stream, fills its row of the grid)
///     -> `RadixUniqExactBarrierTransform`  (finishes its outputs only once all the routing is done)
///     -> `RadixUniqExactBuildTransform` x min(threads, P)  (takes buckets one at a time, builds their sets)
///     -> `RadixUniqExactSumTransform`      (sums the counts into the single result row, records the statistics).
///
/// During the routing, a stream does not append every key to its buffers right away. Each stream also has a hash set of
/// its own (`local_set`), and it is in one of three modes that decide where a key goes:
/// - `BoundedSet`: the key is inserted into `local_set`. Every stream starts here, so low-cardinality inputs never pay
///   for the routing. Once the set outgrows `MAX_LOCAL_SET_BYTES`, its keys are moved into the buffers and the stream
///   switches to `Routing`.
/// - `Routing`: the key is appended to the buffer of its bucket, unless it equals the previous key. When the stream has
///   more buffered keys, over all its buffers, than its budget allows, each of its buffers is deduplicated against
///   itself. The buckets are disjoint, so this leaves exactly the distinct keys of the stream, and the budget grows to
///   `DEDUPLICATION_BUDGET_FACTOR` times that: the buffered keys are bounded by the distinct keys of the stream, not by
///   its rows. If a deduplication finds most keys routed since the previous one repeated, and the distinct keys fit into
///   `MAX_UNBOUNDED_SET_BYTES`, the buffered keys are moved back into `local_set` and the stream switches to
///   `UnboundedSet`.
/// - `UnboundedSet`: the key is inserted into `local_set`, which now grows without a limit.
/// When a stream finishes, the keys of `local_set` are moved into its buffers, so the build reads only the grid.
///
/// After a query, `RadixUniqExactEntry` keeps in the statistics of hash tables: the result, the medians over the streams
/// of the number of keys and of distinct keys of a stream, and whether most streams ended in a set. The distinct keys of
/// a stream that ended in `Routing` are estimated: its buffers hold repeated keys, so a sample of buffers is deduplicated and
/// its count is scaled to all of them. When the same query runs again with as many streams:
/// - The number of buckets and the sizes of the bucket sets follow from the result.
/// - A stream starts in `UnboundedSet`, allocated for its distinct keys, if most streams of the previous run ended in a
///   set or if the set fits into `MAX_UNBOUNDED_SET_BYTES`, and in `Routing` otherwise. Only the majority is kept,
///   because which stream reads which rows depends on the source.
/// - A stream in `Routing` is not deduplicated if its keys are at most twice its budget, because a deduplication late in
///   the stream costs a pass over its keys that the build repeats anyway. For example, with 700000 distinct keys the
///   budget is 5.6 million keys: a stream of 6 million keys is not deduplicated, and one of 20 million is first
///   deduplicated at 5.6 million.
///
/// On `hits` with 16 streams, the later runs of `count(DISTINCT WatchID)` are 1.6x faster than without the statistics,
/// `URLHash` 1.4x, and `EventTime` 1.1x.
class IRadixUniqExactState
{
public:
    virtual ~IRadixUniqExactState() = default;

    /// Routes the rows of `column` (and skips those for which `null_map` is set, if any) for the stream `stream`.
    /// Called only by the route transform of the stream.
    virtual void route(size_t stream, const IColumn & column, const UInt8 * null_map) = 0;
    /// Called once per stream after its last chunk, by the route transform of the stream.
    virtual void finishStream(size_t stream) = 0;
    /// Builds the set of the bucket `bucket` and returns the number of distinct keys in it. Called after all the
    /// streams are finished, concurrently for different buckets.
    virtual size_t buildBucket(size_t bucket) = 0;
    /// Called once with the result, after all the buckets are built.
    virtual void recordStatistics(size_t num_distinct) = 0;

    virtual size_t numBuckets() const = 0;

    /// Total number of rows that were passed to `route`, including NULLs.
    size_t numRows() const { return rows.load(std::memory_order_relaxed); }

    /// Buckets are claimed by the build transforms one at a time.
    size_t claimBucket() { return next_bucket.fetch_add(1, std::memory_order_relaxed); }

    /// The memory of the buffers and sets across all threads is accounted to a tracker of the state, as `Aggregator`
    /// does, so that `EXPLAIN ANALYZE` reports it. The transforms switch to it while they work (see `useOwnTracker`).
    MemoryTracker * getMemoryTracker() const { return memory_tracker.get(); }
    bool useOwnTracker() const;
    std::optional<UInt64> getPeakMemoryUsage() const;

protected:
    std::atomic<size_t> rows{0};

private:
    std::atomic<size_t> next_bucket{0};
    std::unique_ptr<MemoryTracker> memory_tracker = tryCreateMemoryTrackerUnderCurrentQuery();
};

using RadixUniqExactStatePtr = std::shared_ptr<IRadixUniqExactState>;

/// Whether the argument of `uniqExact` has a type the radix partitioned `uniqExact` can compute. It may be `Nullable`,
/// NULLs are skipped as `uniqExact` does.
bool isRadixUniqExactSupported(const IDataType & argument_type);

/// The argument type must be supported (see `isRadixUniqExactSupported`).
RadixUniqExactStatePtr createRadixUniqExactState(
    const IDataType & argument_type, size_t num_streams, const StatsCollectingParams & stats_collecting_params);


class RadixUniqExactRouteTransform final : public IProcessor
{
public:
    RadixUniqExactRouteTransform(SharedHeader input_header, SharedHeader output_header, RadixUniqExactStatePtr state_, size_t stream_, size_t key_position_);

    String getName() const override { return "RadixUniqExactRouteTransform"; }
    Status prepare() override;
    void work() override;

    const IRadixUniqExactState & getState() const { return *state; }

    void setRowsBeforeAggregationCounter(RowsBeforeStepCounterPtr counter) override { rows_before_aggregation.swap(counter); }

private:
    RadixUniqExactStatePtr state;
    const size_t stream;
    const size_t key_position;
    RowsBeforeStepCounterPtr rows_before_aggregation;
    Chunk current_chunk;
    bool has_chunk = false;
    bool stream_finished = false;
};

class RadixUniqExactBarrierTransform final : public IProcessor
{
public:
    RadixUniqExactBarrierTransform(SharedHeader header, size_t num_inputs, size_t num_outputs);

    String getName() const override { return "RadixUniqExactBarrierTransform"; }
    Status prepare() override;
};

class RadixUniqExactBuildTransform final : public IProcessor
{
public:
    RadixUniqExactBuildTransform(SharedHeader header, RadixUniqExactStatePtr state_);

    String getName() const override { return "RadixUniqExactBuildTransform"; }
    Status prepare() override;
    void work() override;

private:
    RadixUniqExactStatePtr state;
    Chunk result;
    bool built = false;
};

class RadixUniqExactSumTransform final : public IAccumulatingTransform
{
public:
    RadixUniqExactSumTransform(SharedHeader header, RadixUniqExactStatePtr state_, bool empty_result_for_empty_set_);

    String getName() const override { return "RadixUniqExactSumTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    RadixUniqExactStatePtr state;
    const bool empty_result_for_empty_set;
    UInt64 total = 0;
    bool generated = false;
};

}
