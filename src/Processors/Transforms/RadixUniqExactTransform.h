#pragma once

#include <Processors/IAccumulatingTransform.h>
#include <Processors/IProcessor.h>

#include <atomic>
#include <memory>


namespace DB
{

class IDataType;

/// Computes a single keyless `uniqExact` (and so `count(DISTINCT x)`) without merging per-thread hash sets.
///
/// Every reading stream routes its keys by the top bits of their hash into one of `P` partitions, and once all the
/// streams are finished, one hash set per partition is built (in parallel across partitions) and only its size is
/// kept. The partitions are disjoint, so the result is the sum of their sizes. Unlike the per-thread sets of the
/// ordinary aggregation, which spill out of the CPU caches and are then merged into one big set, each partition set
/// is small enough to stay cache-resident while it is built.
///
/// A stream starts by inserting its keys into a small local set, so low-cardinality inputs never pay for the routing.
/// It switches to routing once its local set outgrows the CPU cache. While routing, it skips consecutive duplicates,
/// and it deduplicates its buffered keys when they accumulate, so they are bounded by the number of distinct keys of
/// the stream, not by the number of rows. If most of the buffered keys turn out to be repeated, the stream goes back to
/// inserting its keys into a local set (now without a size limit), as the ordinary aggregation does.
///
/// The pipeline is:
///     `RadixUniqExactRouteTransform` (per stream)
///     -> `RadixUniqExactBarrierTransform` (finishes its outputs once all streams are routed)
///     -> `RadixUniqExactBuildTransform` (per build thread, claims partitions and counts their distinct keys)
///     -> `RadixUniqExactSumTransform` (sums the counts into the single result row).
class IRadixUniqExactState
{
public:
    virtual ~IRadixUniqExactState() = default;

    /// Routes the rows of `column` (and skips those for which `null_map` is set, if any) for the stream `stream`.
    virtual void route(size_t stream, const IColumn & column, const UInt8 * null_map) = 0;
    /// Called once per stream after its last chunk.
    virtual void finishStream(size_t stream) = 0;
    /// Builds the set of the partition `partition` and returns the number of distinct keys in it.
    virtual size_t buildPartition(size_t partition) = 0;

    virtual size_t numPartitions() const = 0;

    /// Total number of rows that were passed to `route`, including NULLs.
    size_t numRows() const { return rows.load(std::memory_order_relaxed); }

    /// Partitions are claimed by the build transforms one at a time.
    size_t claimPartition() { return next_partition.fetch_add(1, std::memory_order_relaxed); }

protected:
    std::atomic<size_t> rows{0};

private:
    std::atomic<size_t> next_partition{0};
};

using RadixUniqExactStatePtr = std::shared_ptr<IRadixUniqExactState>;

/// Returns nullptr if the argument type is not supported.
/// `argument_type` may be `Nullable`, NULLs are skipped as `uniqExact` does.
RadixUniqExactStatePtr createRadixUniqExactState(const IDataType & argument_type, size_t num_streams);


class RadixUniqExactRouteTransform final : public IProcessor
{
public:
    RadixUniqExactRouteTransform(SharedHeader input_header, SharedHeader output_header, RadixUniqExactStatePtr state_, size_t stream_, size_t key_position_);

    String getName() const override { return "RadixUniqExactRouteTransform"; }
    Status prepare() override;
    void work() override;

private:
    RadixUniqExactStatePtr state;
    const size_t stream;
    const size_t key_position;
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
