#pragma once

#include <atomic>
#include <vector>

#include <Common/Arena.h>
#include <Common/PODArray.h>
#include <Interpreters/AdaptiveAggregation.h>
#include <Interpreters/AdaptiveAggregationStaging.h>
#include <Interpreters/Aggregator.h>

namespace DB
{

/// What draining staged chunks needs from the aggregation: state creation and
/// aggregate-function application over one contiguous slice of a chunk's records. This is the
/// drain's whole contract with the aggregator - a different consumer of staged chunks would
/// reimplement exactly this surface. Passed by reference into the drain loops; the calls cost
/// what the aggregator's own paths pay (no virtual dispatch).
class StagedSliceApplier
{
public:
    explicit StagedSliceApplier(const Aggregator & aggregator_) : aggregator(aggregator_) { }

    /// The methods are defined inline: `createStates` runs once per inserted record in the
    /// drain's hot loop and must compile exactly as the aggregator's own paths do.

    size_t aggregatesSize() const { return aggregator.params.aggregates_size; }

    /// Whether the compiled aggregation applies to a slice of this chunk: unlike the frozen
    /// consume loop, whose misses are null places the compiled row loop cannot skip, every
    /// place in a drain slice is non-null, and the staged argument columns are always dense.
    bool useCompiledFunctions([[maybe_unused]] const Aggregator::AggregateFunctionInstruction * instructions) const
    {
#if USE_EMBEDDED_COMPILER
        return aggregator.compiled_aggregate_functions_holder && !Aggregator::hasSparseArguments(instructions);
#else
        return false;
#endif
    }

    /// Allocates and initializes one row's aggregate states in `arena`.
    ALWAYS_INLINE AggregateDataPtr createStates(Arena & arena, bool use_compiled_functions) const
    {
        AggregateDataPtr place = arena.alignedAlloc(aggregator.total_size_of_aggregate_states, aggregator.align_aggregate_states);
        aggregator.createAggregateStates(place, use_compiled_functions);
        return place;
    }

    /// Applies the chunk's prepared instructions to records [row_begin, row_end): the slice is
    /// a contiguous row range of the compacted argument columns and of `places`, so the
    /// standard executor applies to it directly.
    void applyInstructions(
        Arena * arena,
        size_t row_begin,
        size_t row_end,
        const Aggregator::AggregateFunctionInstruction * instructions,
        AggregateDataPtr * places,
        bool use_compiled_functions) const
    {
        aggregator.executeAggregateInstructions(
            arena,
            row_begin,
            row_end,
            instructions,
            places,
            /*key_start=*/row_begin,
            /*has_only_one_value_since_last_reset=*/false,
            /*all_keys_are_const=*/false,
            use_compiled_functions);
    }

private:
    const Aggregator & aggregator;
};

/// The consumer side of the staging module, the counterpart of `StagedChunkBuilder`: drains
/// bucket-grouped staged chunks into two-level hash tables, emplacing every record's key with
/// its staged routing hash and applying its payload (a run-length count or the prepared
/// aggregate instructions through a `StagedSliceApplier`). The merge-time drain borrows the
/// chunks' key bytes (they are retained until the merged bucket converts and retires); a
/// pressure-time batch drain copies them into the table's arenas, because freeing the chunks
/// is its purpose.
class StagedChunkDrainer
{
public:
    explicit StagedChunkDrainer(AdaptiveAggregationSession & session_) : session(session_) { }

    /// Drains bucket b's backlog into the destination's bucket b at merge time. The caller is
    /// the merge task owning the bucket, so no locks are needed; keys point into the retained
    /// chunks' staged bytes.
    void drainBucketForMerge(
        AggregatedDataVariants & dest,
        Arena * arena,
        size_t bucket_index,
        const StagedSliceApplier & applier,
        std::atomic<bool> & is_cancelled) const;

    /// Drains a claimed batch of chunks into `table`, bucket-major: bucket b's slices from
    /// all of the batch's chunks drain consecutively, so the destination subtable and its
    /// arena stay cache-hot across the whole batch instead of being revisited once per chunk
    /// - the measured win of the pressure drains. The price is that the batch stays alive
    /// until the pass ends: the callers bound a batch at about one spill floor of records and
    /// release the chunks right after the call. The keys persist into the table's per-bucket
    /// arenas so the chunks can be freed. Returns the records drained; a cancelled drain
    /// stops between buckets and reports its actual progress.
    size_t drainBatch(
        AggregatedDataVariants & table,
        const std::vector<StagedChunkPtr> & batch,
        std::atomic<bool> & is_cancelled,
        PaddedPODArray<AggregateDataPtr> & places_scratch,
        const StagedSliceApplier & applier) const;

    /// Retires a bucket's chunk references after its merge-and-convert completed: the borrow
    /// of staged key bytes ends at conversion. A chunk frees once the last bucket holding it
    /// retires.
    void retireMergedBucket(AggregatedDataVariants & dest, size_t bucket) const;

private:
    template <AdaptiveKeyStorage key_storage, typename Method>
    size_t drainBucketBacklog(
        Method & method,
        Arena * arena,
        const std::vector<StagedChunkPtr> & backlog,
        size_t bucket_index,
        size_t total_records,
        PaddedPODArray<AggregateDataPtr> & places,
        const StagedSliceApplier & applier,
        std::atomic<bool> & is_cancelled) const;

    template <AdaptiveKeyStorage key_storage, typename Method>
    void drainBucketSlice(
        Method & method,
        Arena * bucket_arena,
        const StagedChunk & block,
        size_t slice_begin,
        size_t slice_end,
        PaddedPODArray<AggregateDataPtr> & places,
        size_t bucket_index,
        const StagedSliceApplier & applier) const;

    AdaptiveAggregationSession & session;
};

/// Tuning of the drain loops.
/// The drain reserves a bucket's table after sampling this fraction of its records.
constexpr size_t adaptive_reserve_sample_inverse = 8;
/// Headroom over the sampled insert rate when reserving.
constexpr double adaptive_reserve_headroom = 1.25;
/// Fixed lookahead of the drain's hash prefetch.
constexpr size_t adaptive_drain_prefetch_look_ahead = 16;

}
