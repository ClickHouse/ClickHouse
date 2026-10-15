#pragma once

#include <Processors/ISimpleTransform.h>
#include <Processors/SpillableAdapter.h>
#include <Processors/Transforms/DistinctSetFilter.h>
#include <Processors/Transforms/DistinctSetMemoryTracker.h>
#include <QueryPipeline/SizeLimits.h>

#include <optional>

namespace DB
{

/// Preliminary per-stream deduplication (the preliminary `DISTINCT`, see `DistinctStep`, or the
/// pre-deduplication in front of a set fill, see `CreatingSetStep`) pays off only when it removes
/// rows: the consumer deduplicates anyway, so on mostly-unique input the transform removes almost
/// nothing while its hash table duplicates the memory of the structure being filled downstream.
///
/// This controller accumulates, over all chunks seen so far, how many rows survived deduplication.
/// Once enough chunks have been observed for the rate to be meaningful, it is checked after every
/// chunk; when nearly all rows survive, the transform abandons: it drops the accumulated hash table
/// and passes the remaining chunks through untouched.
class DeduplicationAbandonController
{
public:
    /// Updates the observations and returns whether the caller should abandon deduplication.
    bool update(size_t num_rows, size_t num_unique_rows, size_t set_bytes);

private:
    /// Number of chunks to observe before the rate is checked.
    static constexpr size_t OBSERVATION_CHUNK_COUNT = 5;

    /// The observation itself retains memory: until the first check, the hash table keeps every unique
    /// key seen, which for wide keys is chunk count * block size * key size per stream. One chunk of
    /// rows already gives a meaningful rate, so once the set is this large the check starts immediately
    /// instead of waiting out the chunk window. Integer keys stay on the full window (their set is
    /// about half this size at the fifth chunk).
    static constexpr size_t MAX_OBSERVATION_SET_BYTES = 16 * 1024 * 1024;

    /// Fraction of the observed rows that survived deduplication. Above this rate the removal is too
    /// small to help the consumer, while the hash table keeps growing with the unique rows.
    static constexpr double UNIQUE_RATE_THRESHOLD = 0.9;

    size_t chunks_observed = 0;
    size_t rows_observed = 0;
    size_t unique_rows_observed = 0;
};

/// The streaming hash-based `DISTINCT`: emits the first occurrence of each key as soon as it is seen. The
/// deduplication logic itself lives in `DistinctSetFilter` (shared with `ExternalDistinctTransform`, which
/// additionally spills to disk under memory pressure).
class DistinctTransform final : public ISimpleTransform
{
public:
    /// `allow_abandoning_` permits giving up on mostly-unique input (see `DeduplicationAbandonController`):
    /// the output is then no longer fully deduplicated, so it must only be enabled when the consumer
    /// deduplicates the output anyway. `skip_null_keys_` drops rows with a `NULL` in any key column instead
    /// of emitting them (see `DistinctSetFilter`); it must only be enabled when the consumer drops them
    /// anyway. `max_bytes_before_pass_through_` is a query-memory threshold for preliminary `DISTINCT`
    /// followed by an exact deduplicating consumer. The transform frees its set when this threshold is
    /// exceeded or projected growth and filtering exceed its remaining budget. Subsequent rows pass
    /// through, giving up any remaining local limit hint. Zero disables this memory policy.
    /// `shared_set_bytes_` accounts for retained set memory across disjoint streams and attaches
    /// snapshots to output chunks for global limit accounting by `DistinctLimitsCheckingTransform`.
    /// A nonzero `coalesce_rows_` merges the output chunks until they hold this many rows, or until
    /// `COALESCE_INPUT_BLOCKS` times as many input rows were consumed since the last output. Preliminary
    /// deduplication of highly duplicated input emits a stream of tiny chunks, and each of them costs
    /// scheduling in the consumer. This is worst for a parallel final `DISTINCT`, which scatters every
    /// chunk across all partitions and merges the many resulting pieces into the query output. The bound
    /// in input rows keeps the delay finite on an unbounded input that adds no new keys, so a downstream
    /// limit can still stop the query. Chunks with a `ChunkInfo` are not coalesced, see `transform`.
    /// `allow_spilling_` permits the scheduler to request the same release for preliminary deduplication.
    DistinctTransform(
        SharedHeader header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        bool allow_abandoning_ = false,
        bool skip_null_keys_ = false,
        UInt64 max_bytes_before_pass_through_ = 0,
        DistinctSetMemoryTracker::SharedCounter shared_set_bytes_ = nullptr,
        UInt64 coalesce_rows_ = 0,
        bool allow_spilling_ = false);

    String getName() const override { return "DistinctTransform"; }

    Status prepare() override;

    std::span<ISpillable * const> getSpillables() override { return {&spillable_ptr, allow_spilling ? 1uz : 0uz}; }
    ProcessorMemoryStats getMemoryStats() const;
    size_t spill(size_t at_least_bytes);

protected:
    void transform(Chunk & input_chunk, Chunk & output_chunk) override;
    void transform(Chunk & chunk) override;
    bool needInputData() const override { return !forward_input_after_flush; }

private:
    void deduplicate(Chunk & chunk);
    Chunk flushPendingChunks();

    /// Outlives the set so its contribution is removed after the retained allocations are released.
    DistinctSetMemoryTracker set_memory;
    SpillableAdapter<DistinctTransform> spillable{*this};
    ISpillable * const spillable_ptr = &spillable;
    /// An absent filter means subsequent chunks pass through without deduplication.
    std::optional<DistinctSetFilter> distinct_set;
    const UInt64 limit_hint;

    std::optional<DeduplicationAbandonController> abandon_controller;

    const UInt64 max_bytes_before_pass_through;

    /// A parallel final `DISTINCT` splits every chunk into up to 16 partitions (see `DistinctStep`).
    static constexpr UInt64 COALESCE_INPUT_BLOCKS = 16;

    const UInt64 coalesce_rows;
    /// Output chunks held back for coalescing, their rows, and the input rows consumed since the last output.
    Chunks pending_chunks;
    UInt64 pending_rows = 0;
    UInt64 pending_input_rows = 0;
    /// The held back rows were emitted ahead of an input chunk with a `ChunkInfo`, which is processed next.
    bool forward_input_after_flush = false;
    const bool allow_spilling;
};

}
