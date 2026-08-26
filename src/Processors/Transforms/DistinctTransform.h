#pragma once

#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/DistinctSetFilter.h>
#include <QueryPipeline/SizeLimits.h>

#include <optional>

namespace DB
{

/// Preliminary per-stream deduplication (e.g. in front of a set fill, see `CreatingSetStep`) pays off
/// only when it removes rows: the consumer deduplicates anyway, so on mostly-unique input the transform
/// removes almost nothing while its hash table duplicates the memory of the structure being filled
/// downstream.
///
/// This controller accumulates, over all chunks seen so far, how many rows survived deduplication.
/// Once enough chunks have been observed for the rate to be meaningful, it is checked after every
/// chunk; when nearly all rows survive, the transform abandons: it drops the accumulated hash table
/// and passes the remaining chunks through untouched.
class DeduplicationAbandonController
{
public:
    bool isAbandoned() const { return abandoned; }

    void update(size_t num_rows, size_t num_unique_rows, size_t set_bytes);

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

    bool abandoned = false;
    size_t chunks_observed = 0;
    size_t rows_observed = 0;
    size_t unique_rows_observed = 0;
};

/// The streaming hash-based DISTINCT: emits the first occurrence of each key as soon as it is seen.
/// The deduplication logic itself lives in DistinctSetFilter (shared with ExternalDistinctTransform,
/// which additionally spills to disk under memory pressure).
class DistinctTransform final : public ISimpleTransform
{
public:
    /// `allow_abandoning_` permits giving up on mostly-unique input (see `DeduplicationAbandonController`):
    /// the output is then no longer fully deduplicated, so it must only be enabled when the consumer
    /// deduplicates the output anyway.
    /// `skip_null_keys_` drops rows with a NULL in any key column instead of emitting them, mirroring a
    /// set fill with `transform_null_in = 0`, which skips such rows; it must only be enabled when the
    /// consumer drops them anyway.
    /// `max_bytes_before_pass_through_` (0 - disabled) is only for a preliminary DISTINCT followed by an
    /// exact one: when the memory usage of the query exceeds it, the transform frees its set and lets all
    /// rows through, leaving the deduplication to the final DISTINCT (which can spill to disk, see
    /// ExternalDistinctTransform).
    DistinctTransform(
        SharedHeader header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        bool allow_abandoning_ = false,
        bool skip_null_keys_ = false,
        UInt64 max_bytes_before_pass_through_ = 0);

    String getName() const override { return "DistinctTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    /// Drops the rows that have a NULL in any key component (skip_null_keys mode).
    void skipNullKeyRows(Chunk & chunk) const;

    DistinctSetFilter distinct_set;
    const UInt64 limit_hint;

    std::optional<DeduplicationAbandonController> abandon_controller;

    const bool skip_null_keys;
    /// A constant NULL key component makes every key contain a NULL, so a consumer that skips NULL
    /// keys drops all rows; the transform then emits nothing and stops the input.
    bool const_null_key = false;

    const UInt64 max_bytes_before_pass_through;
    bool pass_through = false;
};

}
