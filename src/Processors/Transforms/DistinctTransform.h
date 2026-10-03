#pragma once

#include <Interpreters/BloomFilter.h>
#include <Interpreters/SetVariants.h>
#include <Common/ThreadPool_fwd.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/DistinctSetFilter.h>
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
    /// `is_pre_distinct_` selects the preliminary (per-stream) mode: there the transform may absorb
    /// new keys into a bloom filter instead of the hash set once the set has grown past
    /// `set_limit_for_enabling_bloom_filter_` (0 disables it). In the final mode the transform may
    /// instead switch to a two-level hash set probed in parallel on `max_threads_` threads.
    DistinctTransform(
        SharedHeader header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        bool allow_abandoning_ = false,
        bool skip_null_keys_ = false,
        UInt64 max_bytes_before_pass_through_ = 0,
        bool is_pre_distinct_ = false,
        UInt64 set_limit_for_enabling_bloom_filter_ = 0,
        UInt64 bloom_filter_bytes_ = 0,
        Float64 pass_ratio_threshold_for_disabling_bloom_filter_ = 0,
        Float64 max_ratio_of_set_bits_in_bloom_filter_ = 0,
        size_t max_threads_ = 1);

    ~DistinctTransform() override;

    String getName() const override { return "DistinctTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    /// An absent filter means subsequent chunks pass through without deduplication.
    std::optional<DistinctSetFilter> distinct_set;
    const UInt64 limit_hint;

    const bool is_pre_distinct;

    /// The bloom-filter preliminary `DISTINCT` and the parallel final `DISTINCT` need direct access to
    /// the hash set, so they keep their own state instead of `distinct_set`. `own_set` is enabled when
    /// one of these optimizations may apply; it is reset (together with `bloom_filter`) when the
    /// transform switches to pass-through.
    bool use_own_set = false;
    bool own_set_released = false;
    bool own_set_limit_reached = false;
    ColumnNumbers key_columns_pos;
    /// Positions of the columns that are not constant in the header: only these are materialized.
    ColumnNumbers non_constant_columns_pos;
    std::unique_ptr<SetVariants> data;
    Sizes key_sizes;
    std::unique_ptr<DistinctLowCardinalityFilter> lc_filter;
    std::unique_ptr<BloomFilter> bloom_filter;
    std::unique_ptr<ThreadPool> pool;

    /// BloomFilter Pre DISTINCT optimization
    size_t total_passed_bf = 0;
    /// Rows forwarded by the `check_only` mode. Such rows are not recorded anywhere (neither in the
    /// hash set nor in the bloom filter), so the same key is forwarded on every occurrence and this
    /// counter is not a count of distinct keys - it must not be compared against `limit_hint`.
    size_t total_passed_check_only = 0;
    bool use_bf = false;
    bool try_init_bf = false;
    UInt64 set_limit_for_enabling_bloom_filter = 1000000;
    UInt64 bloom_filter_bytes = 0;
    Float64 pass_ratio_threshold_for_disabling_bloom_filter = 0.7;
    Float64 max_ratio_of_set_bits_in_bloom_filter = 0.7;
    UInt64 bf_worthless_last_set_bits = 0;
    UInt64 bf_worthless_total_set_bits = 0;
    UInt64 bf_worthless_last_bf_pass = 0;

    /// Restrictions on the maximum size of the output data (for the own set).
    SizeLimits set_size_limits;

    std::optional<DeduplicationAbandonController> abandon_controller;

    const UInt64 max_bytes_before_pass_through;

    template <typename Method>
    void buildSetFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumn::Filter & filter,
        size_t rows,
        SetVariants & variants,
        const IColumn::Filter * mask) const;

    template <typename Method>
    void buildCombinedFilter(
        Method & method,
        const ColumnRawPtrs & columns,
        IColumnFilter & filter,
        size_t rows,
        SetVariants & variants,
        size_t & passed_bf) const;

    template <typename Method>
    void checkSetFilter(
        Method & method,
        const ColumnRawPtrs & columns,
        IColumnFilter & filter,
        size_t rows,
        SetVariants & variants,
        size_t & passed_bf) const;

    template <typename Method>
    void buildSetParallelFilter(
        Method & method,
        const ColumnRawPtrs & columns,
        IColumnFilter & filter,
        size_t rows,
        SetVariants & variants,
        ThreadPool & thread_pool) const;

    /// Disables bloom filter if it is likely to have bad selectivity
    void checkBloomFilterWorthiness();

    size_t getOwnSetByteCount() const;

    /// Frees the own set state: the remaining chunks then pass through untouched.
    void releaseOwnSet();

    /// The deduplication through the own set (see `use_own_set`).
    void transformWithOwnSet(Chunk & chunk);
};

}
