#pragma once

#include <Columns/ColumnLowCardinality.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/SetVariants.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/Arena.h>
#include <Common/ColumnsHashing.h>
#include <Common/ThreadPool_fwd.h>

#include <array>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

class PhasedWorkers;


/// The LowCardinality optimization in DistinctTransform tracks seen dictionary
/// indices in a bitmap and skips hash table insertions for rows whose index was
/// already seen. This helps when many rows share few dictionary entries, but
/// becomes pure overhead when most rows carry a new index (e.g. after a
/// preliminary in-order DISTINCT that already removed duplicates).
///
/// This controller observes the first few chunks and measures how many
/// rows reference a dictionary index that was not seen in earlier rows.
/// If nearly all rows do, the bitmap is not filtering anything useful
/// and we disable the optimization.
class LCOptimizationController
{
public:
    bool isEnabled() const { return state != State::Disabled; }

    void update(size_t num_rows, size_t new_indices_in_chunk);

private:
    enum class State : uint8_t
    {
        Observing,
        Enabled,
        Disabled
    };

    /// Number of chunks to observe before deciding.
    static constexpr size_t OBSERVATION_CHUNK_COUNT = 5;

    /// Fraction of rows whose LC dictionary index was seen for the first time.
    /// When this rate is this high, the mask filters almost nothing and its
    /// bookkeeping cost (dictionary hashing, seen-index bitmap, per-row branch)
    /// is not justified.
    static constexpr double NEW_INDEX_RATE_THRESHOLD = 0.95;

    State state = State::Observing;
    size_t chunks_observed = 0;
    size_t rows_observed = 0;
    size_t new_indices_observed = 0;
};

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

class DistinctTransform final : public ISimpleTransform
{
public:
    /// `allow_abandoning_` permits giving up on mostly-unique input (see `DeduplicationAbandonController`):
    /// the output is then no longer fully deduplicated, so it must only be enabled when the consumer
    /// deduplicates the output anyway. It also marks a preliminary (reduction) deduplication: the
    /// two-level parallel build is skipped for it, because that only pays off for the single-stream
    /// final deduplication (a preliminary one runs per input stream, so a pool per stream would just
    /// oversubscribe the CPU). Callers of a preliminary deduplication also pass `max_threads_ = 1`.
    /// `skip_null_keys_` drops rows with a NULL in any key column instead of emitting them, mirroring a
    /// set fill with `transform_null_in = 0`, which skips such rows; it must only be enabled when the
    /// consumer drops them anyway.
    DistinctTransform(
        SharedHeader header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        size_t max_threads_,
        UInt64 two_level_threshold_,
        UInt64 two_level_threshold_bytes_,
        UInt64 parallel_build_min_rows_,
        bool allow_abandoning_ = false,
        bool skip_null_keys_ = false);

    ~DistinctTransform() override;

    String getName() const override { return "DistinctTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ColumnNumbers key_columns_pos;
    /// Reset after the controller abandons deduplication, freeing the accumulated set.
    std::optional<SetVariants> data{std::in_place};
    Sizes key_sizes;
    const UInt64 limit_hint;
    const UInt64 two_level_threshold;
    const UInt64 two_level_threshold_bytes;
    const UInt64 parallel_build_min_rows;

    std::unique_ptr<ThreadPool> pool;

    /// Per-chunk parallel-vs-serial build decision. True when a pool exists and the chunk is large
    /// enough to keep at least two workers busy (see `twoLevelWorkerCount`). A seam: can later be
    /// driven by online per-block signals.
    bool shouldBuildParallel(size_t num_rows) const;

    /// Number of workers the two-level parallel build would use for `num_rows`, or 0 without a pool.
    /// Shared by `shouldBuildParallel` (which rejects a single-worker chunk in favor of the cheaper
    /// serial path) and `buildTwoLevelParallelFilter` (which sizes its scratch to it), so the gate
    /// and the build never disagree on the worker count.
    size_t twoLevelWorkerCount(size_t num_rows) const;

    /// Total bytes held by the deduplication set. Includes the per-bucket string arenas of the
    /// two-level parallel build, which live outside `SetVariants::string_pool` and so are invisible to
    /// `SetVariants::getTotalByteCount`; without them a two-level string `DISTINCT` would undercount
    /// and could slip past `max_bytes_in_distinct`.
    size_t totalSetByteCount() const;

    /// Restrictions on the maximum size of the output data.
    SizeLimits set_size_limits;

    using HashedTwoLevelMethod = SetMethodHashedTwoLevel<TwoLevelHashSet<UInt128, UInt128TrivialHash>>;
    static constexpr size_t two_level_num_fine_buckets = HashedTwoLevelMethod::Data::NUM_BUCKETS;

    /// Scratch for buildTwoLevelParallelFilter, hoisted out of the per-chunk path to avoid allocator
    /// churn: buffers are resized (not reallocated) and reused. Called only from `transform`, one chunk
    /// at a time, so never accessed concurrently.
    struct TwoLevelScratch
    {
        /// Phase-A partition buffers, indexed `[worker * NUM_BUCKETS + bucket]`: each worker stores the
        /// row id and cached hash of its own rows (private, so no prefix-sum pass). Outer vectors sized
        /// once; inner arrays `clear()`-ed (capacity kept) per chunk.
        std::vector<PaddedPODArray<UInt32>> local_rows;
        std::vector<PaddedPODArray<UInt64>> local_hashes;

        /// Phase-A key bytes, `sizeof(KeyType)` per row, for the trivially-copyable (non-string) key
        /// families. Phase B reads them back instead of calling `getKeyHolder` again. This matters for
        /// the `hashed` carrier, whose key is `hash128` over every key column: without the cache that
        /// wide hash would run twice per row (once to bucket, once to emplace). Stored type-erased so
        /// the same buffers serve every key type across chunks; read back with `memcpy` (the byte offset
        /// is not aligned for `UInt128`/`UInt256`). Left empty for the string families.
        std::vector<PaddedPODArray<char>> local_keys;

        /// One arena per bucket for the string-key build, so each bucket persists its keys without
        /// contending on `SetVariants::string_pool`. Lazily built in phase B; single-writer per bucket,
        /// so the arena-backed `std::string_view` keys never dangle.
        std::array<std::unique_ptr<Arena>, two_level_num_fine_buckets> bucket_arenas;
    };
    mutable TwoLevelScratch two_level_scratch;

    /// Started on the first parallel build and reused for every later chunk. Declared after `pool`, so
    /// that it is destroyed - which stops and joins the workers - before the pool they run on.
    mutable std::unique_ptr<PhasedWorkers> two_level_workers;

    /// The worker set, creating it on first use. `num_workers` of it are active per chunk.
    PhasedWorkers & twoLevelWorkers(ThreadPool & thread_pool) const;

    using LCDictionaryKey = ColumnsHashing::LowCardinalityDictionaryCache::DictionaryKey;
    using LCDictionaryKeyHash = ColumnsHashing::LowCardinalityDictionaryCache::DictionaryKeyHash;

    struct LCDictState
    {
        /// seen_indices[idx] == 1 means dictionary index `idx` has been seen
        /// at least once for this dictionary identity.
        PaddedPODArray<UInt8> seen_indices;

        /// Number of dictionary indices we have seen at least once. When this
        /// reaches the dictionary size, any future row for the parent chunk cannot
        /// introduce a new distinct value.
        UInt64 seen_count = 0;
    };

    /// Per-dictionary state which may cover multiple IColumns.
    std::unordered_map<LCDictionaryKey, LCDictState, LCDictionaryKeyHash> lc_dict_states;

    LCOptimizationController lc_optimization_controller;

    std::optional<DeduplicationAbandonController> abandon_controller;

    bool skip_null_keys = false;

    /// A constant NULL key component makes every key contain a NULL, so a consumer that skips NULL
    /// keys drops all rows; the transform then emits nothing and stops the input.
    bool const_null_key = false;

    /// mask[i] == 0 -> row i is known duplicate (by LC index) and is never inserted.
    template <typename Method>
    void buildFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumn::Filter & filter,
        size_t rows,
        SetVariants & variants,
        const IColumn::Filter * mask) const;

    template <typename Method>
    void buildTwoLevelParallelFilter(
        Method & method,
        const ColumnRawPtrs & columns,
        IColumnFilter & filter,
        size_t rows,
        ThreadPool & thread_pool) const;

    /// For a single LowCardinality key column, build a mask of rows that are
    /// the first occurrence of their LC dictionary index for this dictionary identity. Then, only those
    /// rows need to be checked for distinctness.
    /// Returns {mask, new_indices_count}.
    std::pair<IColumn::Filter, size_t> buildLowCardinalityMask(const ColumnLowCardinality & column, size_t num_rows);
};

}
