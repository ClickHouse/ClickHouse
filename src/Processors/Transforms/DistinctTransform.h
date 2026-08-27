#pragma once

#include <Columns/ColumnLowCardinality.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/SetVariants.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/ColumnsHashing.h>

#include <optional>
#include <unordered_map>

namespace DB
{

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

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

/// Result of `buildLowCardinalityMask`. `mask` marks rows that are the first occurrence of their
/// LC dictionary index; `new_indices_count` is the number of such rows; `processed_rows` is how
/// many rows were handled before the call returned (num_rows normally, or the stop point when the
/// call stopped early on a soft timeout).
struct LowCardinalityMaskResult
{
    IColumn::Filter mask;
    size_t new_indices_count;
    size_t processed_rows;
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
    /// deduplicates the output anyway.
    /// `skip_null_keys_` drops rows with a NULL in any key column instead of emitting them, mirroring a
    /// set fill with `transform_null_in = 0`, which skips such rows; it must only be enabled when the
    /// consumer drops them anyway.
    DistinctTransform(
        SharedHeader header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        QueryStatusPtr process_list_element_,
        bool allow_abandoning_ = false,
        bool skip_null_keys_ = false);

    String getName() const override { return "DistinctTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ColumnNumbers key_columns_pos;
    /// Reset after the controller abandons deduplication, freeing the accumulated set.
    std::optional<SetVariants> data{std::in_place};
    Sizes key_sizes;
    const UInt64 limit_hint;

    /// Restrictions on the maximum size of the output data.
    SizeLimits set_size_limits;

    /// Query status of the running query. Used to honor max_execution_time inside a single
    /// transform() call (the executor only enforces it between work() calls).
    QueryStatusPtr process_list_element;

    /// In `timeout_overflow_mode = 'break'` the soft timeout is sticky: once `checkTimeLimit`
    /// returns false it keeps returning false. Latch it so all loops stop immediately without
    /// reading the clock again, and so `transform()` can distinguish a soft timeout (preserve the
    /// already-processed prefix) from a hard cancellation (drop the chunk).
    mutable bool time_limit_exceeded = false;

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
    /// Rows in [0, processed_prefix) are already identified as candidates by a preceding
    /// `buildLowCardinalityMask` call (its soft-timeout partial result) and must be hashed even
    /// when the time limit already fired, so the processed prefix is preserved instead of dropped.
    template <typename Method>
    void buildFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumn::Filter & filter,
        size_t rows,
        SetVariants & variants,
        const IColumn::Filter * mask,
        size_t processed_prefix = 0) const;

    /// For a single LowCardinality key column, build a mask of rows that are
    /// the first occurrence of their LC dictionary index for this dictionary identity. Then, only those
    /// rows need to be checked for distinctness.
    /// Returns {mask, new_indices_count, processed_rows}, where processed_rows is the number of rows
    /// handled when the call stopped (equals num_rows on normal completion, or the stop point on a soft timeout).
    LowCardinalityMaskResult buildLowCardinalityMask(const ColumnLowCardinality & column, size_t num_rows);

    /// Returns true when the query's max_execution_time was exceeded in `timeout_overflow_mode = 'break'`
    /// (soft timeout). Hard cancellations (KILL QUERY, Ctrl+C) are surfaced through `isCancelled()`
    /// together with a non-`UNDEFINED` cancel reason and are deliberately not treated as soft timeouts,
    /// so `transform()` keeps dropping the chunk in that case.
    bool isSoftTimeout() const;

    /// Like `isSoftTimeout`, but additionally recognizes the case where the break-mode deadline was
    /// observed by the executor poll loop (`PipelineExecutor::checkTimeLimitSoft`) instead of by this
    /// transform: the executor cancels the whole pipeline with `CancelledByTimeout`, which sets
    /// `is_cancelled` on every processor without setting a user-facing cancel reason. Such a cancel
    /// must be treated as a soft timeout, so the already-committed chunk prefix is preserved instead
    /// of being cleared like a real user cancellation. Uses `checkTimeLimitSoft`, so it never throws.
    bool isCancelledBySoftTimeout() const;
};

}
