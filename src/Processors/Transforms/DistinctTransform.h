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

    /// For a single LowCardinality key column, build a mask of rows that are
    /// the first occurrence of their LC dictionary index for this dictionary identity. Then, only those
    /// rows need to be checked for distinctness.
    /// Returns {mask, new_indices_count}.
    std::pair<IColumn::Filter, size_t> buildLowCardinalityMask(const ColumnLowCardinality & column, size_t num_rows);

    /// Feed the observed chunk to the abandon controller and, once it abandons, free the accumulated
    /// state: the remaining chunks then pass through untouched. A no-op when abandoning is not allowed.
    void maybeAbandonDeduplication(size_t num_rows, size_t num_unique_rows);
};

}
