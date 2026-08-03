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
#include <unordered_map>
#include <vector>

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

class DistinctTransform final : public ISimpleTransform
{
public:
    DistinctTransform(
        SharedHeader header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        bool is_pre_distinct_,
        size_t max_threads_,
        UInt64 two_level_threshold_,
        UInt64 two_level_threshold_bytes_,
        UInt64 parallel_build_min_rows_);

    ~DistinctTransform() override;

    String getName() const override { return "DistinctTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ColumnNumbers key_columns_pos;
    SetVariants data;
    Sizes key_sizes;
    const UInt64 limit_hint;
    const bool is_pre_distinct;
    const UInt64 two_level_threshold;
    const UInt64 two_level_threshold_bytes;
    const UInt64 parallel_build_min_rows;

    std::unique_ptr<ThreadPool> pool;

    /// Per-chunk parallel-vs-serial build decision. True when a pool exists and the chunk clears
    /// the min-rows gate. A seam: can later be driven by online per-block signals.
    bool shouldBuildParallel(size_t num_rows) const;

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
        /// row id and cached hash of its own rows (private, so no prefix-sum pass). The key is re-derived
        /// from the row at emplace time, keeping these key-type independent. Outer vectors sized once;
        /// inner arrays `clear()`-ed (capacity kept) per chunk.
        std::vector<PaddedPODArray<UInt32>> local_rows;
        std::vector<PaddedPODArray<UInt64>> local_hashes;

        /// One arena per bucket for the string-key build, so each bucket persists its keys without
        /// contending on `SetVariants::string_pool`. Lazily built in phase B; single-writer per bucket,
        /// so the arena-backed `std::string_view` keys never dangle.
        std::array<std::unique_ptr<Arena>, two_level_num_fine_buckets> bucket_arenas;
    };
    mutable TwoLevelScratch two_level_scratch;

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
