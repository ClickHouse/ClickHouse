#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Core/Range.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Set.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <base/types.h>
#include <boost/noncopyable.hpp>

namespace DB
{

class RuntimeFilter;
using UniqueRuntimeFilterPtr = std::unique_ptr<RuntimeFilter>;
using SharedRuntimeFilterPtr = std::shared_ptr<RuntimeFilter>;
using RuntimeFilterConstPtr = std::shared_ptr<const RuntimeFilter>;

struct RuntimeFilterStats
{
    std::atomic<Int64> rows_checked = 0;
    std::atomic<Int64> rows_passed = 0;
    std::atomic<Int64> rows_skipped = 0;
    std::atomic<Int64> blocks_processed = 0;
    std::atomic<Int64> blocks_skipped = 0;
};

struct RuntimeFilterConfig
{
    const Float64 pass_ratio_threshold_for_disabling = 0.7;
    const UInt64 blocks_to_skip_before_reenabling = 30;
};

namespace detail
{

struct RuntimeFilterBuildState
{
    explicit RuntimeFilterBuildState(size_t filters_to_merge_, bool inserts_are_finished_ = false)
        : filters_to_merge(filters_to_merge_)
        , inserts_are_finished(inserts_are_finished_)
    {
    }

    void assertCanInsert() const;
    void assertCanFind() const;
    void assertCanMerge() const;
    bool hasPendingMerges() const { return filters_to_merge != 0; }
    bool isFinished() const { return inserts_are_finished; }
    void finishMerge();
    void finishInserts() { inserts_are_finished = true; }

private:
    size_t filters_to_merge = 0;
    bool inserts_are_finished = false;
};

/// Thread-safe, nonnegative row budget used to throttle runtime-filter evaluation.
class RuntimeFilterSkipBudget
{
public:
    /// Add `rows * multiplier` to the budget, saturating at the maximum representable value.
    void replenish(UInt64 rows, UInt64 multiplier);

    /// Consume `rows` and return whether enough budget remains to skip the current block.
    bool consume(size_t rows);

private:
    std::atomic<Int64> rows_to_skip = 0;
};
}

class RuntimeFilterEvaluationState
{
public:
    explicit RuntimeFilterEvaluationState(RuntimeFilterConfig config_);

    void updateStats(UInt64 rows_checked, UInt64 rows_passed) const;
    const RuntimeFilterStats & getStats() const { return stats; }
    const RuntimeFilterConfig & getConfig() const { return config; }
    void markKeySetDropped() { key_set_dropped = true; }

    /// Checks if a block should bypass the filter because its key set was dropped or evaluation is temporarily throttled.
    bool shouldSkip(size_t next_block_rows) const;

private:
    void recordSkippedBlock(size_t rows_skipped) const;

    const RuntimeFilterConfig config;

    mutable RuntimeFilterStats stats;

    mutable detail::RuntimeFilterSkipBudget skip_budget;
    std::atomic<bool> key_set_dropped = false;
};

template <bool negate>
class ExactSetRuntimeFilter
{
public:
    static constexpr bool is_prebuilt = false;

    ExactSetRuntimeFilter(const DataTypePtr & filter_column_target_type_, UInt64 bytes_limit_, UInt64 exact_values_limit_);

    UInt64 getBytesLimit() const noexcept { return bytes_limit; }

    bool isFull() const noexcept { return is_full; }

    void insert(ColumnPtr values);
    void finishInsert();
    void finishInsert(RuntimeFilterEvaluationState & evaluation_state);
    /// `rows_passed` is set only when the probe can count the passed rows for free while producing
    /// the mask; otherwise it is left untouched and the caller derives the count from the mask.
    ColumnPtr find(const ColumnWithTypeAndName & values, std::optional<size_t> & rows_passed) const;
    ColumnPtr getValuesColumn() const;
    void mergeFrom(const ExactSetRuntimeFilter & source);
    void releaseExactValues();
    ColumnPtr getRecordedKeyValues() const;
    DataTypePtr getTargetType() const { return filter_column_target_type; }

private:
    struct Empty
    {
        ColumnPtr column;
    };

    struct Single
    {
        ColumnPtr column;
    };

    struct Many
    {
        SetPtr exact_values;
    };

    using LookupState = std::variant<Empty, Single, Many>;

    Set & getExactValues();
    const Set & getExactValues() const;

    const DataTypePtr filter_column_target_type;
    const bool argument_can_have_nulls;
    const UInt64 bytes_limit;
    const UInt64 exact_values_limit;

    LookupState lookup_state;

    bool is_full = false;
    bool is_finished = false;
};

extern template class ExactSetRuntimeFilter<false>;
extern template class ExactSetRuntimeFilter<true>;

/// Bloom-backed runtime filter for approximate set membership checks.
class ApproximateSetRuntimeFilter
{
public:
    static constexpr bool is_prebuilt = false;

    static bool isDataTypeSupported(const DataTypePtr & data_type);

    ApproximateSetRuntimeFilter(UInt64 bytes_limit_, UInt64 bloom_filter_hash_functions_);

    void insert(ColumnPtr values);
    /// Sets `rows_passed` to the number of rows that passed the filter: the bloom probe counts the
    /// matches while filling the mask, so the caller must not rescan the mask to collect stats.
    ColumnPtr find(const ColumnWithTypeAndName & values, std::optional<size_t> & rows_passed) const;
    void mergeFrom(const ApproximateSetRuntimeFilter & source);
    bool isWorthUsing(Float64 max_ratio_of_set_bits_in_bloom_filter) const;

private:
    void insertIntoBloomFilter(const ColumnPtr & values);

    BloomFilter bloom_filter;
};

/// Starts with an exact set and switches to an approximate set once the exact set becomes too large.
class AdaptiveSetRuntimeFilter
{
public:
    static constexpr bool is_prebuilt = false;

    static bool isDataTypeSupported(const DataTypePtr & data_type);

    AdaptiveSetRuntimeFilter(
        const DataTypePtr & filter_column_target_type_,
        UInt64 bytes_limit_,
        UInt64 exact_values_limit_,
        UInt64 bloom_filter_hash_functions_,
        Float64 max_ratio_of_set_bits_in_bloom_filter_,
        std::optional<UInt64> distinct_keys_hint_,
        bool distinct_keys_hint_matches_filter_key_);

    void insert(ColumnPtr values);
    void finishInsert(RuntimeFilterEvaluationState & evaluation_state);
    /// Forwards `rows_passed` to the underlying exact/approximate filter (see their docs).
    ColumnPtr find(const ColumnWithTypeAndName & values, std::optional<size_t> & rows_passed) const;
    void mergeFrom(const AdaptiveSetRuntimeFilter & source);
    ColumnPtr getRecordedKeyValues() const;
    DataTypePtr getTargetType() const { return filter_column_target_type; }

private:
    using ExactFilter = ExactSetRuntimeFilter<false>;
    struct KeySetDropped
    {
    };
    using Filter = std::variant<ExactFilter, ApproximateSetRuntimeFilter, KeySetDropped>;

    void insert(ColumnPtr values, Filter & filter);
    void dropKeySet(Filter & filter);
    ApproximateSetRuntimeFilter * switchToApproximateFilter(Filter & filter);

    /// Disables approximate filter if it is likely to have bad selectivity.
    void checkApproximateFilterWorthiness(
        RuntimeFilterEvaluationState & evaluation_state, const ApproximateSetRuntimeFilter & approximate_filter) const;

    const DataTypePtr filter_column_target_type;
    const UInt64 bloom_filter_hash_functions;
    const Float64 max_ratio_of_set_bits_in_bloom_filter = 0.7;
    /// Measured distinct build-side keys from prior statistics, used to choose the bloom filter size.
    const std::optional<UInt64> distinct_keys_hint;
    /// Whether the hint counts distinct values of this complete filter key, rather than only providing an upper bound.
    const bool distinct_keys_hint_matches_filter_key;

    Filter filter;
};

/// Runtime filter that delegates probe to a function captured at publication time.
/// Used to share an already-built data structure (e.g. `HashJoin`'s `FixedHashMap`)
/// as a runtime filter without copying the data. The `probe_fn` closure is expected
/// to hold a `shared_ptr` to the underlying structure, so the data stays alive as
/// long as this filter is alive.
class SharedFixedHashTableRuntimeFilter
{
public:
    static constexpr bool is_prebuilt = true;

    using ProbeFn = std::function<ColumnPtr(const ColumnWithTypeAndName &)>;

    SharedFixedHashTableRuntimeFilter(
        const DataTypePtr & filter_column_target_type_,
        ProbeFn probe_fn_,
        std::optional<Range> key_range_ = {},
        ColumnPtr recorded_key_values_ = {});

    /// All build entry points are no-ops: the data was built inside `HashJoin` already.
    void insert(ColumnPtr) { }
    void finishInsert(RuntimeFilterEvaluationState &) { }
    void mergeFrom(const SharedFixedHashTableRuntimeFilter &) { }
    ColumnPtr find(const ColumnWithTypeAndName & values, std::optional<size_t> & rows_passed) const;
    ColumnPtr getRecordedKeyValues() const { return recorded_key_values; }
    DataTypePtr getTargetType() const { return filter_column_target_type; }
    const std::optional<Range> & getInitialKeyRange() const { return key_range; }

private:
    const DataTypePtr filter_column_target_type;
    ProbeFn probe_fn;
    std::optional<Range> key_range;
    ColumnPtr recorded_key_values;
};

class RuntimeFilter final
{
public:
    using ExactContains = ExactSetRuntimeFilter<false>;
    using ExactNotContains = ExactSetRuntimeFilter<true>;
    using Adaptive = AdaptiveSetRuntimeFilter;
    using SharedFixedHashTable = SharedFixedHashTableRuntimeFilter;

private:
    using Filter = std::variant<ExactContains, ExactNotContains, Adaptive, SharedFixedHashTable>;

    struct Data
    {
        detail::RuntimeFilterBuildState build_state;
        Filter filter;
        bool index_analysis_enabled = false;
        bool has_range = false;
        Field range_min{};
        Field range_max{};
    };

    template <typename FilterImpl>
    static Data makeData(size_t filters_to_merge, FilterImpl && filter)
    {
        using FilterType = std::decay_t<FilterImpl>;
        Data result{
            detail::RuntimeFilterBuildState(FilterType::is_prebuilt ? 0 : filters_to_merge, FilterType::is_prebuilt),
            Filter(std::forward<FilterImpl>(filter))};
        if constexpr (std::is_same_v<FilterType, SharedFixedHashTable>)
        {
            result.index_analysis_enabled = true;
            if (const auto & range = std::get<SharedFixedHashTable>(result.filter).getInitialKeyRange())
            {
                result.range_min = range->left;
                result.range_max = range->right;
                result.has_range = true;
            }
        }
        return result;
    }

    RuntimeFilter(RuntimeFilterConfig config_, Data data_);

public:
    template <typename FilterImpl>
    RuntimeFilter(size_t filters_to_merge_, RuntimeFilterConfig config_, FilterImpl && filter_)
        : RuntimeFilter(std::move(config_), makeData(filters_to_merge_, std::forward<FilterImpl>(filter_)))
    {
    }

    void insert(ColumnPtr values);

    /// No more inserts after this call, only finds.
    void finishInsert();

    /// Looks up each value and returns column of Bool values.
    ColumnPtr find(const ColumnWithTypeAndName & values) const;

    /// Whether building and all expected merges have finished.
    bool isReady() const
    {
        SharedLockGuard lock(mutex);
        return data.build_state.isFinished();
    }

    /// Add all keys from one filter to the other so that destination filter contains the union of both filters.
    /// The source must be a distinct filter. Both filters are locked in stable address order.
    void merge(const RuntimeFilter & source);

    /// Opt in to collecting build-side metadata for storage index analysis.
    void enableIndexAnalysis();
    ColumnPtr getRecordedKeyValues() const;
    std::optional<Range> getRecordedKeyRanges() const;
    DataTypePtr getFilterColumnTargetType() const { return filter_column_target_type; }

    /// Usage statistics
    const RuntimeFilterStats & getStats() const { return evaluation_state.getStats(); }
    const RuntimeFilterConfig & getConfig() const { return evaluation_state.getConfig(); }

private:
    const DataTypePtr filter_column_target_type;
    const bool range_supported;
    const bool range_positive;

    RuntimeFilterEvaluationState evaluation_state;
    mutable SharedMutex mutex;
    Data data TSA_GUARDED_BY(mutex);
};

/// Store and find per-query runtime filters that are used for optimizing some kinds of JOINs
/// by early pre-filtering of the left side of the JOIN.
struct IRuntimeFilterLookup : boost::noncopyable
{
    virtual ~IRuntimeFilterLookup() = default;

    /// Add a runtime filter under the given rendezvous key. `display_name` is the readable structural
    /// id kept only for logging; the lookup is keyed by `key`.
    virtual void add(const String & key, const String & display_name, UniqueRuntimeFilterPtr runtime_filter) = 0;

    /// Replace the runtime filter with the specified name (if it exists, it is overwritten).
    /// Used by `HashJoin` to install a runtime filter backed by
    /// `SharedFixedHashTableRuntimeFilter` that supersedes the `Set`/`BloomFilter`
    /// built by `BuildRuntimeFilterStep`.
    virtual void replace(const String & name, UniqueRuntimeFilterPtr runtime_filter) = 0;

    /// Return a registered filter, which may still be building. Check `isReady` before probing.
    virtual RuntimeFilterConstPtr find(const String & name) const = 0;

    /// Log various RuntimeFilter usage statistics such as number of filtered rows
    virtual void logStats() const { }
};

using RuntimeFilterLookupPtr = std::shared_ptr<IRuntimeFilterLookup>;

RuntimeFilterLookupPtr createRuntimeFilterLookup();

/// A runtime filter (by rendezvous key) bound to a left-side column to prune.
struct RuntimeFilterIndexAnalysisDescriptor
{
    String filter_id;
    String key_column_name;
    DataTypePtr key_column_type;
};

/// Whether a runtime filter over a key of this type tracks the [min, max] envelope, i.e.
/// can produce a range predicate even after the exact value set overflows into a bloom filter.
bool runtimeFilterKeySupportsMinMaxRange(const DataTypePtr & type);

/// AND the descriptors into one pruning predicate; nullptr if none (fail-open).
const ActionsDAG::Node * buildRuntimeRangePredicate(
    const IRuntimeFilterLookup & lookup,
    const std::vector<RuntimeFilterIndexAnalysisDescriptor> & descriptors,
    ActionsDAG & dag,
    const ContextPtr & context);

}
