#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/Set.h>
#include <Common/MutexProtected.h>

#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <variant>

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
    void finishMerge();
    void finishInserts() { inserts_are_finished = true; }

private:
    size_t filters_to_merge = 0;
    bool inserts_are_finished = false;
};

}

class RuntimeFilterEvaluationState
{
public:
    explicit RuntimeFilterEvaluationState(RuntimeFilterConfig config_);

    void updateStats(UInt64 rows_checked, UInt64 rows_passed) const;
    const RuntimeFilterStats & getStats() const { return stats; }
    const RuntimeFilterConfig & getConfig() const { return config; }
    void setFullyDisabled() { is_fully_disabled = true; }

    /// Checks if a block of rows should be skipped because this filter was disabled.
    bool shouldSkip(size_t next_block_rows) const;

private:
    const RuntimeFilterConfig config;

    mutable RuntimeFilterStats stats;

    /// How many rows should be skipped before trying to re-enable the filter after it was disabled due to
    /// low percentage of filtered rows
    mutable std::atomic<Int64> rows_to_skip = 0;
    std::atomic<bool> is_fully_disabled = false;
};

template <bool negate>
class ExactSetRuntimeFilter
{
public:
    static constexpr bool is_prebuilt = false;

    ExactSetRuntimeFilter(
        const DataTypePtr & filter_column_target_type_,
        UInt64 bytes_limit_,
        UInt64 exact_values_limit_
    );

    UInt64 getBytesLimit() const noexcept { return bytes_limit; }

    bool isFull() const noexcept { return is_full; }

    void insert(ColumnPtr values);
    void finishInsert();
    void finishInsert(RuntimeFilterEvaluationState & evaluation_state);
    ColumnPtr find(const ColumnWithTypeAndName & values) const;
    ColumnPtr getValuesColumn() const;
    void mergeFrom(const ExactSetRuntimeFilter & source);
    void releaseExactValues();

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
    Set & getExactValues() const;

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

    ApproximateSetRuntimeFilter(
        UInt64 bytes_limit_,
        UInt64 bloom_filter_hash_functions_);

    void insert(ColumnPtr values);
    ColumnPtr find(const ColumnWithTypeAndName & values) const;
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
        Float64 max_ratio_of_set_bits_in_bloom_filter_);

    void insert(ColumnPtr values);
    void finishInsert(RuntimeFilterEvaluationState & evaluation_state);
    ColumnPtr find(const ColumnWithTypeAndName & values) const;
    void mergeFrom(const AdaptiveSetRuntimeFilter & source);

private:
    using ExactFilter = ExactSetRuntimeFilter<false>;
    using Filter = std::variant<ExactFilter, ApproximateSetRuntimeFilter>;

    void insert(ColumnPtr values, Filter & filter);
    ApproximateSetRuntimeFilter & switchToApproximateFilter(Filter & filter);

    /// Disables approximate filter if it is likely to have bad selectivity.
    void checkApproximateFilterWorthiness(RuntimeFilterEvaluationState & evaluation_state, const ApproximateSetRuntimeFilter & approximate_filter);

    const UInt64 bloom_filter_hash_functions;
    const Float64 max_ratio_of_set_bits_in_bloom_filter = 0.7;

    Filter filter;
};

/// Runtime filter that delegates probe to a function captured at publication time.
/// Used to share an already-built data structure (e.g. `HashJoin`'s `FixedHashMap`)
/// as a runtime filter without copying the data. The `probe_fn` closure is expected
/// to hold a `shared_ptr` to the underlying structure, so the data stays alive as
/// long as this filter is alive.
class SharedFixedHashTableRuntimeFilterImpl
{
public:
    static constexpr bool is_prebuilt = true;

    using ProbeFn = std::function<ColumnPtr(const ColumnWithTypeAndName &)>;

    explicit SharedFixedHashTableRuntimeFilterImpl(ProbeFn probe_fn_);

    /// All build entry points are no-ops: the data was built inside `HashJoin` already.
    void insert(ColumnPtr) {}
    void finishInsert(RuntimeFilterEvaluationState &) {}
    void mergeFrom(const SharedFixedHashTableRuntimeFilterImpl &) {}
    ColumnPtr find(const ColumnWithTypeAndName & values) const;

private:
    ProbeFn probe_fn;
};

class RuntimeFilter final
{
public:
    using ExactContains = ExactSetRuntimeFilter<false>;
    using ExactNotContains = ExactSetRuntimeFilter<true>;
    using Adaptive = AdaptiveSetRuntimeFilter;
    using SharedFixedHashTable = SharedFixedHashTableRuntimeFilterImpl;

private:
    using Filter = std::variant<ExactContains, ExactNotContains, Adaptive, SharedFixedHashTable>;

    struct Data
    {
        detail::RuntimeFilterBuildState build_state;
        Filter filter;
    };

    RuntimeFilter(RuntimeFilterConfig config_, Data data_)
        : evaluation_state(std::move(config_))
        , data(std::move(data_))
    {
    }

public:
    template <typename FilterImpl>
    RuntimeFilter(
        size_t filters_to_merge_,
        RuntimeFilterConfig config_,
        FilterImpl && filter_)
        : RuntimeFilter(
            std::move(config_),
            Data{
                detail::RuntimeFilterBuildState(
                    std::decay_t<FilterImpl>::is_prebuilt ? 0 : filters_to_merge_,
                    std::decay_t<FilterImpl>::is_prebuilt),
                Filter(std::forward<FilterImpl>(filter_))})
    {
    }

    void insert(ColumnPtr values);

    /// No more inserts after this call, only finds.
    void finishInsert();

    /// Looks up each value and returns column of Bool values.
    ColumnPtr find(const ColumnWithTypeAndName & values) const;

    /// Add all keys from one filter to the other so that destination filter contains the union of both filters.
    void merge(const RuntimeFilter * source);

    /// Usage statistics
    const RuntimeFilterStats & getStats() const { return evaluation_state.getStats(); }
    const RuntimeFilterConfig & getConfig() const { return evaluation_state.getConfig(); }

private:
    RuntimeFilterEvaluationState evaluation_state;
    MutexProtected<Data> data;
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
    /// `SharedFixedHashTableRuntimeFilterImpl` that supersedes the `Set`/`BloomFilter`
    /// built by `BuildRuntimeFilterStep`.
    virtual void replace(const String & name, UniqueRuntimeFilterPtr runtime_filter) = 0;

    /// Get filter by name
    virtual RuntimeFilterConstPtr find(const String & name) const = 0;

    /// Log various RuntimeFilter usage statistics such as number of filtered rows
    virtual void logStats() const {}
};

using RuntimeFilterLookupPtr = std::shared_ptr<IRuntimeFilterLookup>;

RuntimeFilterLookupPtr createRuntimeFilterLookup();

}
