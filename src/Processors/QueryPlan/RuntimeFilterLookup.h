#pragma once

#include <Core/Range.h>
#include <DataTypes/hasNullable.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Set.h>

#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

class IRuntimeFilter;
using UniqueRuntimeFilterPtr = std::unique_ptr<IRuntimeFilter>;
using SharedRuntimeFilterPtr = std::shared_ptr<IRuntimeFilter>;
using RuntimeFilterConstPtr = std::shared_ptr<const IRuntimeFilter>;

struct RuntimeFilterStats
{
    std::atomic<Int64> rows_checked = 0;
    std::atomic<Int64> rows_passed = 0;
    std::atomic<Int64> rows_skipped = 0;
    std::atomic<Int64> blocks_processed = 0;
    std::atomic<Int64> blocks_skipped = 0;
};

class IRuntimeFilter
{
public:
    virtual ~IRuntimeFilter() = default;

    virtual void insert(ColumnPtr values) = 0;

    /// No more insert()-s after this call, only find()-s
    void finishInsert();

    /// Looks up each value and returns column of Bool-s
    ColumnPtr find(const ColumnWithTypeAndName & values) const;

    /// Add all keys from one filter to the other so that destination filter contains the union of both filters.
    virtual void merge(const IRuntimeFilter * source) = 0;

    /// The exact distinct key values collected from the build side, as a single column.
    virtual ColumnPtr getRecordedKeyValues() const { return nullptr; }

    /// A closed [min, max] envelope of the values inserted into the filter, if one can be computed
    virtual std::optional<Range> getRecordedKeyRanges() const;

    /// Opt in to tracking the [min, max] key-range envelope during build.
    void enableIndexAnalysis() { index_analysis_enabled = true; }

    /// Usage statistics
    void updateStats(UInt64 rows_checked, UInt64 rows_passed) const;
    const RuntimeFilterStats & getStats() const { return stats; }
    void setFullyDisabled() { is_fully_disabled = true; }

    Float64 getPassRatioThresholdForDisabling() const { return pass_ratio_threshold_for_disabling; }
    UInt64 getBlocksToSkipBeforeReenabling() const { return blocks_to_skip_before_reenabling; }
    const DataTypePtr & getFilterColumnTargetType() const { return filter_column_target_type; }

protected:

    IRuntimeFilter(
        size_t filters_to_merge_,
        const DataTypePtr & filter_column_target_type_,
        Float64 pass_ratio_threshold_for_disabling_,
        UInt64 blocks_to_skip_before_reenabling_);

    /// Checks if a block of rows should be skipped because this filter was disabled.
    bool shouldSkip(size_t next_block_rows) const;

    virtual void finishInsertImpl() = 0;

    virtual ColumnPtr findImpl(const ColumnWithTypeAndName & values) const = 0;

    void updateRange(const IColumn & column);
    /// Merges another filter's envelope in, for parallel build streams.
    void mergeRange(const IRuntimeFilter & source);

    size_t filters_to_merge;
    const DataTypePtr filter_column_target_type;

    std::atomic<bool> inserts_are_finished = false;

    const Float64 pass_ratio_threshold_for_disabling = 0.7;
    const UInt64 blocks_to_skip_before_reenabling = 30;

    mutable RuntimeFilterStats stats;

    /// How many rows should be skipped before trying to re-enable the filter after it was disabled due to
    /// low percentage of filtered rows
    mutable std::atomic<Int64> rows_to_skip = 0;
    std::atomic<bool> is_fully_disabled = false;

    /// Key-range envelope tracking (see updateRange/getRecordedKeyRanges).
    bool index_analysis_enabled = false;
    bool range_supported = false;
    bool range_positive = true;
    bool has_range = false;
    Field range_min;
    Field range_max;
};

template <bool negate>
class RuntimeFilterBase : public IRuntimeFilter
{
public:

    RuntimeFilterBase(
        size_t filters_to_merge_,
        const DataTypePtr & filter_column_target_type_,
        Float64 pass_ratio_threshold_for_disabling_,
        UInt64 blocks_to_skip_before_reenabling_,
        UInt64 bytes_limit_,
        UInt64 exact_values_limit_
    )
        : IRuntimeFilter(filters_to_merge_, filter_column_target_type_, pass_ratio_threshold_for_disabling_, blocks_to_skip_before_reenabling_)
        , argument_can_have_nulls(hasTypeThatCanContainNulls(filter_column_target_type))
        , bytes_limit(bytes_limit_)
        , exact_values_limit(exact_values_limit_)
        , exact_values(std::make_shared<Set>(SizeLimits{}, -1, argument_can_have_nulls))
    {
        ColumnsWithTypeAndName set_header = { ColumnWithTypeAndName(filter_column_target_type, String()) };
        exact_values->setHeader(set_header);
        exact_values->fillSetElements();    /// Save the values, not just hashes
    }

    void insert(ColumnPtr values) override
    {
        if (inserts_are_finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert into runtime filter after it was marked as finished");

        /// Track the envelope of every value even when the set is full.
        updateRange(*values);

        if (is_full)
            return;

        exact_values->insertFromColumns({values});
        is_full = exact_values->getTotalRowCount() > exact_values_limit || exact_values->getTotalByteCount() > bytes_limit;
    }

    void finishInsertImpl() override
    {
        exact_values->finishInsert();

        /// If the set is empty just return Const False column
        if (exact_values->getTotalRowCount() == 0)
        {
            values_count = ValuesCount::ZERO;
            return;
        }

        /// If only 1 element in the set then use " == const" instead of set lookup
        /// But if the argument is Nullable we cannot use "==" so fallback to Set because it can handle NULLs
        if (exact_values->getTotalRowCount() == 1 && !argument_can_have_nulls)
        {
            values_count = ValuesCount::ONE;
            single_element_column = exact_values->getSetElements().front();
            return;
        }

        values_count = ValuesCount::MANY;
    }

    ColumnPtr findImpl(const ColumnWithTypeAndName & values) const override;

    ColumnPtr getRecordedKeyValues() const override
    {
        /// ANTI (NOT IN) can't be used as a positive IN predicate on the left side.
        if constexpr (negate)
            return nullptr;
        /// exact_values is released once the set overflows to a bloom filter.
        if (!index_analysis_enabled || !exact_values)
            return nullptr;
        const auto elements = exact_values->getSetElements();
        if (elements.empty())
            return nullptr;
        return elements.front();
    }

protected:

    UInt64 getBytesLimit() const noexcept { return bytes_limit; }

    bool isFull() const noexcept { return is_full; }

    ColumnPtr getValuesColumn() const
    {
        exact_values->finishInsert();
        return exact_values->getSetElements().front();
    }

    void releaseExactValues() { exact_values.reset(); }

private:
    enum class ValuesCount
    {
        UNKNOWN,
        ZERO, /// If filter set has no elements then find() always returns false
        ONE, /// If filter set has just one element then "find(value)" is replaced with "value==element"
        MANY,
    };

    const bool argument_can_have_nulls;
    const UInt64 bytes_limit;
    const UInt64 exact_values_limit;

    SetPtr exact_values;
    ValuesCount values_count = ValuesCount::UNKNOWN;

    bool is_full = false;

    ColumnPtr single_element_column;
};

class ExactContainsRuntimeFilter : public RuntimeFilterBase<false>
{
    using Base = RuntimeFilterBase<false>;

public:
    ExactContainsRuntimeFilter(
        size_t filters_to_merge_,
        const DataTypePtr & filter_column_target_type_,
        Float64 pass_ratio_threshold_for_disabling_,
        UInt64 blocks_to_skip_before_reenabling_,
        UInt64 bytes_limit_,
        UInt64 exact_values_limit_
    )
        : RuntimeFilterBase(filters_to_merge_, filter_column_target_type_, pass_ratio_threshold_for_disabling_, blocks_to_skip_before_reenabling_, bytes_limit_, exact_values_limit_)
    {}

    void merge(const IRuntimeFilter * source) override;

    void finishInsertImpl() override;
};

class ExactNotContainsRuntimeFilter : public RuntimeFilterBase<true>
{
public:
    ExactNotContainsRuntimeFilter(
        size_t filters_to_merge_,
        const DataTypePtr & filter_column_target_type_,
        Float64 pass_ratio_threshold_for_disabling_,
        UInt64 blocks_to_skip_before_reenabling_,
        UInt64 bytes_limit_,
        UInt64 exact_values_limit_
    )
        : RuntimeFilterBase(filters_to_merge_, filter_column_target_type_, pass_ratio_threshold_for_disabling_, blocks_to_skip_before_reenabling_, bytes_limit_, exact_values_limit_)
    {
        /// ANTI join: a positive range on the left is unsound, so expose none.
        range_positive = false;
    }

    void merge(const IRuntimeFilter * source) override;
};

/// As long as the number of unique values is small they are stored in a Set but when it grows beyond the limit
/// the values are moved into a BloomFilter.
class ApproximateRuntimeFilter : public RuntimeFilterBase<false>
{
    using Base = RuntimeFilterBase<false>;
public:
    static bool isDataTypeSupported(const DataTypePtr & data_type);

    ApproximateRuntimeFilter(
        size_t filters_to_merge_,
        const DataTypePtr & filter_column_target_type_,
        Float64 pass_ratio_threshold_for_disabling_,
        UInt64 blocks_to_skip_before_reenabling_,
        UInt64 bytes_limit_,
        UInt64 exact_values_limit_,
        UInt64 bloom_filter_hash_functions_,
        Float64 max_ratio_of_set_bits_in_bloom_filter_,
        std::optional<UInt64> distinct_keys_hint_);

    void insert(ColumnPtr values) override;

    /// No more insert()-s after this call, only find()-s
    void finishInsertImpl() override;

    /// Looks up each value and returns column of Bool-s
    ColumnPtr findImpl(const ColumnWithTypeAndName & values) const override;

    /// Add all keys from one filter to the other so that destination filter contains the union of both filters.
    void merge(const IRuntimeFilter * source) override;

private:
    void insertIntoBloomFilter(ColumnPtr values);
    void switchToBloomFilter();

    /// Disables bloom filter if it is likely to have bad selectivity
    void checkBloomFilterWorthiness();

    const UInt64 bloom_filter_hash_functions;
    const Float64 max_ratio_of_set_bits_in_bloom_filter = 0.7;
    /// Measured distinct build-side keys from prior statistics, used to choose the bloom filter size.
    const std::optional<UInt64> distinct_keys_hint;

    BloomFilterPtr bloom_filter;
};

/// Runtime filter that delegates probe to a function captured at publication time.
/// Used to share an already-built data structure (e.g. HashJoin's FixedHashMap)
/// as a runtime filter without copying the data. The probe_fn closure is expected
/// to hold a shared_ptr to the underlying structure, so the data stays alive as
/// long as this filter is alive.
class SharedFixedHashTableRuntimeFilter final : public IRuntimeFilter
{
public:
    using ProbeFn = std::function<ColumnPtr(const ColumnWithTypeAndName &)>;

    SharedFixedHashTableRuntimeFilter(
        const DataTypePtr & filter_column_target_type_,
        Float64 pass_ratio_threshold_for_disabling_,
        UInt64 blocks_to_skip_before_reenabling_,
        ProbeFn probe_fn_,
        std::optional<Range> key_range_ = {},
        ColumnPtr recorded_key_values_ = {});

    /// All "build" entry points are no-ops: the data was built inside HashJoin already.
    void insert(ColumnPtr) override {}
    void merge(const IRuntimeFilter *) override {}

    /// Carried over from the replaced filter (non-null only when it kept an exact set), so the
    /// left-side index-analysis path can still build IN(...) instead of only a [min,max] range.
    ColumnPtr getRecordedKeyValues() const override { return recorded_key_values; }

protected:
    void finishInsertImpl() override {}
    ColumnPtr findImpl(const ColumnWithTypeAndName & values) const override;

private:
    ProbeFn probe_fn;
    ColumnPtr recorded_key_values;
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
    /// Used by HashJoin to install a SharedFixedHashTableRuntimeFilter that supersedes the
    /// Set/BloomFilter built by BuildRuntimeFilterStep.
    virtual void replace(const String & name, UniqueRuntimeFilterPtr runtime_filter) = 0;

    /// Get filter by name
    virtual RuntimeFilterConstPtr find(const String & name) const = 0;

    /// Log various RuntimeFilter usage statistics such as number of filtered rows
    virtual void logStats() const {}
};

using RuntimeFilterLookupPtr = std::shared_ptr<IRuntimeFilterLookup>;

RuntimeFilterLookupPtr createRuntimeFilterLookup();

/// A runtime filter (by rendezvous key) bound to a left-side column to prune.
struct RuntimeFilterIndexAnalysisDescriptor
{
    String filter_id;          /// rendezvous key for IRuntimeFilterLookup::find
    String key_column_name;
    DataTypePtr key_column_type;
};

/// AND the descriptors into one pruning predicate; nullptr if none (fail-open).
const ActionsDAG::Node * buildRuntimeRangePredicate(
    const IRuntimeFilterLookup & lookup,
    const std::vector<RuntimeFilterIndexAnalysisDescriptor> & descriptors,
    ActionsDAG & dag,
    const ContextPtr & context);

}
