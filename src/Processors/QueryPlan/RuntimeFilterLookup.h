#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/hasNullable.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/Set.h>

#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <cstddef>
#include <memory>

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

    /// Usage statistics
    void updateStats(UInt64 rows_checked, UInt64 rows_passed) const;
    const RuntimeFilterStats & getStats() const { return stats; }
    void setFullyDisabled() { is_fully_disabled = true; }

protected:

    IRuntimeFilter(
        size_t filters_to_merge_,
        const DataTypePtr & filter_column_target_type_,
        Float64 pass_ratio_threshold_for_disabling_,
        UInt64 blocks_to_skip_before_reenabling_)
        : filters_to_merge(filters_to_merge_)
        , filter_column_target_type(filter_column_target_type_)
        , pass_ratio_threshold_for_disabling(pass_ratio_threshold_for_disabling_)
        , blocks_to_skip_before_reenabling(blocks_to_skip_before_reenabling_)
    {}

    /// Checks if a block of rows should be skipped because this filter was disabled.
    bool shouldSkip(size_t next_block_rows) const;

    virtual void finishInsertImpl() = 0;

    virtual ColumnPtr findImpl(const ColumnWithTypeAndName & values) const = 0;

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
        , argument_can_have_nulls(hasNullable(filter_column_target_type) ||
            WhichDataType(filter_column_target_type).isDynamic())
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
            single_element_in_set = (*exact_values->getSetElements().front())[0];
            return;
        }

        values_count = ValuesCount::MANY;
    }

    ColumnPtr findImpl(const ColumnWithTypeAndName & values) const override;

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

    std::optional<Field> single_element_in_set;
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
    {}

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
        Float64 max_ratio_of_set_bits_in_bloom_filter_);

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

    BloomFilterPtr bloom_filter;
};

/// Store and find per-query runtime filters that are used for optimizing some kinds of JOINs
/// by early pre-filtering of the left side of the JOIN.
struct IRuntimeFilterLookup : boost::noncopyable
{
    virtual ~IRuntimeFilterLookup() = default;

    /// Add runtime filter with the specified name
    virtual void add(const String & name, UniqueRuntimeFilterPtr runtime_filter) = 0;

    /// Get filter by name
    virtual RuntimeFilterConstPtr find(const String & name) const = 0;

    /// Log various RuntimeFilter usage statistics such as number of filtered rows
    virtual void logStats() const {}
};

using RuntimeFilterLookupPtr = std::shared_ptr<IRuntimeFilterLookup>;

}
