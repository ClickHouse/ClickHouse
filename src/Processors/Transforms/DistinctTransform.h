#pragma once

#include <Columns/ColumnLowCardinality.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/SetVariants.h>
#include <Interpreters/BloomFilter.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/ThreadPool.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/ColumnsHashing.h>
#include <base/types.h>

#include <unordered_map>

namespace DB
{

class DistinctTransform : public ISimpleTransform
{
public:
    DistinctTransform(
        SharedHeader header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        bool is_pre_distinct_,
        UInt64 set_limit_for_enabling_bloom_filter_,
        UInt64 bloom_filter_bytes_,
        Float64 pass_ratio_threshold_for_disabling_bloom_filter_,
        Float64 max_ratio_of_set_bits_in_bloom_filter_,
        size_t max_threads_);

    String getName() const override { return "DistinctTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ColumnNumbers key_columns_pos;
    SetVariants data;
    std::unique_ptr<BloomFilter> bloom_filter;
    std::unique_ptr<ThreadPool> pool;


    Sizes key_sizes;
    const UInt64 limit_hint;

    const bool is_pre_distinct;

    /// BloomFilter Pre DISTINCT optimization
    size_t total_passed_bf = 0;
    bool use_bf = false;
    bool try_init_bf;
    UInt64 set_limit_for_enabling_bloom_filter = 1000000;
    UInt64 bloom_filter_bytes = 0;
    Float64 pass_ratio_threshold_for_disabling_bloom_filter = 0.7;
    Float64 max_ratio_of_set_bits_in_bloom_filter = 0.7;
    UInt64 bf_worthless_last_set_bits = 0;
    UInt64 bf_worthless_total_set_bits = 0;
    UInt64 bf_worthless_last_bf_pass = 0;

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

    /// mask[i] == 0 -> row i is known duplicate (by LC index) and is never inserted.
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
        size_t &  passed_bf) const;

    template <typename Method>
    void checkSetFilter(
        Method & method,
        const ColumnRawPtrs & columns,
        IColumnFilter & filter,
        size_t rows,
        SetVariants & variants,
        size_t &  passed_bf) const;

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

    /// For a single LowCardinality key column, build a mask of rows that are
    /// the first occurrence of their LC dictionary index for this dictionary identity. Then, only those
    /// rows need to be checked for distinctness.
    IColumn::Filter buildLowCardinalityMask(const ColumnLowCardinality & column, size_t num_rows);
};

}
