#pragma once
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/SetVariants.h>
#include <Interpreters/BloomFilter.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/ThreadPool.h>

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
        size_t max_threads_);

    String getName() const override { return "DistinctTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ColumnNumbers key_columns_pos;
    SetVariants data;
    std::unique_ptr<BloomFilter> bloom_filter;
    std::unique_ptr<ThreadPool> pool;

    ///Statistics for BloomFilter optimization
    size_t total_passed_bf = 0;
    size_t new_passes = 0;
    bool use_bf = false;
    bool try_init_bf;

    Sizes key_sizes;
    const UInt64 limit_hint;

    const bool is_pre_distinct;

    /// Restrictions on the maximum size of the output data.
    SizeLimits set_size_limits;

    template <typename Method>
    void buildCombinedFilter(
        Method & method,
        const ColumnRawPtrs & columns,
        IColumnFilter & filter,
        size_t rows,
        SetVariants & variants,
        size_t &  passed_bf) const;

    template <typename Method>
    void buildSetFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumn::Filter & filter,
        size_t rows,
        SetVariants & variants) const;

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
};

}
