#pragma once
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/SetVariants.h>
#include <Interpreters/BloomFilter.h>
#include "Common/ThreadPool_fwd.h"
#include <Common/ThreadPool.h>
#include "Common/PODArray_fwd.h"
#include "Columns/ColumnsNumber.h"
#include "Columns/IColumn.h"
#include "Columns/IColumn_fwd.h"
#include "base/types.h"

namespace DB
{

class DistinctChunkInfo final : public ChunkInfoCloneable<DistinctChunkInfo>
{
public:
    bool lazy_filter = false;
    size_t bucket_column_pos;
    size_t filter_column_pos;

    DistinctChunkInfo() = default;

    DistinctChunkInfo(const DistinctChunkInfo& other)
    : lazy_filter(other.lazy_filter), bucket_column_pos(other.bucket_column_pos), filter_column_pos(other.filter_column_pos)
    {}
};

class DistinctTransform : public ISimpleTransform
{
public:
    DistinctTransform(
        const Block & header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        bool is_pre_distinct_,
        size_t max_threads_);

    static Block transformHeader(const Block & header, bool is_pre_distinct);

    String getName() const override { return "DistinctTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ColumnNumbers key_columns_pos;
    SetVariants data;
    std::unique_ptr<BloomFilter> bloom_filter;

    ///Statistics for BloomFilter optimization
    size_t total_passed_bf = 0;
    size_t new_passes = 0;
    bool use_bf = false;
    bool try_init_bf = true;

    Sizes key_sizes;
    const UInt64 limit_hint;

    const bool is_pre_distinct;

    /// Restrictions on the maximum size of the output data.
    SizeLimits set_size_limits;
    const UInt64 max_rows_in_distinct_before_bloom_filter_passthrough = 100000;

    std::unique_ptr<ThreadPool> pool;

    template <typename Method>
    void buildSetFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumnFilter & filter,
        size_t rows,
        SetVariants & variant) const;

    template <typename Method>
    void buildSetBucketFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumnFilter & filter,
        size_t rows,
        SetVariants & variant,
        PaddedPODArray<UInt8> & bucket) const;

    template <typename Method>
    void checkSetBucketFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumnFilter & filter,
        size_t rows,
        SetVariants & variant,
        size_t & passed_bf,
        PaddedPODArray<UInt8> & bucket) const;

    template <typename Method>
    void buildSetFinalFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumnFilter & filter,
        size_t rows,
        SetVariants & variant) const;

    template <typename Method>
    void buildSetParallelFinalFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumnFilter & filter,
        size_t rows,
        SetVariants & variant,
        const PaddedPODArray<UInt8> & bucket,
        ThreadPool & thread_pool) const;

    template <typename Method>
    void buildCombinedFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumnFilter & filter,
        size_t rows,
        SetVariants & variant,
        size_t & passed_bf) const;

    template <typename Method>
    void buildCombinedBucketFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumnFilter & filter,
        size_t rows,
        SetVariants & variant,
        size_t & passed_bf,
        PaddedPODArray<UInt8> & bucket) const;
    };
}
