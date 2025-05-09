#include <cassert>
#include <cstddef>
#include <memory>
#include <set>
#include <vector>
#include <Processors/Transforms/DistinctTransform.h>
#include "Common/PODArray.h"
#include "Common/PODArray_fwd.h"
#include "Common/ThreadPool_fwd.h"
#include <Common/ThreadPool.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/scope_guard_safe.h>
#include <Common/CurrentMetrics.h>
#include <exception>
#include "Common/assert_cast.h"
#include "Columns/ColumnVector.h"
#include "Columns/ColumnsNumber.h"
#include "Columns/IColumn.h"
#include "Columns/IColumn_fwd.h"
#include "Core/Block.h"
#include "Core/ColumnNumbers.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "DataTypes/DataTypesNumber.h"
#include "Interpreters/AggregationCommon.h"
#include "Interpreters/BloomFilter.h"
#include "Interpreters/SetVariants.h"
#include "Processors/Chunk.h"
#include "Processors/Transforms/SortingTransform.h"
#include "base/defines.h"
#include "base/types.h"

static inline size_t intHash32(UInt64 x)
{
    x = (~x) + (x << 18);
    x = x ^ ((x >> 31) | (x << 33));
    x = x * 21;
    x = x ^ ((x >> 11) | (x << 53));
    x = x + (x << 6);
    x = x ^ ((x >> 22) | (x << 42));

    return x;
}


static constexpr size_t BITS_FOR_BUCKET = 8;
static constexpr UInt32 NUM_BUCKETS = 1ULL << BITS_FOR_BUCKET;
static constexpr UInt32 MAX_BUCKET = NUM_BUCKETS - 1;

namespace CurrentMetrics
{
    extern const Metric DestroyAggregatesThreads;
    extern const Metric DestroyAggregatesThreadsActive;
    extern const Metric DestroyAggregatesThreadsScheduled;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

//static constexpr UInt64 SEED_GEN_A = 845897321;

DistinctTransform::DistinctTransform(
    const Block & header_,
    const SizeLimits & set_size_limits_,
    UInt64 limit_hint_,
    const Names & columns_,
    bool is_pre_distinct_,
    size_t max_threads_)
    : ISimpleTransform(header_, transformHeader(header_, is_pre_distinct_), true)
    , limit_hint(limit_hint_)
    , is_pre_distinct(is_pre_distinct_)
    , set_size_limits(set_size_limits_)
{
    const size_t num_columns = columns_.empty() ? header_.columns() : columns_.size();
    key_columns_pos.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto pos = columns_.empty() ? i : header_.getPositionByName(columns_[i]);
        const auto & col = header_.getByPosition(pos).column;
        if (col && !isColumnConst(*col))
            key_columns_pos.emplace_back(pos);
    }

    if (is_pre_distinct_)
    {
        pool = nullptr;
    } else
    {
        pool = std::make_unique<ThreadPool>(
            CurrentMetrics::DestroyAggregatesThreads,
            CurrentMetrics::DestroyAggregatesThreadsActive,
            CurrentMetrics::DestroyAggregatesThreadsScheduled,
            max_threads_);
    }

    setInputNotNeededAfterRead(true);
}

Block DistinctTransform::transformHeader(const Block & header, bool is_pre_distinct)
{
    Block result = header;

    if (is_pre_distinct)
    {
        result.insertUnique(ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(),"_pre_distinct_bucket"));
        result.insertUnique(ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(),"_pre_distinct_filter"));
    }
    else
    {
        if (result.findByName("_pre_distinct_bucket"))
            result.erase("_pre_distinct_bucket");
        if (result.findByName("_pre_distinct_filter"))
            result.erase("_pre_distinct_filter");
    }

    return result;
}

template <typename Method>
void DistinctTransform::buildCombinedFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumnFilter & filter,
    const size_t rows,
    SetVariants & variants,
    size_t &  passed_bf) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
    {

        auto row_hash = columns[0]->getInt(i);

        auto hash1 = row_hash;
        auto hash2 = intHash32(row_hash);

        auto has_element = bloom_filter->findRawHash(hash1/*, SEED_GEN_A*/) && bloom_filter->findRawHash(hash2/*, SEED_GEN_A*/);

        if (has_element)
        {
            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
            /// Emit the record if there is no such key in the current set yet.
            /// Skip it otherwise.
            filter[i] = emplace_result.isInserted();
        } else
        {
            bloom_filter->addRawHash(hash1/*, SEED_GEN_A*/);
            bloom_filter->addRawHash(hash2/*, SEED_GEN_A*/);
            passed_bf++;
        }
    }
}

template <typename Method>
void DistinctTransform::buildCombinedBucketFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumnFilter & filter,
    const size_t rows,
    SetVariants & variants,
    size_t &  passed_bf,
    PaddedPODArray<UInt8> & bucket) const
{
    typename Method::State state(columns, key_sizes, nullptr);


    for (size_t i = 0; i < rows; ++i)
    {
        //auto result = state.findKey(method.data, i, variants.string_pool);

        if (false)//result.isFound()
        {
            filter[i] = false;
            continue;
        } else
        {
            auto row_hash = state.getHash(method.data, i, variants.string_pool);
            auto hash1 = row_hash;
            auto hash2 = intHash32(row_hash);
            auto bucket_id = (row_hash >> (32 - BITS_FOR_BUCKET)) & MAX_BUCKET;

            auto has_element = bloom_filter->findRawHash(hash1/*, SEED_GEN_A*/) && bloom_filter->findRawHash(hash2/*, SEED_GEN_A*/);

            if (has_element)
            {
                auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
                /// Emit the record if there is no such key in the current set yet.
                /// Skip it otherwise.
                filter[i] = emplace_result.isInserted();
                bucket[i] = bucket_id;
            } else
            {
                bloom_filter->addRawHash(hash1/*, SEED_GEN_A*/);
                bloom_filter->addRawHash(hash2/*, SEED_GEN_A*/);
                passed_bf++;
                bucket[i] = bucket_id;
            }
        }
    }
}

template <typename Method>
void DistinctTransform::buildSetFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumnFilter & filter,
    const size_t rows,
    SetVariants & variants) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
    {
        auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
        /// Emit the record if there is no such key in the current set yet.
        /// Skip it otherwise.
        filter[i] = emplace_result.isInserted();
    }
}

template <typename Method>
void DistinctTransform::buildSetBucketFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumnFilter & filter,
    const size_t rows,
    SetVariants & variants,
    PaddedPODArray<UInt8> & bucket) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
    {
        auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
        /// Emit the record if there is no such key in the current set yet.
        /// Skip it otherwise.
        filter[i] = emplace_result.isInserted();

        if (emplace_result.isInserted())
        {
            auto row_hash = state.getHash(method.data, i, variants.string_pool);
            auto bucket_id = (row_hash >> (32 - BITS_FOR_BUCKET)) & MAX_BUCKET;
            bucket[i] = bucket_id;
        }
    }
}

template <typename Method>
void DistinctTransform::checkSetBucketFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumnFilter & filter,
    const size_t rows,
    SetVariants & variants,
    size_t &  passed_bf,
    PaddedPODArray<UInt8> & bucket) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
    {
        auto find_result = state.findKey(method.data, i, variants.string_pool);
        /// Emit the record if there is no such key in the current set yet.
        /// Skip it otherwise.
        filter[i] = !find_result.isFound();

        if (!find_result.isFound())
        {
            auto row_hash = state.getHash(method.data, i, variants.string_pool);
            auto bucket_id = (row_hash >> (32 - BITS_FOR_BUCKET)) & MAX_BUCKET;
            bucket[i] = bucket_id;
            passed_bf++;
        }
    }
}


template <typename Method>
void DistinctTransform::buildSetParallelFinalFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumnFilter & filter,
    const size_t rows,
    SetVariants & variants,
    const PaddedPODArray<UInt8> & bucket,
    ThreadPool & thread_pool) const
{
    typename Method::State state(columns, key_sizes, nullptr);
    auto thread_group = CurrentThread::getGroup();

    constexpr size_t num_buckets = 256;

    /// 1. Count rows per bucket
    std::array<size_t, num_buckets> bucket_sizes{};
    for (size_t i = 0; i < rows; ++i)
    {
        if (filter[i])
        {
            size_t b = bucket[i];
            if (b >= num_buckets)
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Bucket index {} out of range", b);
            ++bucket_sizes[b];
        }
    }

    /// 2. Compute start offset for each bucket
    std::array<size_t, num_buckets + 1> bucket_offsets{};
    size_t total_rows = 0;
    for (size_t b = 0; b < num_buckets; ++b)
    {
        bucket_offsets[b] = total_rows;
        total_rows += bucket_sizes[b];
    }
    bucket_offsets[num_buckets] = total_rows;

    /// 3. Allocate flat array of all indices
    PODArray<size_t> all_indices(total_rows);

    /// 4. Fill in the array, writing per-bucket indices at known offset
    auto write_positions = bucket_offsets;
    for (size_t i = 0; i < rows; ++i)
    {
        if (filter[i])
        {
            size_t b = bucket[i];
            all_indices[write_positions[b]++] = i;
        }
    }

    /// 2. Thread-safe iteration through this small list
    auto next = std::make_shared<std::atomic<size_t>>(0);

    for (size_t thread_id = 0; thread_id < thread_pool.getMaxThreads(); ++thread_id)
    {

        thread_pool.scheduleOrThrowOnError([next, &bucket_offsets, &all_indices, &state, &method, &variants, &filter, thread_group]()
        {
            ThreadGroupSwitcher switcher(thread_group, "DistinctFinal", true);

            while (true)
            {
                size_t idx = next->fetch_add(1);
                if (idx >= num_buckets)
                    return;

                size_t begin = bucket_offsets[idx];
                size_t end = bucket_offsets[idx + 1];

                if (begin == end)
                    continue;

                for (size_t j = begin; j < end; ++j)
                {
                    size_t i = all_indices[j];
                    auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
                    filter[i] = emplace_result.isInserted();
                }
            }

        });
    }
    thread_pool.wait();
}

template <typename Method>
void DistinctTransform::buildSetFinalFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumnFilter & filter,
    const size_t rows,
    SetVariants & variants) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
    {
        if (!filter[i])
            continue;

        auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
        /// Emit the record if there is no such key in the current set yet.
        /// Skip it otherwise.
        filter[i] = emplace_result.isInserted();
    }
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    /// Convert to full column, because SetVariant for sparse column is not implemented.
    convertToFullIfSparse(chunk);
    convertToFullIfConst(chunk);

    const auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    /// Special case, - only const columns, return single row
    if (unlikely(key_columns_pos.empty()))
    {
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
        stopReading();
        return;
    }

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(key_columns_pos.size());
    for (auto pos : key_columns_pos)
        column_ptrs.emplace_back(columns[pos].get());

    const auto old_set_size = data.getTotalRowCount();
    const auto old_bf_size =  total_passed_bf;
    const bool has_reasonable_limit = (limit_hint && limit_hint < max_rows_in_distinct_before_bloom_filter_passthrough);

    if ((try_init_bf) && is_pre_distinct && (!has_reasonable_limit) && old_set_size > max_rows_in_distinct_before_bloom_filter_passthrough)
    {
        bloom_filter = std::make_unique<BloomFilter>(BloomFilterParameters(6553600, 1, 0));
        try_init_bf = false;
        use_bf = true;
    }

    if (data.empty())
    {
        auto type = SetVariants::chooseMethod(column_ptrs, key_sizes);
        if ((!is_pre_distinct) && type == SetVariants::Type::hashed)
        {
            data.init(SetVariants::Type::hashed_two_level);
        } else
        {
            data.init(type);
        }

    }

    ColumnPtr filter;
    bool lazy_filter = false;
    ColumnPtr bucket;
    bool fallback_plain_distinct = false;

    if (is_pre_distinct)
    {
        bucket = ColumnVector<UInt8>::create(num_rows);
        filter = ColumnVector<UInt8>::create(num_rows, true);
    } else
    {
        const auto distinct_info = chunk.getChunkInfos().extract<DistinctChunkInfo>();
        if (distinct_info)
        {
            lazy_filter = distinct_info->lazy_filter;

            if (lazy_filter && columns.size() >= distinct_info->filter_column_pos)
                filter = checkAndGetColumn<ColumnVector<UInt8>>(columns[distinct_info->filter_column_pos].get())->getPtr();
            else
                filter = ColumnVector<UInt8>::create(num_rows, true);

            if (columns.size() >= distinct_info->filter_column_pos)
                columns.erase(columns.begin() + distinct_info->filter_column_pos);

            if (columns.size() > distinct_info->bucket_column_pos)
            {
                bucket = checkAndGetColumn<ColumnVector<UInt8>>(columns[distinct_info->bucket_column_pos].get())->getPtr();
                columns.erase(columns.begin() + distinct_info->bucket_column_pos);
            }
            else
                fallback_plain_distinct = true;

        } else
        {
            fallback_plain_distinct = true;
            filter = ColumnVector<UInt8>::create(num_rows, true);
        }
    }

    auto check_only = data.getTotalRowCount() > 5000000;
    use_bf = check_only ? false : use_bf;
    PaddedPODArray<UInt8> * bucket_array = nullptr;
    PaddedPODArray<UInt8> * filter_array;

    if (!fallback_plain_distinct)
    {
        auto * column_vector = assert_cast<ColumnVector<UInt8> *>(const_cast<IColumn *>(bucket.get()));
        bucket_array = &column_vector->getData();
    }
    filter_array = &assert_cast<ColumnVector<UInt8> *>(const_cast<IColumn *>(filter.get()))->getData();

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
            case SetVariants::Type::NAME: \
                if constexpr (SetVariants::Type::NAME == SetVariants::Type::hashed || SetVariants::Type::NAME == SetVariants::Type::hashed_two_level) \
                { \
                    is_pre_distinct \
                    ? use_bf ? buildCombinedBucketFilter(*data.NAME, column_ptrs, *filter_array, num_rows, data, total_passed_bf, *bucket_array) : check_only ? checkSetBucketFilter(*data.NAME, column_ptrs, *filter_array, num_rows, data, total_passed_bf, *bucket_array): buildSetBucketFilter(*data.NAME, column_ptrs, *filter_array, num_rows, data, *bucket_array) \
                    : fallback_plain_distinct ? buildSetFinalFilter(*data.NAME, column_ptrs, *filter_array, num_rows, data) : buildSetParallelFinalFilter(*data.NAME, column_ptrs, *filter_array, num_rows, data, *bucket_array, *pool); \
                } else \
                { \
                    is_pre_distinct \
                    ? use_bf ? buildCombinedBucketFilter(*data.NAME, column_ptrs, *filter_array, num_rows, data, total_passed_bf, *bucket_array): buildSetFilter(*data.NAME, column_ptrs, *filter_array, num_rows, data) \
                    : lazy_filter ? buildSetFinalFilter(*data.NAME, column_ptrs, *filter_array, num_rows, data) : buildSetFilter(*data.NAME, column_ptrs, *filter_array, num_rows, data); \
                } \
                break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    /// Just go to the next chunk if there isn't any new record in the current one.
    size_t new_bf_size = total_passed_bf;
    size_t new_set_size = data.getTotalRowCount();

    new_passes = ((new_set_size - old_set_size) + (new_bf_size - old_bf_size));

    if ((!new_passes) && is_pre_distinct)
        return;

    if (!set_size_limits.check(new_set_size, data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        return;

    auto try_filter_rows = (!is_pre_distinct) || (data.type != SetVariants::Type::hashed && (new_passes * 10) < num_rows);

    if (is_pre_distinct)
    {
        auto infos = std::make_shared<DistinctChunkInfo>();
        columns.push_back(bucket);
        infos->bucket_column_pos = columns.size() - 1;
        infos->lazy_filter = !try_filter_rows;
        columns.push_back(filter);
        infos->filter_column_pos = columns.size() - 1;
        chunk.getChunkInfos().add(infos);
    }

    if (try_filter_rows)
    {
        for (auto & column : columns)
            column = column->filter(*filter_array, new_passes);
    }


    use_bf = use_bf && (new_passes * 10) > num_rows ? true: false;

    chunk.setColumns(std::move(columns), try_filter_rows ? new_passes : num_rows);


    /// Stop reading if we already reach the limit
    if (limit_hint && (new_set_size >= limit_hint || new_bf_size >= limit_hint))
    {
        stopReading();
        return;
    }
}

}
