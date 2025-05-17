#include <Processors/Transforms/DistinctTransform.h>
#include <Common/HashTable/TwoLevelHashTable.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>

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
}

DistinctTransform::DistinctTransform(
    const Block & header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    bool is_pre_distinct_,
    size_t max_threads_)
    : ISimpleTransform(header_, header_, true)
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
        try_init_bf = !(limit_hint_ && limit_hint_ < 1000000);
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
    typename std::remove_reference_t<decltype(method.data)>::LookupResult it;

    for (size_t i = 0; i < rows; ++i)
    {

        auto key_holder = state.getKeyHolder(i, variants.string_pool);
        auto hash = method.data.hash(keyHolderGetKey(key_holder));

        auto hash1 = hash;
        auto hash2 = intHash32(hash);

        auto has_element = bloom_filter->findRawHash(hash1/*, SEED_GEN_A*/) && bloom_filter->findRawHash(hash2/*, SEED_GEN_A*/);

        if (has_element)
        {
            bool inserted;
            method.data.emplace(key_holder, it, inserted, hash);
            //auto emplace_result = state.emplaceImpl(key_holder, method.data);
            /// Emit the record if there is no such key in the current set yet.
            /// Skip it otherwise.
            filter[i] = inserted;
        } else
        {
            bloom_filter->addRawHash(hash1/*, SEED_GEN_A*/);
            bloom_filter->addRawHash(hash2/*, SEED_GEN_A*/);
            passed_bf++;
            filter[i] = true;
        }
    }
}


template <typename Method>
void DistinctTransform::buildSetFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumn::Filter & filter,
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
void DistinctTransform::checkSetFilter(
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
        auto find_result = state.findKey(method.data, i, variants.string_pool);
        /// Emit the record if there is no such key in the current set yet.
        /// Skip it otherwise.
        filter[i] = !find_result.isFound();
        passed_bf+= !find_result.isFound();
    }
}

template <typename Method>
void DistinctTransform::buildSetParallelFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumnFilter & filter,
    const size_t rows,
    SetVariants & variants,
    ThreadPool & thread_pool) const
{
    typename Method::State state(columns, key_sizes, nullptr);
    auto thread_group = CurrentThread::getGroup();
    using KeyHolder = decltype(state.getKeyHolder(std::declval<size_t>(), std::declval<Arena &>()));

    const size_t num_coarse_buckets = thread_pool.getMaxThreads();

    /// 1. Allocate index buffer and per-row bucket ids
    PODArray<size_t> all_indices(rows);
    PODArray<UInt8> coarse_bucket_ids(rows); /// UInt8 is sufficient for ≤ 256 buckets
    PODArray<size_t> bucket_sizes(num_coarse_buckets, 0);
    PODArray<KeyHolder> keys(rows);
    PODArray<size_t> hashes(rows);

    /// 2. First pass: hash each row once, assign to coarse bucket, count per-bucket size
    for (size_t i = 0; i < rows; ++i)
    {
        auto key_holder = state.getKeyHolder(i, variants.string_pool);
        auto hash = method.data.hash(keyHolderGetKey(key_holder));
        auto fine_bucket = method.data.getBucketFromHash(hash);        // 0..255

        size_t coarse_bucket = fine_bucket % num_coarse_buckets;
        coarse_bucket_ids[i] = static_cast<UInt8>(coarse_bucket);
        keys[i] = key_holder;
        hashes[i] = hash;
        ++bucket_sizes[coarse_bucket];
    }

    /// 3. Compute start offset for each bucket
    std::vector<size_t> bucket_offsets(num_coarse_buckets + 1, 0);
    for (size_t i = 1; i <= num_coarse_buckets; ++i)
        bucket_offsets[i] = bucket_offsets[i - 1] + bucket_sizes[i - 1];

    /// 4. Fill in the array, writing per-bucket indices at known offset
    std::vector<size_t> write_positions = bucket_offsets;
    for (size_t i = 0; i < rows; ++i)
    {
        size_t b = coarse_bucket_ids[i];
        all_indices[write_positions[b]++] = i;
    }

    /// 5. Parallel processing by bucket
    auto next_bucket = std::make_shared<std::atomic<size_t>>(0);
    for (size_t thread_id = 0; thread_id < num_coarse_buckets; ++thread_id)
    {
        thread_pool.scheduleOrThrowOnError(
            [next_bucket, &bucket_offsets, &all_indices, &hashes, &keys, &method, &filter, thread_group]()
        {
            ThreadGroupSwitcher switcher(thread_group, "DistinctFinal", true);
            typename std::remove_reference_t<decltype(method.data)>::LookupResult it;

            while (true)
            {
                size_t bucket = next_bucket->fetch_add(1);
                if (bucket >= bucket_offsets.size() - 1)
                    return;

                size_t begin = bucket_offsets[bucket];
                size_t end = bucket_offsets[bucket + 1];

                if (begin == end)
                    continue;

                for (size_t j = begin; j < end; ++j)
                {
                    size_t i = all_indices[j];
                    bool inserted;
                    method.data.emplace(keys[i], it, inserted, hashes[i]);
                    //auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
                    filter[i] = inserted;
                }
            }
        });
    }
    thread_pool.wait();
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
    const auto old_bf_size = total_passed_bf;

    if (try_init_bf && old_set_size > 1000000)
    {
        bloom_filter = std::make_unique<BloomFilter>(BloomFilterParameters(6553600, 1, 0));
        try_init_bf = false;
        use_bf = true;
    }

    if (data.empty())
    {
        auto type = SetVariants::chooseMethod(column_ptrs, key_sizes);

        if (!is_pre_distinct && !(limit_hint && limit_hint < 1000000) && type == SetVariants::Type::hashed)
            data.init(SetVariants::Type::hashed_two_level);
        else
            data.init(type);
    }

    auto check_only = data.getTotalRowCount() > 2000000;

    IColumn::Filter filter(num_rows);

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
            case SetVariants::Type::NAME: \
                if constexpr (SetVariants::Type::NAME == SetVariants::Type::hashed_two_level) \
                { \
                    data.getTotalRowCount() > 1000000 ? buildSetParallelFilter(*data.NAME, column_ptrs, filter, num_rows, data, *pool): buildSetFilter(*data.NAME, column_ptrs, filter, num_rows, data); \
                } else \
                { \
                    is_pre_distinct \
                    ? check_only ? checkSetFilter(*data.NAME, column_ptrs, filter, num_rows, data, total_passed_bf): use_bf ? buildCombinedFilter(*data.NAME, column_ptrs, filter, num_rows, data, total_passed_bf): buildSetFilter(*data.NAME, column_ptrs, filter, num_rows, data) \
                    : buildSetFilter(*data.NAME, column_ptrs, filter, num_rows, data); \
                } \
                break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    /// Just go to the next chunk if there isn't any new record in the current one.
    size_t new_bf_size = total_passed_bf;
    size_t new_set_size = data.getTotalRowCount();
    new_passes = ((new_set_size - old_set_size) + (new_bf_size - old_bf_size));

    if (!new_passes)
        return;

    if (!set_size_limits.check(new_set_size, data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        return;

    for (auto & column : columns)
        column = column->filter(filter, new_passes);

    use_bf = use_bf && (new_passes * 2) > num_rows ? true: false;

    chunk.setColumns(std::move(columns), new_passes);

    /// Stop reading if we already reach the limit
    if (limit_hint && (new_set_size >= limit_hint || new_bf_size >= limit_hint))
    {
        stopReading();
        return;
    }
}

}
