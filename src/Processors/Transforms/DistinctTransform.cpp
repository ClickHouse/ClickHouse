#include <vector>
#include <Processors/Transforms/DistinctTransform.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/HashTable/TwoLevelHashTable.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnsNumber.h>
#include <base/types.h>

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
    extern const Metric DistinctThreads;
    extern const Metric DistinctThreadsActive;
    extern const Metric DistinctThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

DistinctTransform::DistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    bool is_pre_distinct_,
    UInt64 set_limit_for_enabling_bloom_filter_,
    UInt64 bloom_filter_bytes_,
    Float64 pass_ratio_threshold_for_disabling_bloom_filter_,
    Float64 max_ratio_of_set_bits_in_bloom_filter_,
    size_t max_threads_)
    : ISimpleTransform(header_, header_, true)
    , limit_hint(limit_hint_)
    , is_pre_distinct(is_pre_distinct_)
    , set_limit_for_enabling_bloom_filter(set_limit_for_enabling_bloom_filter_)
    , bloom_filter_bytes(bloom_filter_bytes_)
    , pass_ratio_threshold_for_disabling_bloom_filter(pass_ratio_threshold_for_disabling_bloom_filter_)
    , max_ratio_of_set_bits_in_bloom_filter(max_ratio_of_set_bits_in_bloom_filter_)
    , set_size_limits(set_size_limits_)

{
    const size_t num_columns = columns_.empty() ? header_->columns() : columns_.size();
    key_columns_pos.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto pos = columns_.empty() ? i : header_->getPositionByName(columns_[i]);
        const auto & col = header_->getByPosition(pos).column;
        if (col && !isColumnConst(*col))
            key_columns_pos.emplace_back(pos);
    }

    if (is_pre_distinct_)
    {
        pool = nullptr;
        try_init_bf = !((limit_hint_ && limit_hint_ < 1000000) || set_limit_for_enabling_bloom_filter_ == 0);
    } else
    {
        try_init_bf = false;
        if (max_threads_ > 1)
            pool = std::make_unique<ThreadPool>(
                CurrentMetrics::DistinctThreads,
                CurrentMetrics::DistinctThreadsActive,
                CurrentMetrics::DistinctThreadsScheduled,
                max_threads_);
        else
            pool = nullptr;
    }

    setInputNotNeededAfterRead(true);
}

void DistinctTransform::checkBloomFilterWorthiness()
{
    const auto & raw_filter_words = bloom_filter->getFilter();
    const size_t total_bits = raw_filter_words.size() * sizeof(raw_filter_words[0]) * 8;
    size_t set_bits = 0;
    for (auto word : raw_filter_words)
        set_bits += std::popcount(word);
    /// If too many bits are set then it is likely that the filter will not filter out much
    if (set_bits > max_ratio_of_set_bits_in_bloom_filter * total_bits)
        use_bf = false;
    bf_worthless_last_set_bits = set_bits;
    bf_worthless_last_bf_pass = total_passed_bf;
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
    SetVariants & variants,
    const IColumn::Filter * mask) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    if (mask)
    {
        for (size_t i = 0; i < rows; ++i)
        {
            if (!(*mask)[i])
            {
                /// Already known duplicate row (by LC index), skip insertion
                filter[i] = 0;
                continue;
            }

            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
            filter[i] = emplace_result.isInserted();
        }
    }
    else
    {
        for (size_t i = 0; i < rows; ++i)
        {
            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

            /// Emit the record if there is no such key in the current set yet.
            /// Skip it otherwise.
            filter[i] = emplace_result.isInserted();
        }
    }
}

IColumn::Filter DistinctTransform::buildLowCardinalityMask(const ColumnLowCardinality & column, size_t num_rows)
{
    const auto & dictionary = column.getDictionary();
    const auto dict_size = dictionary.size();

    LCDictionaryKey dict_key;
    dict_key.hash = dictionary.getHash();
    dict_key.size = dict_size;

    auto & state = lc_dict_states[dict_key];

    /// The first time we see this dictionary, initialize the seen_indices array to keep track which entries
    /// in the dictionary have been seen.
    chassert(state.seen_count <= dict_size);
    if (state.seen_indices.size() != dict_size)
    {
        chassert(state.seen_indices.empty());
        chassert(state.seen_count == 0);
        state.seen_indices.resize_fill(dict_size);
    }

    /// If we've already seen all dictionary indices for this dictionary,
    /// then no row in this chunk (and also other chunks with the same dictionary) can produce a new distinct value.
    if (state.seen_count == dict_size)
        return {}; /// empty mask == no candidates

    auto & seen = state.seen_indices;

    const auto index_type_size = column.getSizeOfIndexType();
    const IColumn & indexes_column = *column.getIndexesPtr();

    IColumn::Filter mask;

    auto handle_index = [&](size_t idx, size_t row)
    {
        chassert(idx < dict_size);
        if (!seen[idx])
        {
            seen[idx] = 1;
            ++state.seen_count;

            if (mask.empty())
                mask.resize_fill(num_rows);

            mask[row] = 1; /// first time we see this dictionary index for this dictionary
        }
    };

    switch (index_type_size)
    {
        case sizeof(UInt8):
        {
            const auto & col = assert_cast<const ColumnUInt8 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        case sizeof(UInt16):
        {
            const auto & col = assert_cast<const ColumnUInt16 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        case sizeof(UInt32):
        {
            const auto & col = assert_cast<const ColumnUInt32 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        case sizeof(UInt64):
        {
            const auto & col = assert_cast<const ColumnUInt64 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for LowCardinality column in DistinctTransform");
    }

    return mask; /// if empty, then means no candidates in this chunk
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
    std::vector<std::atomic<size_t>> bucket_sizes(num_coarse_buckets);
    PODArray<KeyHolder> keys(rows);
    PODArray<size_t> hashes(rows);
    const size_t block = 1024;

    ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, ThreadName::DISTINCT_FINAL);
    try {
        auto next_row = std::make_shared<std::atomic<size_t>>(0);

        auto thread_func = [next_row, rows, &variants, &state, &coarse_bucket_ids, &bucket_sizes, num_coarse_buckets, &hashes, &keys, &method]()
        {
            while (true)
            {
                const size_t start = next_row->fetch_add(block, std::memory_order_relaxed);
                if (start >= rows)
                    return;

                const size_t end = std::min(start + block, rows);
                for (size_t i = start; i < end; ++i)
                {
                    auto key_holder = state.getKeyHolder(i, variants.string_pool);
                    auto hash = method.data.hash(keyHolderGetKey(key_holder));
                    auto fine_bucket = method.data.getBucketFromHash(hash);        // 0..255

                    size_t coarse_bucket = fine_bucket % num_coarse_buckets;
                    coarse_bucket_ids[i] = static_cast<UInt8>(coarse_bucket);
                    keys[i] = key_holder;
                    hashes[i] = hash;
                    bucket_sizes[coarse_bucket].fetch_add(1, std::memory_order_relaxed);
                }
            }
        };
        for (size_t i = 0; i < thread_pool.getMaxThreads(); ++i)
            runner.enqueueAndKeepTrack(thread_func, Priority{});
    }
    catch (...)
    {
        throw;
    }
    runner.waitForAllToFinishAndRethrowFirstError();

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
    try {
        auto next_bucket = std::make_shared<std::atomic<size_t>>(0);

        auto thread_func = [next_bucket, &bucket_offsets, &all_indices, &hashes, &keys, &method, &filter, thread_group]()
        {
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
        };

        for (size_t i = 0; i < thread_pool.getMaxThreads(); ++i)
            runner.enqueueAndKeepTrack(thread_func, Priority{});
    }
    catch (...)
    {
        throw;
    }
    runner.waitForAllToFinishAndRethrowFirstError();
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    /// Convert to full column, because SetVariant for sparse column is not implemented.
    removeSpecialColumnRepresentations(chunk);
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

    std::optional<IColumn::Filter> lc_mask;

    if (key_columns_pos.size() == 1)
    {
        if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(column_ptrs[0]))
        {
            lc_mask.emplace(buildLowCardinalityMask(*lc, num_rows));

            /// Empty mask -> no candidate rows in this chunk, emit nothing.
            if (lc_mask->empty())
                return;
        }
    }

    const auto old_set_size = data.getTotalRowCount();
    const auto old_bf_size = total_passed_bf;

    if (try_init_bf && old_set_size > set_limit_for_enabling_bloom_filter)
    {
        bloom_filter = std::make_unique<BloomFilter>(BloomFilterParameters(bloom_filter_bytes, 1, 0));
        bf_worthless_total_set_bits = static_cast<UInt64>((bloom_filter_bytes * 8) * max_ratio_of_set_bits_in_bloom_filter);
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

    if ((total_passed_bf - bf_worthless_last_bf_pass)*2 > (bf_worthless_total_set_bits - bf_worthless_last_set_bits))
        checkBloomFilterWorthiness();

    auto check_only = (old_set_size > set_limit_for_enabling_bloom_filter * 2) && set_limit_for_enabling_bloom_filter > 0;
    auto * lc_mask_ptr = lc_mask ? &*lc_mask : nullptr;
    constexpr size_t parallel_threshold = 1000000;
    auto passthrough = false;

    IColumn::Filter filter(num_rows);

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;

#define M(NAME) \
        case SetVariants::Type::NAME: \
        { \
            auto & set = *data.NAME; \
            const auto build = [&] \
            { \
                buildSetFilter(set, column_ptrs, filter, num_rows, data, lc_mask_ptr); \
            }; \
            \
            if constexpr (SetVariants::Type::NAME == SetVariants::Type::hashed_two_level) \
            { \
                if (old_set_size > parallel_threshold && pool && num_rows > 10000) \
                    buildSetParallelFilter(*data.NAME, column_ptrs, filter, num_rows, data, *pool); \
                else \
                    build(); \
            } \
            else if (!is_pre_distinct) \
                build(); \
            else if (check_only) \
                if (pool) \
                    checkSetFilter(set, column_ptrs, filter, num_rows, data, total_passed_bf); \
                else \
                    passthrough = true; \
            else if (use_bf) \
                buildCombinedFilter(set, column_ptrs, filter, num_rows, data, total_passed_bf); \
            else \
                build(); \
            \
            break; \
        }

        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    size_t new_bf_size = total_passed_bf;
    size_t new_set_size = data.getTotalRowCount();

    size_t rows_passed = ((new_set_size - old_set_size) + (new_bf_size - old_bf_size));

    if (passthrough)
        rows_passed = num_rows;

    /// Just go to the next chunk if there isn't any new record in the current one.
    if (!rows_passed)
        return;

    if (!set_size_limits.check(new_set_size, data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        return;

    if (rows_passed != num_rows)
        for (auto & column : columns)
            column = column->filter(filter, rows_passed);

    use_bf = use_bf && (rows_passed > (pass_ratio_threshold_for_disabling_bloom_filter * num_rows)) ? true: false;

    chunk.setColumns(std::move(columns), rows_passed);

    /// Stop reading if we already reach the limit
    if (limit_hint && (new_set_size >= limit_hint || new_bf_size >= limit_hint))
    {
        stopReading();
        return;
    }
}

}
