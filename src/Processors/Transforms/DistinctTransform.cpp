#include <vector>
#include <Processors/Transforms/DistinctTransform.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/NullableUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/HashTable/TwoLevelHashTable.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
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

namespace ProfileEvents
{
    extern const Event DistinctTransformsAbandonedDeduplication;
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

/// A `hashed_two_level` set is only worth its overhead once it holds at least this many keys,
/// because only then can a chunk be deduplicated by `buildSetParallelFilter` over its buckets.
static constexpr size_t PARALLEL_DISTINCT_THRESHOLD = 1000000;

namespace
{

/// Mark rows whose `LowCardinality` index is the dictionary's NULL entry with 0 in `keep`, allocating
/// the filter lazily on the first such row.
void markLowCardinalityNullRows(const ColumnLowCardinality & column, IColumn::Filter & keep, size_t num_rows)
{
    const size_t null_index = column.getDictionary().getNullValueIndex();
    const IColumn & indexes_column = *column.getIndexesPtr();

    auto process = [&](const auto & indexes)
    {
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (static_cast<size_t>(indexes[row]) == null_index)
            {
                if (keep.empty())
                    keep.assign(num_rows, static_cast<UInt8>(1));
                keep[row] = 0;
            }
        }
    };

    switch (column.getSizeOfIndexType())
    {
        case sizeof(UInt8): process(assert_cast<const ColumnUInt8 &>(indexes_column).getData()); break;
        case sizeof(UInt16): process(assert_cast<const ColumnUInt16 &>(indexes_column).getData()); break;
        case sizeof(UInt32): process(assert_cast<const ColumnUInt32 &>(indexes_column).getData()); break;
        case sizeof(UInt64): process(assert_cast<const ColumnUInt64 &>(indexes_column).getData()); break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for LowCardinality column in DistinctTransform");
    }
}

}

void LCOptimizationController::update(size_t num_rows, size_t new_indices_in_chunk)
{
    if (state != State::Observing)
        return;

    ++chunks_observed;
    rows_observed += num_rows;
    new_indices_observed += new_indices_in_chunk;

    if (chunks_observed >= OBSERVATION_CHUNK_COUNT)
    {
        double new_index_rate = static_cast<double>(new_indices_observed) / static_cast<double>(rows_observed);

        /// Disable when the mask is almost a no-op: nearly every row introduces
        /// a new dictionary index, so the bitmap bookkeeping is pure overhead.
        if (new_index_rate >= NEW_INDEX_RATE_THRESHOLD)
            state = State::Disabled;
        else
            state = State::Enabled;
    }
}

void DeduplicationAbandonController::update(size_t num_rows, size_t num_unique_rows, size_t set_bytes)
{
    if (abandoned)
        return;

    ++chunks_observed;
    rows_observed += num_rows;
    unique_rows_observed += num_unique_rows;

    if (chunks_observed < OBSERVATION_CHUNK_COUNT && set_bytes < MAX_OBSERVATION_SET_BYTES)
        return;

    double unique_rate = static_cast<double>(unique_rows_observed) / static_cast<double>(rows_observed);
    abandoned = unique_rate >= UNIQUE_RATE_THRESHOLD;
}

DistinctTransform::DistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    bool allow_abandoning_,
    bool skip_null_keys_,
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
    , skip_null_keys(skip_null_keys_)
{
    if (allow_abandoning_)
        abandon_controller.emplace();

    if (is_pre_distinct_)
    {
        pool = nullptr;
        /// With a LIMIT below the activation threshold reading stops before the set can ever
        /// grow large enough for the bloom filter to be initialized, so don't even try.
        /// Bloom-filter-only keys cannot be counted exactly without retaining their full keys.
        /// Keep the regular set path when an exact row limit is configured, so
        /// `max_rows_in_distinct` retains its usual exact-cardinality contract.
        try_init_bf = !(
            (limit_hint_ && limit_hint_ < set_limit_for_enabling_bloom_filter_)
            || set_limit_for_enabling_bloom_filter_ == 0
            || set_size_limits.max_rows != 0);
    }
    else
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

    const size_t num_columns = columns_.empty() ? header_->columns() : columns_.size();
    key_columns_pos.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto pos = columns_.empty() ? i : header_->getPositionByName(columns_[i]);
        const auto & col = header_->getByPosition(pos).column;
        if (col && !isColumnConst(*col))
            key_columns_pos.emplace_back(pos);
        else if (skip_null_keys && col && col->isNullAt(0))
            const_null_key = true;
    }
}

void DistinctTransform::checkBloomFilterWorthiness()
{
    const auto & raw_filter_words = bloom_filter->getFilter();
    const size_t total_bits = raw_filter_words.size() * sizeof(raw_filter_words[0]) * 8;
    size_t set_bits = 0;
    for (auto word : raw_filter_words)
        set_bits += std::popcount(word);
    /// If too many bits are set then it is likely that the filter will not filter out much
    if (static_cast<Float64>(set_bits) > max_ratio_of_set_bits_in_bloom_filter * static_cast<Float64>(total_bits))
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
    size_t & passed_bf) const
{
    typename Method::State state(columns, key_sizes, nullptr);
    typename std::remove_reference_t<decltype(method.data)>::LookupResult it;

    for (size_t i = 0; i < rows; ++i)
    {
        auto key_holder = state.getKeyHolder(i, variants.string_pool);
        auto hash = method.data.hash(keyHolderGetKey(key_holder));

        auto hash1 = hash;
        auto hash2 = intHash32(hash);

        auto has_element = bloom_filter->findRawHash(hash1) && bloom_filter->findRawHash(hash2);

        if (has_element)
        {
            bool inserted = false;
            method.data.emplace(key_holder, it, inserted, hash);
            /// Emit the record if there is no such key in the current set yet.
            /// Skip it otherwise.
            filter[i] = inserted;
        }
        else
        {
            bloom_filter->addRawHash(hash1);
            bloom_filter->addRawHash(hash2);
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

std::pair<IColumn::Filter, size_t> DistinctTransform::buildLowCardinalityMask(const ColumnLowCardinality & column, size_t num_rows)
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
        return {{}, 0}; /// empty mask == no candidates

    const auto seen_count_before = state.seen_count;
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

    return {std::move(mask), state.seen_count - seen_count_before};
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
                    bool inserted = false;
                    method.data.emplace(keys[i], it, inserted, hashes[i]);
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

void DistinctTransform::maybeAbandonDeduplication(size_t num_rows, size_t num_unique_rows)
{
    if (!abandon_controller)
        return;

    abandon_controller->update(num_rows, num_unique_rows, data->getTotalByteCount());
    if (abandon_controller->isAbandoned())
    {
        data.reset();
        bloom_filter.reset();
        use_bf = false;
        try_init_bf = false;
        lc_dict_states.clear();
        ProfileEvents::increment(ProfileEvents::DistinctTransformsAbandonedDeduplication);
    }
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    if (abandon_controller && abandon_controller->isAbandoned())
        return;

    if (const_null_key)
    {
        chunk.setColumns(chunk.cloneEmptyColumns(), 0);
        stopReading();
        return;
    }

    /// Convert to full column, because SetVariant for sparse column is not implemented.
    removeSpecialColumnRepresentations(chunk);
    convertToFullIfConst(chunk);

    auto num_rows = chunk.getNumRows();
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

    /// The consumer skips rows with a NULL in any key component (a set fill with
    /// `transform_null_in = 0` strips `LowCardinality` and then drops such rows), so they carry no
    /// value downstream: drop them before deduplication and before the abandon accounting. Plain
    /// `Nullable` keys are then hashed by their nested columns, the same way the set fill hashes them.
    ColumnPtr null_map_holder;
    if (skip_null_keys)
    {
        ConstNullMapPtr null_map = nullptr;
        null_map_holder = extractNestedColumnsAndNullMap(column_ptrs, null_map);

        IColumn::Filter keep;
        if (null_map && !memoryIsZero(null_map->data(), 0, num_rows))
        {
            keep.resize(num_rows);
            for (size_t i = 0; i < num_rows; ++i)
                keep[i] = !(*null_map)[i];
        }

        for (const auto * column : column_ptrs)
            if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(column);
                low_cardinality && low_cardinality->nestedIsNullable())
                markLowCardinalityNullRows(*low_cardinality, keep, num_rows);

        if (!keep.empty())
        {
            const auto num_kept = countBytesInFilter(keep);
            for (auto & column : columns)
                column = column->filter(keep, num_kept);
            num_rows = num_kept;

            if (num_rows == 0)
            {
                chunk.setColumns(std::move(columns), 0);
                return;
            }

            column_ptrs.clear();
            for (auto pos : key_columns_pos)
                column_ptrs.emplace_back(columns[pos].get());
            null_map_holder = extractNestedColumnsAndNullMap(column_ptrs, null_map);
        }
    }

    std::optional<IColumn::Filter> lc_mask;

    if (lc_optimization_controller.isEnabled() && key_columns_pos.size() == 1)
    {
        if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(column_ptrs[0]))
        {
            auto [mask, new_indices_count] = buildLowCardinalityMask(*lc, num_rows);
            lc_optimization_controller.update(num_rows, new_indices_count);
            lc_mask.emplace(std::move(mask));

            /// Empty mask -> no candidate rows in this chunk, emit nothing. The chunk is fully
            /// duplicate, which is the strongest evidence in favor of keeping the deduplication, so
            /// the abandon accounting must see it.
            if (lc_mask->empty())
            {
                maybeAbandonDeduplication(num_rows, 0);
                return;
            }
        }
    }

    if (data->empty())
    {
        auto type = SetVariants::chooseMethod(column_ptrs, key_sizes);

        /// A two-level table is usually slower than a single-level one on its own; it only pays off
        /// because it can be probed in parallel bucket by bucket. Without a thread pool (e.g.
        /// `max_threads = 1`) that never happens, so don't switch to it - the cost could never be
        /// recovered. The same holds for a small `LIMIT`: reading stops long before the set grows
        /// past `PARALLEL_DISTINCT_THRESHOLD`, which is what enables the parallel path.
        if (!is_pre_distinct && pool && !(limit_hint && limit_hint < PARALLEL_DISTINCT_THRESHOLD)
            && type == SetVariants::Type::hashed)
            data->init(SetVariants::Type::hashed_two_level);
        else
            data->init(type);
    }

    const auto old_set_size = data->getTotalRowCount();
    const auto old_bf_size = total_passed_bf;
    const auto old_check_only_size = total_passed_check_only;

    if (try_init_bf && old_set_size > set_limit_for_enabling_bloom_filter)
    {
        bloom_filter = std::make_unique<BloomFilter>(BloomFilterParameters(bloom_filter_bytes, 1, 0));
        bf_worthless_total_set_bits = static_cast<UInt64>(static_cast<Float64>(bloom_filter_bytes * 8) * max_ratio_of_set_bits_in_bloom_filter);
        try_init_bf = false;
        use_bf = true;
    }

    if (use_bf && (total_passed_bf - bf_worthless_last_bf_pass) * 2 > (bf_worthless_total_set_bits - bf_worthless_last_set_bits))
        checkBloomFilterWorthiness();

    /// As with the bloom-filter path, `check_only` does not retain every new key. Do not use it
    /// when an exact row limit is configured.
    const bool check_only = is_pre_distinct
        && set_limit_for_enabling_bloom_filter > 0
        && old_set_size > set_limit_for_enabling_bloom_filter * 2
        && set_size_limits.max_rows == 0;
    auto * lc_mask_ptr = lc_mask ? &*lc_mask : nullptr;

    IColumn::Filter filter(num_rows);

    switch (data->type)
    {
        case SetVariants::Type::EMPTY:
            break;

#define M(NAME) \
        case SetVariants::Type::NAME: \
        { \
            auto & set = *data->NAME; \
            const auto build = [&] \
            { \
                buildSetFilter(set, column_ptrs, filter, num_rows, *data, lc_mask_ptr); \
            }; \
            \
            if constexpr (SetVariants::Type::NAME == SetVariants::Type::hashed_two_level) \
            { \
                if (old_set_size > PARALLEL_DISTINCT_THRESHOLD && pool && num_rows > 10000) \
                    buildSetParallelFilter(set, column_ptrs, filter, num_rows, *data, *pool); \
                else \
                    build(); \
            } \
            else if (!is_pre_distinct) \
                build(); \
            else if (check_only) \
                checkSetFilter(set, column_ptrs, filter, num_rows, *data, total_passed_check_only); \
            else if (use_bf) \
                buildCombinedFilter(set, column_ptrs, filter, num_rows, *data, total_passed_bf); \
            else \
                build(); \
            \
            break; \
        }

        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    const size_t new_bf_size = total_passed_bf;
    const size_t new_set_size = data->getTotalRowCount();

    /// Rows forwarded by this chunk: new keys in the hash set, new keys absorbed by the bloom
    /// filter and rows forwarded unrecorded by the `check_only` mode.
    const size_t rows_passed
        = (new_set_size - old_set_size) + (new_bf_size - old_bf_size) + (total_passed_check_only - old_check_only_size);

    maybeAbandonDeduplication(num_rows, rows_passed);

    /// Just go to the next chunk if there isn't any new record in the current one.
    if (rows_passed == 0)
        return;

    /// The bloom filter allocation is resident state and must be accounted for by
    /// `max_bytes_in_distinct`. The optimization is disabled when `max_rows_in_distinct` is set,
    /// because the Bloom filter alone cannot provide an exact distinct-key count.
    size_t new_set_bytes = data ? data->getTotalByteCount() : 0;
    if (bloom_filter)
        new_set_bytes += bloom_filter->getFilterSizeBytes();

    /// In case of overflow_mode = 'break' `check` returns false instead of throwing.
    /// Stop reading, but still emit the new rows from the current chunk (their keys are
    /// already in the set): 'break' means return a partial result as if the source data
    /// ran out, not discard it.
    if (!set_size_limits.check(new_set_size, new_set_bytes, "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        stopReading();

    if (rows_passed == num_rows)
    {
        /// Every row is a new distinct value: keep the chunk unchanged, without copying it.
        chunk.setColumns(std::move(columns), num_rows);
    }
    else
    {
        for (auto & column : columns)
            column = column->filter(filter, rows_passed);

        chunk.setColumns(std::move(columns), rows_passed);
    }

    /// The bloom filter pays off only on high-cardinality data, where most rows are new and can be
    /// absorbed by the filter instead of the hash set. When the pass ratio drops below the threshold
    /// the data is duplicate-heavy: most rows end up in the hash set anyway, so the extra bloom
    /// filter lookup is pure overhead - disable it (permanently) and fall back to the plain set.
    use_bf = use_bf && (static_cast<Float64>(rows_passed) > (pass_ratio_threshold_for_disabling_bloom_filter * static_cast<Float64>(num_rows)));

    /// Stop reading if we already reach the limit.
    /// Only keys that were actually recorded (in the hash set or in the bloom filter) may be counted
    /// here: each of them is emitted exactly once, so reaching `limit_hint` of them means this stream
    /// alone can satisfy the `LIMIT`. Rows forwarded by the `check_only` mode are deliberately not
    /// counted - they are not recorded anywhere, so one key repeated `limit_hint` times would stop the
    /// stream before it emitted `limit_hint` distinct values, losing later distinct values from it.
    if (limit_hint && (new_set_size >= limit_hint || new_bf_size >= limit_hint))
        stopReading();
}

}
