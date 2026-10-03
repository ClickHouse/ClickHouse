#include <Processors/Transforms/DistinctTransform.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <vector>

#include <Columns/ColumnsCommon.h>
#include <Common/BitHelpers.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/HashTable/TwoLevelHashTable.h>
#include <base/arithmeticOverflow.h>
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
    extern const Event DistinctTransformsSwitchedToPassThrough;
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
}

/// A `hashed_two_level` set is only worth its overhead once it holds at least this many keys,
/// because only then can a chunk be deduplicated by `buildSetParallelFilter` over its buckets.
static constexpr size_t PARALLEL_DISTINCT_THRESHOLD = 1000000;

bool DeduplicationAbandonController::update(size_t num_rows, size_t num_unique_rows, size_t set_bytes)
{
    ++chunks_observed;
    rows_observed += num_rows;
    unique_rows_observed += num_unique_rows;

    if (chunks_observed < OBSERVATION_CHUNK_COUNT && set_bytes < MAX_OBSERVATION_SET_BYTES)
        return false;

    double unique_rate = static_cast<double>(unique_rows_observed) / static_cast<double>(rows_observed);
    return unique_rate >= UNIQUE_RATE_THRESHOLD;
}

DistinctTransform::DistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    bool allow_abandoning_,
    bool skip_null_keys_,
    const UInt64 max_bytes_before_pass_through_,
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
    , max_bytes_before_pass_through(max_bytes_before_pass_through_)
{
    if (allow_abandoning_)
        abandon_controller.emplace();

    /// The bloom-filter and parallel paths do not implement the `NULL`-key skipping of `DistinctSetFilter`.
    if (!skip_null_keys_)
    {
        if (is_pre_distinct_)
        {
            /// With a LIMIT below the activation threshold reading stops before the set can ever
            /// grow large enough for the bloom filter to be initialized, so don't even try.
            /// Bloom-filter-only keys cannot be counted exactly without retaining their full keys.
            /// Keep the regular set path when an exact row limit is configured, so
            /// `max_rows_in_distinct` retains its usual exact-cardinality contract.
            try_init_bf = !(
                (limit_hint_ && limit_hint_ < set_limit_for_enabling_bloom_filter_)
                || set_limit_for_enabling_bloom_filter_ == 0
                || set_size_limits.max_rows != 0);
            use_own_set = try_init_bf;
        }
        else if (max_threads_ > 1 && !(limit_hint_ && limit_hint_ < PARALLEL_DISTINCT_THRESHOLD))
        {
            pool = std::make_unique<ThreadPool>(
                CurrentMetrics::DistinctThreads,
                CurrentMetrics::DistinctThreadsActive,
                CurrentMetrics::DistinctThreadsScheduled,
                max_threads_);
            use_own_set = true;
        }
    }

    if (use_own_set)
    {
        key_columns_pos = calculateDistinctKeyColumnsPositions(*header_, columns_);
        non_constant_columns_pos = calculateDistinctKeyColumnsPositions(*header_, {});
        data = std::make_unique<SetVariants>();
        lc_filter = std::make_unique<DistinctLowCardinalityFilter>();
    }
    else
    {
        distinct_set.emplace(*header_, columns_, set_size_limits_, skip_null_keys_);
    }
}

DistinctTransform::~DistinctTransform() = default;

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
void DistinctTransform::checkSetFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumnFilter & filter,
    const size_t rows,
    SetVariants & variants,
    size_t & passed_bf) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
    {
        auto find_result = state.findKey(method.data, i, variants.string_pool);
        /// Emit the record if there is no such key in the current set yet.
        /// Skip it otherwise.
        filter[i] = !find_result.isFound();
        passed_bf += !find_result.isFound();
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
    {
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
                    auto fine_bucket = method.data.getBucketFromHash(hash); /// 0..255

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
    {
        auto next_bucket = std::make_shared<std::atomic<size_t>>(0);

        auto thread_func = [next_bucket, &bucket_offsets, &all_indices, &hashes, &keys, &method, &filter]()
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
    runner.waitForAllToFinishAndRethrowFirstError();
}

size_t DistinctTransform::getOwnSetByteCount() const
{
    size_t bytes = data ? data->getTotalByteCount() : 0;
    if (lc_filter)
        bytes += lc_filter->getTotalByteCount();
    /// The bloom filter allocation is resident state and must be accounted for by `max_bytes_in_distinct`.
    if (bloom_filter)
        bytes += bloom_filter->getFilterSizeBytes();
    return bytes;
}

void DistinctTransform::releaseOwnSet()
{
    data.reset();
    lc_filter.reset();
    bloom_filter.reset();
    use_bf = false;
    try_init_bf = false;
    own_set_released = true;
}

void DistinctTransform::transformWithOwnSet(Chunk & chunk)
{
    /// Releasing the set permanently switches subsequent chunks to pass-through.
    if (own_set_released)
        return;

    /// Special case, - only const columns, return single row
    if (unlikely(key_columns_pos.empty()))
    {
        removeSpecialColumnRepresentations(chunk);
        convertToFullIfConst(chunk);

        auto columns = chunk.detachColumns();
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
        stopReading();
        return;
    }

    /// Convert to full columns, because `SetVariants` for sparse columns is not implemented. As in
    /// `DistinctSetFilter`, columns that are constant in the header stay constant: expanding a wide
    /// constant payload for every block could exceed the memory limit.
    materializeChunk(chunk, non_constant_columns_pos);

    const auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(key_columns_pos.size());
    for (auto pos : key_columns_pos)
        column_ptrs.emplace_back(columns[pos].get());

    if (data->empty())
    {
        auto type = SetVariants::chooseMethod(column_ptrs, key_sizes);

        /// A two-level table is usually slower than a single-level one on its own; it only pays off
        /// because it can be probed in parallel bucket by bucket. Without a thread pool (e.g.
        /// `max_threads = 1`) that never happens, so don't switch to it - the cost could never be
        /// recovered. The same holds for a small `LIMIT`: reading stops long before the set grows
        /// past `PARALLEL_DISTINCT_THRESHOLD`, which is what enables the parallel path.
        if (!is_pre_distinct && pool && type == SetVariants::Type::hashed)
            data->init(SetVariants::Type::hashed_two_level);
        else
            data->init(type);
    }

    /// Preliminary hashing shares the query's remaining spill-threshold budget with the final transform
    /// and other operators. As in the `DistinctSetFilter` path, the set is released before inserting
    /// when the query memory already exceeds the threshold, or when the projected growth of the set
    /// (assuming every row is new), the bloom filter about to be allocated and the filtering workspace
    /// do not fit into the remaining budget.
    if (max_bytes_before_pass_through)
    {
        const UInt64 query_memory_usage = std::max<Int64>(0, getCurrentQueryMemoryUsage());
        const UInt64 available_memory = max_bytes_before_pass_through - std::min(max_bytes_before_pass_through, query_memory_usage);

        size_t filtering_memory = roundUpToPowerOfTwoOrZero(
            num_rows * sizeof(IColumn::Filter::value_type) + IColumn::Filter::pad_left + IColumn::Filter::pad_right) * 2;
        for (const auto & column : columns)
            filtering_memory += column->allocatedBytes();

        size_t growth_memory = data->estimateGrowthMemory(column_ptrs, 0, num_rows);
        if (key_columns_pos.size() == 1
            && common::addOverflow(growth_memory, lc_filter->estimateGrowthMemory(*column_ptrs[0]), growth_memory))
            growth_memory = std::numeric_limits<size_t>::max();
        if (try_init_bf && data->getTotalRowCount() > set_limit_for_enabling_bloom_filter
            && common::addOverflow(growth_memory, static_cast<size_t>(bloom_filter_bytes), growth_memory))
            growth_memory = std::numeric_limits<size_t>::max();

        if (filtering_memory > available_memory || growth_memory > available_memory - filtering_memory)
        {
            LOG_TRACE(getLogger("DistinctTransform"),
                "Switching preliminary DISTINCT to pass-through: {} "
                "(query memory: {}, spill threshold: {}, "
                "estimated peak extra memory for growth: {}, filtering workspace: {})",
                query_memory_usage > max_bytes_before_pass_through
                    ? "query memory exceeded the spill threshold"
                    : "projected allocations exceed the remaining spill-threshold budget",
                formatReadableSizeWithBinarySuffix(query_memory_usage),
                formatReadableSizeWithBinarySuffix(max_bytes_before_pass_through),
                formatReadableSizeWithBinarySuffix(growth_memory),
                formatReadableSizeWithBinarySuffix(filtering_memory));

            releaseOwnSet();
            ProfileEvents::increment(ProfileEvents::DistinctTransformsSwitchedToPassThrough);
            chunk.setColumns(std::move(columns), num_rows);
            return;
        }
    }

    std::optional<IColumn::Filter> lc_mask;
    if (key_columns_pos.size() == 1)
    {
        lc_mask = lc_filter->buildMaskIfApplicable(*column_ptrs[0], num_rows);

        /// Empty mask -> no candidate rows in this chunk, emit nothing. The chunk is fully
        /// duplicate, which is the strongest evidence in favor of keeping the deduplication, so
        /// the abandon accounting must see it.
        if (lc_mask && lc_mask->empty())
        {
            if (abandon_controller && abandon_controller->update(num_rows, 0, getOwnSetByteCount()))
            {
                releaseOwnSet();
                ProfileEvents::increment(ProfileEvents::DistinctTransformsAbandonedDeduplication);
            }
            return;
        }
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

    /// In case of overflow_mode = 'break' `check` returns false instead of throwing.
    /// Stop reading, but still emit the new rows from the current chunk (their keys are
    /// already in the set): 'break' means return a partial result as if the source data
    /// ran out, not discard it. The optimization is disabled when `max_rows_in_distinct` is set,
    /// because the Bloom filter alone cannot provide an exact distinct-key count.
    if (!set_size_limits.check(new_set_size, getOwnSetByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        own_set_limit_reached = true;

    if (rows_passed == 0)
    {
        /// No new record in the current chunk: the chunk stays empty (its columns were detached).
    }
    else if (rows_passed == num_rows)
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
    if (own_set_limit_reached || (limit_hint && (new_set_size >= limit_hint || new_bf_size >= limit_hint)))
    {
        stopReading();
        return;
    }

    if (abandon_controller && abandon_controller->update(num_rows, rows_passed, getOwnSetByteCount()))
    {
        LOG_TRACE(getLogger("DistinctTransform"),
            "Switching DISTINCT to pass-through: input is mostly unique (retained keys: {}, set memory: {})",
            new_set_size, formatReadableSizeWithBinarySuffix(getOwnSetByteCount()));

        /// The new rows of the current chunk are still emitted (the following chunks flow
        /// through unfiltered).
        releaseOwnSet();
        ProfileEvents::increment(ProfileEvents::DistinctTransformsAbandonedDeduplication);
    }
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    if (use_own_set)
    {
        transformWithOwnSet(chunk);
        return;
    }

    /// Releasing the filter permanently switches subsequent chunks to pass-through.
    if (!distinct_set)
        return;

    /// A constant `NULL` key component makes every key contain a `NULL`, so a consumer that skips `NULL`
    /// keys drops all rows; emit nothing and stop the input.
    if (distinct_set->hasConstNullKey())
    {
        chunk.setColumns(chunk.cloneEmptyColumns(), 0);
        stopReading();
        return;
    }

    /// Special case - only const columns, return single row.
    if (unlikely(!distinct_set->hasKeyColumns()))
    {
        removeSpecialColumnRepresentations(chunk);
        convertToFullIfConst(chunk);

        auto columns = chunk.detachColumns();
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
        stopReading();
        return;
    }

    if (max_bytes_before_pass_through)
    {
        distinct_set->prepareForInsert(chunk);

        /// Preliminary hashing shares the query's remaining spill-threshold budget with the final
        /// transform and other operators.
        const UInt64 query_memory_usage = std::max<Int64>(0, getCurrentQueryMemoryUsage());
        const UInt64 available_memory = max_bytes_before_pass_through - std::min(max_bytes_before_pass_through, query_memory_usage);

        const size_t filtering_memory = distinct_set->estimateFilteringMemory(chunk);
        const size_t growth_memory = distinct_set->estimateGrowthMemory(chunk);
        if (filtering_memory > available_memory || growth_memory > available_memory - filtering_memory)
        {
            LOG_TRACE(getLogger("DistinctTransform"),
                "Switching preliminary DISTINCT to pass-through: {} "
                "(query memory: {}, spill threshold: {}, "
                "estimated peak extra memory for growth: {}, filtering workspace: {})",
                query_memory_usage > max_bytes_before_pass_through
                    ? "query memory exceeded the spill threshold"
                    : "projected allocations exceed the remaining spill-threshold budget",
                formatReadableSizeWithBinarySuffix(query_memory_usage),
                formatReadableSizeWithBinarySuffix(max_bytes_before_pass_through),
                formatReadableSizeWithBinarySuffix(growth_memory),
                formatReadableSizeWithBinarySuffix(filtering_memory));

            distinct_set.reset();
            ProfileEvents::increment(ProfileEvents::DistinctTransformsSwitchedToPassThrough);
            return;
        }
    }

    const size_t num_rows = chunk.getNumRows();
    chunk = distinct_set->filter(std::move(chunk));

    /// Return the current chunk and stop before releasing the set if a size limit or the hint is reached.
    if (distinct_set->isLimitReached() || (limit_hint && distinct_set->getTotalRowCount() >= limit_hint))
    {
        stopReading();
        return;
    }

    if (abandon_controller)
    {
        /// The rate is measured against the rows the transform received: the rows dropped as `NULL` keys
        /// (in the `skip_null_keys` mode, inside the filter) count as removed by the deduplication, so a
        /// stream that mostly consists of `NULL` keys keeps the transform even when the non-`NULL` part is
        /// unique - dropping the `NULL` rows is exactly the reduction the consumer benefits from.
        if (abandon_controller->update(num_rows, chunk.getNumRows(), distinct_set->getTotalByteCount()))
        {
            LOG_TRACE(getLogger("DistinctTransform"),
                "Switching DISTINCT to pass-through: input is mostly unique (retained keys: {}, set memory: {})",
                distinct_set->getTotalRowCount(), formatReadableSizeWithBinarySuffix(distinct_set->getTotalByteCount()));

            /// The new rows of the current chunk are still emitted (the following chunks flow
            /// through unfiltered).
            distinct_set.reset();
            ProfileEvents::increment(ProfileEvents::DistinctTransformsAbandonedDeduplication);
            return;
        }
    }

    /// Preliminary hashing can release its set under memory pressure because a downstream step
    /// deduplicates the output exactly. This also gives up any remaining local limit hint. The set
    /// can be released even when the current chunk produces no new rows.
    if (max_bytes_before_pass_through)
    {
        const Int64 query_memory_usage = getCurrentQueryMemoryUsage();
        if (query_memory_usage > static_cast<Int64>(max_bytes_before_pass_through))
        {
            LOG_TRACE(getLogger("DistinctTransform"),
                "Switching preliminary DISTINCT to pass-through: query memory exceeded the spill threshold after insertion "
                "(query memory: {}, spill threshold: {})",
                formatReadableSizeWithBinarySuffix(query_memory_usage),
                formatReadableSizeWithBinarySuffix(max_bytes_before_pass_through));

            distinct_set.reset();
            ProfileEvents::increment(ProfileEvents::DistinctTransformsSwitchedToPassThrough);
            return;
        }
    }
}

}
