#include <Processors/Transforms/DistinctTransform.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/assert_cast.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>

#include <algorithm>
#include <atomic>
#include <vector>

namespace CurrentMetrics
{
    extern const Metric DistinctThreads;
    extern const Metric DistinctThreadsActive;
    extern const Metric DistinctThreadsScheduled;
}

namespace ProfileEvents
{
    extern const Event DistinctHashTablesInitializedAsTwoLevel;
    extern const Event DistinctTwoLevelParallelFilterBuilds;
    extern const Event DistinctTwoLevelSerialFilterBuilds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The two-level build is memory-bandwidth-bound (per-bucket allocate + zero-fill dominates), so
/// throughput saturates well before very high thread counts. Cap the build pool here regardless of
/// `max_threads` to avoid spawning threads that only add scheduling and allocator contention.
constexpr size_t MAX_TWO_LEVEL_BUILD_THREADS = 16;

/// Run `body(worker_idx)` once per worker with deterministic worker_idx, no block stealing.
/// Use when each worker owns a fixed slice of the input that must match across phases.
template <typename Body>
void parallelPerWorker(
    ThreadPool & pool,
    ThreadName name,
    size_t num_workers,
    Body && body)
{
    ThreadPoolCallbackRunnerLocal<void> runner(pool, name);
    for (size_t w = 0; w < num_workers; ++w)
        runner.enqueueAndKeepTrack([&, w] { body(w); }, Priority{});
    runner.waitForAllToFinishAndRethrowFirstError();
}

/// Run `body(item_idx)` for every item in [0, total) with `num_workers` tasks pulling work
/// via an atomic counter. Used to dispatch one task per bucket.
template <typename Body>
void parallelDispatch(
    ThreadPool & pool,
    ThreadName name,
    size_t num_workers,
    size_t total,
    Body && body)
{
    ThreadPoolCallbackRunnerLocal<void> runner(pool, name);
    std::atomic<size_t> next{0};
    for (size_t w = 0; w < num_workers; ++w)
    {
        runner.enqueueAndKeepTrack([&]
        {
            while (true)
            {
                const size_t i = next.fetch_add(1, std::memory_order_relaxed);
                if (i >= total)
                    return;
                body(i);
            }
        }, Priority{});
    }
    runner.waitForAllToFinishAndRethrowFirstError();
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

DistinctTransform::DistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    bool is_pre_distinct_,
    size_t max_threads_,
    UInt64 two_level_threshold_,
    UInt64 two_level_threshold_bytes_,
    UInt64 parallel_build_min_rows_)
    : ISimpleTransform(header_, header_, true)
    , limit_hint(limit_hint_)
    , is_pre_distinct(is_pre_distinct_)
    , two_level_threshold(two_level_threshold_)
    , two_level_threshold_bytes(two_level_threshold_bytes_)
    , parallel_build_min_rows(parallel_build_min_rows_)
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

    if (!is_pre_distinct && max_threads_ > 1)
        pool = std::make_unique<ThreadPool>(
            CurrentMetrics::DistinctThreads,
            CurrentMetrics::DistinctThreadsActive,
            CurrentMetrics::DistinctThreadsScheduled,
            std::min(max_threads_, MAX_TWO_LEVEL_BUILD_THREADS));
}

DistinctTransform::~DistinctTransform() = default;

bool DistinctTransform::shouldBuildParallel(size_t num_rows) const
{
    /// Parallelize only with a pool and a chunk big enough to amortize the scatter.
    /// `parallel_build_min_rows == 0` disables the minimum. A policy hook: the decision
    /// can later be driven by online per-block signals instead of this fixed heuristic.
    return pool != nullptr && num_rows > parallel_build_min_rows;
}

template <typename Method>
void DistinctTransform::buildFilter(
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

/// Build the chunk's distinctness filter against a two-level set, parallelized by bucket. Two barriers:
/// A. Each worker scans its own row slice once, appending `(row, hash)` into its own per-bucket buffers
///    (`local_rows`/`local_hashes[w * NUM_BUCKETS + b]`) — private, so no contention or prefix-sum pass.
/// B. One task per bucket emplaces every worker's slice for that bucket. Buckets are disjoint, so it is
///    lock-free; the key is re-derived from the row (buffers stay key-type independent) and phase-A hashes
///    are reused, prefetching ~16 entries ahead.
/// String keys (`KeyType == std::string_view`) still point into the transient chunk, so phase B copies the
/// bytes into this bucket's own arena before emplacing — the stored key outlives the chunk. One worker per
/// bucket means each arena is single-writer.
/// Scratch lives in `two_level_scratch`, reused across chunks; called only from `transform` (one chunk at
/// a time), so never accessed concurrently.
template <typename Method>
void DistinctTransform::buildTwoLevelParallelFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumnFilter & filter,
    const size_t rows,
    ThreadPool & thread_pool) const
{
    using BucketData = std::decay_t<decltype(method.data)>;
    constexpr size_t NUM_BUCKETS = BucketData::NUM_BUCKETS;
    static_assert(NUM_BUCKETS == two_level_num_fine_buckets);
    static_assert(NUM_BUCKETS <= 256);

    using KeyType = typename BucketData::key_type;

    /// Scale worker count to the chunk size so a chunk that barely clears the gate does not fan out to
    /// the whole pool: each worker should own at least `parallel_build_min_rows` rows. The pool size is
    /// already capped at construction (`MAX_TWO_LEVEL_BUILD_THREADS`).
    const size_t grain = std::max<size_t>(parallel_build_min_rows, 1);
    const size_t work_workers = std::max<size_t>((rows + grain - 1) / grain, 1);
    const size_t num_workers = std::min({thread_pool.getMaxThreads(), NUM_BUCKETS, work_workers});
    if (num_workers == 0 || rows == 0)
        return;

    auto & scratch = two_level_scratch;
    const size_t num_slots = num_workers * NUM_BUCKETS;
    if (scratch.local_rows.size() < num_slots)
    {
        scratch.local_rows.resize(num_slots);
        scratch.local_hashes.resize(num_slots);
    }
    for (size_t s = 0; s < num_slots; ++s)
    {
        scratch.local_rows[s].clear();
        scratch.local_hashes[s].clear();
    }

    const auto worker_range = [rows, num_workers](size_t w)
    {
        const size_t per_worker = (rows + num_workers - 1) / num_workers;
        const size_t lo = std::min(w * per_worker, rows);
        const size_t hi = std::min(lo + per_worker, rows);
        return std::pair{lo, hi};
    };

    /// Phase A: hash + partition each worker's row-slice into its own per-bucket buffers.
    parallelPerWorker(thread_pool, ThreadName::DISTINCT_FINAL, num_workers,
        [&](size_t w)
        {
            typename Method::State state(columns, key_sizes, nullptr);
            Arena unused_pool;
            PaddedPODArray<UInt32> * rows_buf = &scratch.local_rows[w * NUM_BUCKETS];
            PaddedPODArray<UInt64> * hash_buf = &scratch.local_hashes[w * NUM_BUCKETS];
            const auto [lo, hi] = worker_range(w);
            for (size_t i = lo; i < hi; ++i)
            {
                auto kh = state.getKeyHolder(i, unused_pool);
                const auto h = method.data.hash(keyHolderGetKey(kh));
                const auto b = method.data.getBucketFromHash(h);
                rows_buf[b].push_back(static_cast<UInt32>(i));
                hash_buf[b].push_back(h);
            }
        });

    /// Phase B: one task per bucket, emplacing every worker's slice for that bucket.
    parallelDispatch(thread_pool, ThreadName::DISTINCT_FINAL, num_workers, NUM_BUCKETS,
        [&](size_t bucket)
        {
            auto & impl = method.data.impls[bucket];
            typename BucketData::Impl::LookupResult it;
            constexpr size_t prefetch_dist = 16;

            [[maybe_unused]] Arena * bucket_arena = nullptr;
            if constexpr (std::is_same_v<KeyType, std::string_view>)
            {
                auto & arena_ptr = scratch.bucket_arenas[bucket];
                if (!arena_ptr)
                    arena_ptr = std::make_unique<Arena>();
                bucket_arena = arena_ptr.get();
            }

            typename Method::State state(columns, key_sizes, nullptr);
            Arena unused_pool;
            for (size_t w = 0; w < num_workers; ++w)
            {
                const auto & rows_buf = scratch.local_rows[w * NUM_BUCKETS + bucket];
                const auto & hash_buf = scratch.local_hashes[w * NUM_BUCKETS + bucket];
                const size_t n = rows_buf.size();
                for (size_t j = 0; j < n; ++j)
                {
                    if (j + prefetch_dist < n)
                        impl.prefetchByHash(hash_buf[j + prefetch_dist]);

                    const UInt32 row = rows_buf[j];
                    auto kh = state.getKeyHolder(row, unused_pool);
                    KeyType key = keyHolderGetKey(kh);
                    if constexpr (std::is_same_v<KeyType, std::string_view>)
                    {
                        const char * persisted = bucket_arena->insert(key.data(), key.size());
                        key = std::string_view(persisted, key.size());
                    }
                    bool inserted;
                    impl.emplace(key, it, inserted, hash_buf[j]);
                    filter[row] = inserted;
                }
            }
        });
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

    if (lc_optimization_controller.isEnabled() && key_columns_pos.size() == 1)
    {
        if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(column_ptrs[0]))
        {
            auto [mask, new_indices_count] = buildLowCardinalityMask(*lc, num_rows);
            lc_optimization_controller.update(num_rows, new_indices_count);
            lc_mask.emplace(std::move(mask));

            /// Empty mask -> no candidate rows in this chunk, emit nothing.
            if (lc_mask->empty())
                return;
        }
    }

    if (data.empty())
        data.init(SetVariants::chooseMethod(column_ptrs, key_sizes));

    /// Promote single-level → two-level once the set crosses the row-count OR byte threshold,
    /// which unlocks the per-bucket parallel build below. A threshold of 0 disables that trigger;
    /// both 0 disables promotion entirely.
    if (!is_pre_distinct
        && pool
        && SetVariants::isConvertibleToTwoLevel(data.type)
        && ((two_level_threshold != 0 && data.getTotalRowCount() > two_level_threshold)
            || (two_level_threshold_bytes != 0 && data.getTotalByteCount() > two_level_threshold_bytes)))
    {
        data.convertToTwoLevel();
        ProfileEvents::increment(ProfileEvents::DistinctHashTablesInitializedAsTwoLevel);
    }

    const auto old_set_size = data.getTotalRowCount();
    IColumn::Filter filter(num_rows);
    auto * lc_mask_ptr = lc_mask ? &*lc_mask : nullptr;

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;

#define APPLY_FOR_SET_VARIANTS_DISTINCT(M) \
        M(key8) \
        M(key16) \
        M(key32) \
        M(key64) \
        M(key_string) \
        M(key_fixed_string) \
        M(keys128) \
        M(keys256) \
        M(nullable_keys128) \
        M(nullable_keys256) \
        M(hashed)

#define M(NAME) \
        case SetVariants::Type::NAME: \
            buildFilter(*data.NAME, column_ptrs, filter, num_rows, data, lc_mask_ptr); \
        break;
        APPLY_FOR_SET_VARIANTS_DISTINCT(M)
#undef M
#undef APPLY_FOR_SET_VARIANTS_DISTINCT

        /// Two-level fixed-width-key families: parallel build when `shouldBuildParallel`, else serial.
#define DISPATCH_TWO_LEVEL(NAME) \
        case SetVariants::Type::NAME: \
        { \
            auto & set = *data.NAME; \
            if (shouldBuildParallel(num_rows)) \
            { \
                ProfileEvents::increment(ProfileEvents::DistinctTwoLevelParallelFilterBuilds); \
                buildTwoLevelParallelFilter(set, column_ptrs, filter, num_rows, *pool); \
            } \
            else \
            { \
                ProfileEvents::increment(ProfileEvents::DistinctTwoLevelSerialFilterBuilds); \
                buildFilter(set, column_ptrs, filter, num_rows, data, lc_mask_ptr); \
            } \
            break; \
        }
        DISPATCH_TWO_LEVEL(hashed_two_level)
        DISPATCH_TWO_LEVEL(key32_two_level)
        DISPATCH_TWO_LEVEL(key64_two_level)
        DISPATCH_TWO_LEVEL(keys128_two_level)
        DISPATCH_TWO_LEVEL(keys256_two_level)
        DISPATCH_TWO_LEVEL(nullable_keys128_two_level)
        DISPATCH_TWO_LEVEL(nullable_keys256_two_level)

        /// String two-level variants: phase B persists keys into per-bucket arenas
        /// (see `buildTwoLevelParallelFilter`).
        DISPATCH_TWO_LEVEL(key_string_two_level)
        DISPATCH_TWO_LEVEL(key_fixed_string_two_level)
#undef DISPATCH_TWO_LEVEL
    }

    const auto new_set_size = data.getTotalRowCount();
    const size_t num_selected = new_set_size - old_set_size;

    /// Just go to the next chunk if there isn't any new record in the current one.
    if (num_selected == 0)
        return;

    /// In case of overflow_mode = 'break' `check` returns false instead of throwing.
    /// Stop reading, but still emit the new rows from the current chunk (their keys are
    /// already in the set): 'break' means return a partial result as if the source data
    /// ran out, not discard it.
    if (!set_size_limits.check(new_set_size, data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        stopReading();

    if (num_selected == num_rows)
    {
        /// Every row is a new distinct value: keep the chunk unchanged, without copying it.
        chunk.setColumns(std::move(columns), num_rows);
    }
    else
    {
        for (auto & column : columns)
            column = column->filter(filter, -1);

        chunk.setColumns(std::move(columns), num_selected);
    }

    /// Stop reading if we already reach the limit
    if (limit_hint && new_set_size >= limit_hint)
        stopReading();
}

/// Explicit instantiations of `buildTwoLevelParallelFilter` for all convertible two-level
/// methods, including the string families (`key_string_two_level`, `key_fixed_string_two_level`),
/// whose phase-3 emplace persists keys into per-bucket arenas.
#define INSTANTIATE_TWO_LEVEL_BUILD(METHOD_TYPE) \
    template void DistinctTransform::buildTwoLevelParallelFilter<METHOD_TYPE>( \
        METHOD_TYPE &, const ColumnRawPtrs &, IColumnFilter &, size_t, ThreadPool &) const;

using NonClearableHashedTwoLevel          = SetMethodHashedTwoLevel<TwoLevelHashSet<UInt128, UInt128TrivialHash>>;
using NonClearableKey32TwoLevel           = SetMethodOneNumber<UInt32, TwoLevelHashSet<UInt32, HashCRC32<UInt32>>>;
using NonClearableKey64TwoLevel           = SetMethodOneNumber<UInt64, TwoLevelHashSet<UInt64, HashCRC32<UInt64>>>;
using NonClearableKeyStringTwoLevel       = SetMethodString<TwoLevelHashSetWithSavedHash<std::string_view>>;
using NonClearableKeyFixedStringTwoLevel  = SetMethodFixedString<TwoLevelHashSetWithSavedHash<std::string_view>>;
using NonClearableKeys128TwoLevel         = SetMethodKeysFixed<TwoLevelHashSet<UInt128, UInt128HashCRC32>>;
using NonClearableKeys256TwoLevel         = SetMethodKeysFixed<TwoLevelHashSet<UInt256, UInt256HashCRC32>>;
using NonClearableNullableKeys128TwoLevel = SetMethodKeysFixed<TwoLevelHashSet<UInt128, UInt128HashCRC32>, true>;
using NonClearableNullableKeys256TwoLevel = SetMethodKeysFixed<TwoLevelHashSet<UInt256, UInt256HashCRC32>, true>;

INSTANTIATE_TWO_LEVEL_BUILD(NonClearableHashedTwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKey32TwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKey64TwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKeyStringTwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKeyFixedStringTwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKeys128TwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKeys256TwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableNullableKeys128TwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableNullableKeys256TwoLevel)

#undef INSTANTIATE_TWO_LEVEL_BUILD

}
