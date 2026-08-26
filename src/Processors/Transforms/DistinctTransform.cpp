#include <Processors/Transforms/DistinctTransform.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <DataTypes/NullableUtils.h>
#include <Common/assert_cast.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>

#include <Common/Exception.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <exception>
#include <mutex>
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

/// Workers of the two-level parallel build, started once on the first parallel chunk and reused for
/// every later one.
///
/// The build used to enqueue `num_workers` thread-pool jobs for each of its two phases, so the job
/// count grew with the number of chunks rather than with the query's data - a 50M-row `DISTINCT`
/// enqueued about 9000 of them. Every pool job wraps its callback in a `ThreadGroupSwitcher`, whose
/// attach snapshots the ~1500-entry `ProfileEvents::Counters` array and reads `/proc/thread-self/*`,
/// so that per-job cost dominated the build's fixed overhead. The aggregation path never pays it,
/// because it fans out once, at merge time. Here the fan-out happens once too: the workers stay on
/// their pool jobs for the transform's lifetime and are handed each chunk's phases through a
/// condition variable, which leaves two rendezvous per chunk instead of `2 * num_workers` jobs.
///
/// A parked worker holds a pool job but consumes no CPU. The set is sized to the pool, so the jobs
/// always have a thread to run on and a phase cannot wait on a job that was never scheduled.
class TwoLevelBuildWorkers
{
public:
    TwoLevelBuildWorkers(ThreadPool & pool_, ThreadName thread_name_, size_t max_workers_)
        : max_workers(max_workers_), runner(pool_, thread_name_)
    {
    }

    ~TwoLevelBuildWorkers()
    {
        {
            std::lock_guard lock(mutex);
            stop = true;
        }
        work_available.notify_all();
        try
        {
            runner.waitForAllToFinishAndRethrowFirstError();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    /// `body(w)` once for each of the first `active` workers, with its own index. Use when each worker
    /// owns a fixed slice of the input that has to line up across phases.
    void runPerWorker(TwoLevelPhaseBody & phase_body, size_t active)
    {
        runPhase(phase_body, PhaseKind::PerWorker, active, 0);
    }

    /// `body(i)` for every i in [0, total), the first `active` workers pulling through an atomic cursor.
    void runDispatch(TwoLevelPhaseBody & phase_body, size_t active, size_t total)
    {
        runPhase(phase_body, PhaseKind::Dispatch, active, total);
    }

private:
    enum class PhaseKind : uint8_t
    {
        PerWorker,
        Dispatch,
    };

    /// Start workers up to `needed`, so a query whose chunks only ever use a few of them never parks
    /// the whole pool. Called only from the main thread, which is the only writer of `started_workers`.
    void ensureStarted(size_t needed)
    {
        for (; started_workers < needed; ++started_workers)
            runner.enqueueAndKeepTrack([this, w = started_workers] { workerLoop(w); }, Priority{});
    }

    void runPhase(TwoLevelPhaseBody & phase_body, PhaseKind phase_kind, size_t active, size_t total)
    {
        chassert(active <= max_workers);
        ensureStarted(active);

        {
            std::lock_guard lock(mutex);
            body = &phase_body;
            kind = phase_kind;
            active_workers = active;
            dispatch_total = total;
            cursor.store(0, std::memory_order_relaxed);
            done_count = 0;
            ++phase_seq;
        }
        work_available.notify_all();

        std::unique_lock lock(mutex);
        phase_finished.wait(lock, [this] { return done_count == active_workers; });
        body = nullptr;

        if (first_error)
        {
            auto error = first_error;
            first_error = {};
            std::rethrow_exception(error);
        }
    }

    void workerLoop(size_t worker_index)
    {
        size_t seen_seq = 0;
        while (true)
        {
            TwoLevelPhaseBody * phase_body = nullptr;
            PhaseKind phase_kind = PhaseKind::PerWorker;
            size_t total = 0;
            {
                std::unique_lock lock(mutex);
                work_available.wait(lock, [this, &seen_seq] { return stop || phase_seq != seen_seq; });
                if (stop)
                    return;
                seen_seq = phase_seq;

                /// Not taking part in this phase: back to waiting, without counting towards it.
                if (worker_index >= active_workers)
                    continue;

                phase_body = body;
                phase_kind = kind;
                total = dispatch_total;
            }

            try
            {
                if (phase_kind == PhaseKind::PerWorker)
                {
                    phase_body->run(worker_index);
                }
                else
                {
                    while (true)
                    {
                        const size_t i = cursor.fetch_add(1, std::memory_order_relaxed);
                        if (i >= total)
                            break;
                        phase_body->run(i);
                    }
                }
            }
            catch (...)
            {
                std::lock_guard lock(mutex);
                if (!first_error)
                    first_error = std::current_exception();
            }

            {
                std::lock_guard lock(mutex);
                ++done_count;
                if (done_count == active_workers)
                {
                    /// Notify with the lock held: the waiter's predicate reads `done_count`.
                    phase_finished.notify_one();
                }
            }
        }
    }

    /// Upper bound on the workers, and so on the pool jobs this may hold: sized to the pool, so a
    /// started worker always has a thread and a phase never waits on a job the pool could not run.
    const size_t max_workers;
    ThreadPoolCallbackRunnerLocal<void> runner;
    size_t started_workers = 0;

    std::mutex mutex;
    std::condition_variable work_available;
    std::condition_variable phase_finished;

    /// Bumped by the main thread for every published phase; a worker compares it against the last
    /// phase it ran to tell a new phase from a spurious wake-up.
    size_t phase_seq = 0;
    size_t done_count = 0;
    size_t active_workers = 0;
    size_t dispatch_total = 0;
    TwoLevelPhaseBody * body = nullptr;
    PhaseKind kind = PhaseKind::PerWorker;
    std::atomic<size_t> cursor{0};
    bool stop = false;
    std::exception_ptr first_error;
};

TwoLevelBuildWorkers & DistinctTransform::twoLevelWorkers(ThreadPool & thread_pool) const
{
    if (!two_level_workers)
    {
        /// Size the set to the pool, so every worker's job has a thread and a phase never waits on a
        /// job that the pool could not schedule. Only `num_workers` of them are active per chunk.
        const size_t size = std::min<size_t>(thread_pool.getMaxThreads(), two_level_num_fine_buckets);
        two_level_workers = std::make_unique<TwoLevelBuildWorkers>(thread_pool, ThreadName::DISTINCT_FINAL, size);
    }
    return *two_level_workers;
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
    size_t max_threads_,
    UInt64 two_level_threshold_,
    UInt64 two_level_threshold_bytes_,
    UInt64 parallel_build_min_rows_,
    bool allow_abandoning_,
    bool skip_null_keys_)
    : ISimpleTransform(header_, header_, true)
    , limit_hint(limit_hint_)
    , two_level_threshold(two_level_threshold_)
    , two_level_threshold_bytes(two_level_threshold_bytes_)
    , parallel_build_min_rows(parallel_build_min_rows_)
    , set_size_limits(set_size_limits_)
    , skip_null_keys(skip_null_keys_)
{
    if (allow_abandoning_)
        abandon_controller.emplace();

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

    /// The two-level parallel build only pays off for the single-stream final deduplication. A
    /// preliminary (abandoning) deduplication runs per input stream and is given `max_threads_ = 1`
    /// by its caller, so no pool is created for it and it never oversubscribes the CPU.
    if (!allow_abandoning_ && max_threads_ > 1)
        pool = std::make_unique<ThreadPool>(
            CurrentMetrics::DistinctThreads,
            CurrentMetrics::DistinctThreadsActive,
            CurrentMetrics::DistinctThreadsScheduled,
            std::min(max_threads_, MAX_TWO_LEVEL_BUILD_THREADS));
}

DistinctTransform::~DistinctTransform() = default;

size_t DistinctTransform::twoLevelWorkerCount(size_t num_rows) const
{
    if (!pool)
        return 0;

    /// Each worker should own at least `parallel_build_min_rows` rows (a floor division, so the
    /// per-worker slice never drops below the grain). `parallel_build_min_rows == 0` disables that
    /// minimum. Capped by the pool size and the bucket count, matching `buildTwoLevelParallelFilter`.
    const size_t grain = std::max<size_t>(parallel_build_min_rows, 1);
    const size_t work_workers = std::max<size_t>(num_rows / grain, 1);
    return std::min({pool->getMaxThreads(), two_level_num_fine_buckets, work_workers});
}

bool DistinctTransform::shouldBuildParallel(size_t num_rows) const
{
    /// Parallelize only when the chunk is large enough to keep at least two workers busy. A chunk
    /// that would floor to a single worker takes the cheaper serial path instead of paying the
    /// two-phase scatter (partition + per-bucket emplace) for no parallelism. A policy hook: the
    /// decision can later be driven by online per-block signals instead of this fixed heuristic.
    return twoLevelWorkerCount(num_rows) > 1;
}

size_t DistinctTransform::totalSetByteCount() const
{
    size_t bytes = data ? data->getTotalByteCount() : 0;
    for (const auto & arena : two_level_scratch.bucket_arenas)
        if (arena)
            bytes += arena->allocatedBytes();
    return bytes;
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

    /// Worker count scales with the chunk size (each worker owns at least `parallel_build_min_rows`
    /// rows) and is capped by the pool and the bucket count. `shouldBuildParallel` gates the dispatch
    /// on the same helper returning > 1, so a single-worker chunk never reaches here — it takes the
    /// cheaper serial path instead. The pool size is already capped at construction
    /// (`MAX_TWO_LEVEL_BUILD_THREADS`).
    const size_t num_workers = twoLevelWorkerCount(rows);
    if (num_workers == 0 || rows == 0)
        return;

    /// The key cache is only populated for the trivially-copyable key families; string keys re-derive
    /// their (cheap) view in phase B and persist it through a per-bucket arena.
    constexpr bool cache_keys = !std::is_same_v<KeyType, std::string_view>;

    auto & scratch = two_level_scratch;
    const size_t num_slots = num_workers * NUM_BUCKETS;
    if (scratch.local_rows.size() < num_slots)
    {
        scratch.local_rows.resize(num_slots);
        scratch.local_hashes.resize(num_slots);
        scratch.local_keys.resize(num_slots);
    }
    for (size_t s = 0; s < num_slots; ++s)
    {
        scratch.local_rows[s].clear();
        scratch.local_hashes[s].clear();
        if constexpr (cache_keys)
            scratch.local_keys[s].clear();
    }

    const auto worker_range = [rows, num_workers](size_t w)
    {
        const size_t per_worker = (rows + num_workers - 1) / num_workers;
        const size_t lo = std::min(w * per_worker, rows);
        const size_t hi = std::min(lo + per_worker, rows);
        return std::pair{lo, hi};
    };

    auto & workers = twoLevelWorkers(thread_pool);

    /// Phase A: hash + partition each worker's row-slice into its own per-bucket buffers.
    auto phase_a = [&](size_t w)
        {
            typename Method::State state(columns, key_sizes, nullptr);
            Arena unused_pool;
            PaddedPODArray<UInt32> * rows_buf = &scratch.local_rows[w * NUM_BUCKETS];
            PaddedPODArray<UInt64> * hash_buf = &scratch.local_hashes[w * NUM_BUCKETS];
            PaddedPODArray<char> * keys_buf = &scratch.local_keys[w * NUM_BUCKETS];
            const auto [lo, hi] = worker_range(w);
            for (size_t i = lo; i < hi; ++i)
            {
                auto kh = state.getKeyHolder(i, unused_pool);
                const auto & key = keyHolderGetKey(kh);
                const auto h = method.data.hash(key);
                const auto b = method.data.getBucketFromHash(h);
                rows_buf[b].push_back(static_cast<UInt32>(i));
                hash_buf[b].push_back(h);
                if constexpr (cache_keys)
                {
                    const char * key_bytes = reinterpret_cast<const char *>(&key);
                    keys_buf[b].insert(key_bytes, key_bytes + sizeof(KeyType));
                }
            }
        };
    TwoLevelPhaseBodyOf<decltype(phase_a)> phase_a_body{phase_a};
    workers.runPerWorker(phase_a_body, num_workers);

    /// Phase B: one task per bucket, emplacing every worker's slice for that bucket.
    auto phase_b = [&](size_t bucket)
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

            [[maybe_unused]] typename Method::State state(columns, key_sizes, nullptr);
            [[maybe_unused]] Arena unused_pool;
            for (size_t w = 0; w < num_workers; ++w)
            {
                const auto & rows_buf = scratch.local_rows[w * NUM_BUCKETS + bucket];
                const auto & hash_buf = scratch.local_hashes[w * NUM_BUCKETS + bucket];
                [[maybe_unused]] const auto & keys_buf = scratch.local_keys[w * NUM_BUCKETS + bucket];
                const size_t n = rows_buf.size();
                for (size_t j = 0; j < n; ++j)
                {
                    if (j + prefetch_dist < n)
                        impl.prefetchByHash(hash_buf[j + prefetch_dist]);

                    const UInt32 row = rows_buf[j];
                    bool inserted = false;
                    if constexpr (std::is_same_v<KeyType, std::string_view>)
                    {
                        /// The string view is cheap to re-derive; persist it into the per-bucket arena
                        /// only when the key is actually inserted. `ArenaKeyHolder` copies on
                        /// `keyHolderPersistKey` (called by `emplace` on insert) and discards otherwise,
                        /// so a duplicate row adds no bytes and the arena stays proportional to the
                        /// distinct keys, matching the serial path.
                        auto kh = state.getKeyHolder(row, unused_pool);
                        KeyType key = keyHolderGetKey(kh);
                        ArenaKeyHolder key_holder{key, *bucket_arena};
                        impl.emplace(key_holder, it, inserted, hash_buf[j]);
                    }
                    else
                    {
                        /// Reuse the key computed in phase A instead of re-deriving it, so the `hashed`
                        /// carrier does not run its `hash128` over every key column a second time.
                        KeyType key{};
                        memcpy(&key, keys_buf.data() + j * sizeof(KeyType), sizeof(KeyType));
                        impl.emplace(key, it, inserted, hash_buf[j]);
                    }
                    filter[row] = inserted;
                }
            }
        };
    TwoLevelPhaseBodyOf<decltype(phase_b)> phase_b_body{phase_b};
    workers.runDispatch(phase_b_body, num_workers, NUM_BUCKETS);
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
                if (abandon_controller)
                    abandon_controller->update(num_rows, 0, data->getTotalByteCount());
                return;
            }
        }
    }

    if (data->empty())
        data->init(SetVariants::chooseMethod(column_ptrs, key_sizes));

    /// Promote single-level -> two-level, which unlocks the per-bucket parallel build below.
    ///
    /// The trigger is aligned to the single-level table's own growth. `HashTable::resize` rehashes every
    /// cell into the enlarged buffer, and building the two-level table rehashes every cell into the 256
    /// sub-tables - the same amount of work. So converting *instead of* resizing costs approximately
    /// nothing: the extra rehash is paid for by the resize it replaces. Converting at an arbitrary set
    /// size would instead add a full O(set size) rehash on top, which is pure overhead for a query whose
    /// set stops growing shortly afterwards - the case that made a `DISTINCT` over exactly
    /// `distinct_two_level_threshold` keys slower than the single-level path.
    ///
    /// A single-level table grows once its element count passes half of its cell capacity
    /// (`HashTableGrower::maxFill`), so `rows_in_set + num_rows > cells / 2` says this chunk is about to
    /// push it over. `num_rows` over-estimates the insertions, since duplicate rows do not insert, so the
    /// conversion can fire one chunk early; that is harmless, because the rehash it pays still scales
    /// with the current set size and the resize is still skipped.
    ///
    /// `two_level_threshold` is a minimum set size, not the trigger: below it the set holds too little
    /// data to be worth spreading over 256 sub-tables. `two_level_threshold_bytes` stays an independent
    /// trigger - it fires early for expensive keys, where a set of few but long keys crosses it while the
    /// table itself is still small, and the parallel build already pays off. A threshold of 0 disables
    /// that trigger; both 0 disables promotion entirely.
    ///
    /// Promotion exists only to unlock the parallel build, so it is gated on exactly the condition the
    /// dispatch below uses to choose that build: a chunk too small to keep two workers busy, or a live
    /// `LowCardinality` first-occurrence mask (which only the serial build consumes), would leave a
    /// promoted set paying the conversion and never using what it bought. This matters for an input a
    /// preliminary DISTINCT has already reduced to small chunks - there the two-level table would
    /// otherwise be built and then only ever probed serially. A pool only exists for the final
    /// deduplication (see the constructor), and `shouldBuildParallel` is false without one.
    if (shouldBuildParallel(num_rows) && !lc_mask && SetVariants::isConvertibleToTwoLevel(data->type))
    {
        const size_t rows_in_set = data->getTotalRowCount();

        /// About to rehash anyway: fold the two-level split into the growth it replaces.
        bool convert = two_level_threshold != 0
            && rows_in_set + num_rows >= two_level_threshold
            && rows_in_set + num_rows > data->getBufferSizeInCells() / 2;

        if (!convert && two_level_threshold_bytes != 0)
        {
            size_t projected_bytes = data->getTotalByteCount();
            for (const auto * col : column_ptrs)
                projected_bytes += col->byteSize();
            convert = projected_bytes >= two_level_threshold_bytes;
        }

        if (convert)
        {
            data->convertToTwoLevel();
            ProfileEvents::increment(ProfileEvents::DistinctHashTablesInitializedAsTwoLevel);
        }
    }

    const auto old_set_size = data->getTotalRowCount();
    IColumn::Filter filter(num_rows);
    auto * lc_mask_ptr = lc_mask ? &*lc_mask : nullptr;

    switch (data->type)
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
        M(keys32) \
        M(keys64) \
        M(keys128) \
        M(keys256) \
        M(nullable_keys128) \
        M(nullable_keys256) \
        M(hashed)

#define M(NAME) \
        case SetVariants::Type::NAME: \
            buildFilter(*data->NAME, column_ptrs, filter, num_rows, *data, lc_mask_ptr); \
        break;
        APPLY_FOR_SET_VARIANTS_DISTINCT(M)
#undef M
#undef APPLY_FOR_SET_VARIANTS_DISTINCT

        /// Two-level fixed-width-key families: parallel build when `shouldBuildParallel`, else serial.
        /// A non-null `lc_mask_ptr` forces the serial path: only it consumes the single-column
        /// `LowCardinality` first-occurrence mask, so probing in parallel would re-hash every duplicate
        /// row and turn the O(dictionary size) LC fast path back into O(rows).
#define DISPATCH_TWO_LEVEL(NAME) \
        case SetVariants::Type::NAME: \
        { \
            auto & set = *data->NAME; \
            if (shouldBuildParallel(num_rows) && !lc_mask_ptr) \
            { \
                ProfileEvents::increment(ProfileEvents::DistinctTwoLevelParallelFilterBuilds); \
                buildTwoLevelParallelFilter(set, column_ptrs, filter, num_rows, *pool); \
            } \
            else \
            { \
                ProfileEvents::increment(ProfileEvents::DistinctTwoLevelSerialFilterBuilds); \
                buildFilter(set, column_ptrs, filter, num_rows, *data, lc_mask_ptr); \
            } \
            break; \
        }
        DISPATCH_TWO_LEVEL(hashed_two_level)
        DISPATCH_TWO_LEVEL(key32_two_level)
        DISPATCH_TWO_LEVEL(key64_two_level)
        DISPATCH_TWO_LEVEL(keys32_two_level)
        DISPATCH_TWO_LEVEL(keys64_two_level)
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

    const auto new_set_size = data->getTotalRowCount();
    const size_t num_selected = new_set_size - old_set_size;

    if (abandon_controller)
    {
        abandon_controller->update(num_rows, num_selected, data->getTotalByteCount());
        if (abandon_controller->isAbandoned())
        {
            data.reset();
            lc_dict_states.clear();
        }
    }

    /// Just go to the next chunk if there isn't any new record in the current one.
    if (num_selected == 0)
        return;

    /// In case of overflow_mode = 'break' `check` returns false instead of throwing.
    /// Stop reading, but still emit the new rows from the current chunk (their keys are
    /// already in the set): 'break' means return a partial result as if the source data
    /// ran out, not discard it.
    if (!set_size_limits.check(new_set_size, totalSetByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
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
        METHOD_TYPE &, const ColumnRawPtrs &, IColumnFilter &, size_t, ThreadPool &) const; // NOLINT(bugprone-macro-parentheses)

using NonClearableHashedTwoLevel          = SetMethodHashedTwoLevel<TwoLevelHashSet<UInt128, UInt128TrivialHash>>;
using NonClearableKey32TwoLevel           = SetMethodOneNumber<UInt32, TwoLevelHashSet<UInt32, HashCRC32<UInt32>>>;
using NonClearableKey64TwoLevel           = SetMethodOneNumber<UInt64, TwoLevelHashSet<UInt64, HashCRC32<UInt64>>>;
using NonClearableKeyStringTwoLevel       = SetMethodString<TwoLevelHashSetWithSavedHash<std::string_view>>;
using NonClearableKeyFixedStringTwoLevel  = SetMethodFixedString<TwoLevelHashSetWithSavedHash<std::string_view>>;
using NonClearableKeys32TwoLevel          = SetMethodKeysFixed<TwoLevelHashSet<UInt32, HashCRC32<UInt32>>>;
using NonClearableKeys64TwoLevel          = SetMethodKeysFixed<TwoLevelHashSet<UInt64, HashCRC32<UInt64>>>;
using NonClearableKeys128TwoLevel         = SetMethodKeysFixed<TwoLevelHashSet<UInt128, UInt128HashCRC32>>;
using NonClearableKeys256TwoLevel         = SetMethodKeysFixed<TwoLevelHashSet<UInt256, UInt256HashCRC32>>;
using NonClearableNullableKeys128TwoLevel = SetMethodKeysFixed<TwoLevelHashSet<UInt128, UInt128HashCRC32>, true>;
using NonClearableNullableKeys256TwoLevel = SetMethodKeysFixed<TwoLevelHashSet<UInt256, UInt256HashCRC32>, true>;

INSTANTIATE_TWO_LEVEL_BUILD(NonClearableHashedTwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKey32TwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKey64TwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKeyStringTwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKeyFixedStringTwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKeys32TwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKeys64TwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKeys128TwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableKeys256TwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableNullableKeys128TwoLevel)
INSTANTIATE_TWO_LEVEL_BUILD(NonClearableNullableKeys256TwoLevel)

#undef INSTANTIATE_TWO_LEVEL_BUILD

}
