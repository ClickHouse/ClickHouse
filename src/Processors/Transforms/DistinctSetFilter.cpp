#include <Processors/Transforms/DistinctSetFilter.h>
#include <Processors/Transforms/PhasedWorkers.h>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NullableUtils.h>
#include <Common/Arena.h>
#include <Common/BitHelpers.h>
#include <Common/ColumnsHashing.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/assert_cast.h>
#include <Common/setThreadName.h>
#include <base/arithmeticOverflow.h>

#include <array>
#include <cstring>
#include <limits>
#include <optional>
#include <unordered_map>
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

ColumnNumbers calculateDistinctKeyColumnsPositions(const Block & header, const Names & columns)
{
    const size_t num_columns = columns.empty() ? header.columns() : columns.size();
    ColumnNumbers key_columns_pos;
    key_columns_pos.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto pos = columns.empty() ? i : header.getPositionByName(columns[i]);
        const auto & col = header.getByPosition(pos).column;
        if (col && !isColumnConst(*col))
            key_columns_pos.emplace_back(pos);
    }
    return key_columns_pos;
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

struct DistinctLowCardinalityFilter::DictionariesState
{
    using LCDictionaryKey = ColumnsHashing::LowCardinalityDictionaryCache::DictionaryKey;
    using LCDictionaryKeyHash = ColumnsHashing::LowCardinalityDictionaryCache::DictionaryKeyHash;

    struct LCDictState
    {
        explicit LCDictState(size_t dictionary_size) : seen_indices(dictionary_size, UInt8{0})
        {
        }

        /// `seen_indices[idx] == 1` means dictionary index `idx` has been seen at least once for this
        /// dictionary identity.
        PODArray<UInt8> seen_indices;

        /// Number of dictionary indices we have seen at least once. When this
        /// reaches the dictionary size, any future row for the parent chunk cannot
        /// introduce a new distinct value.
        UInt64 seen_count = 0;
    };

    /// Per-dictionary state which may cover multiple `IColumn` instances.
    std::unordered_map<LCDictionaryKey, LCDictState, LCDictionaryKeyHash> lc_dict_states;
};

DistinctLowCardinalityFilter::DistinctLowCardinalityFilter()
    : dictionaries_state(std::make_unique<DictionariesState>())
{
}

DistinctLowCardinalityFilter::~DistinctLowCardinalityFilter() = default;

std::optional<IColumn::Filter> DistinctLowCardinalityFilter::buildMaskIfApplicable(const IColumn & column, size_t num_rows)
{
    if (!lc_optimization_controller.isEnabled())
        return std::nullopt;

    const auto * lc = typeid_cast<const ColumnLowCardinality *>(&column);
    if (!lc)
        return std::nullopt;

    auto [mask, new_indices_count] = buildMask(*lc, num_rows);
    lc_optimization_controller.update(num_rows, new_indices_count);

    if (!lc_optimization_controller.isEnabled())
    {
        dictionaries_state.reset();
        total_byte_count = 0;
    }

    return std::optional<IColumn::Filter>(std::move(mask));
}

size_t DistinctLowCardinalityFilter::estimateGrowthMemory(const IColumn & column) const
{
    if (!lc_optimization_controller.isEnabled() || column.empty())
        return 0;

    const auto * lc = typeid_cast<const ColumnLowCardinality *>(&column);
    if (!lc)
        return 0;

    const auto & dictionary = lc->getDictionary();
    const DictionariesState::LCDictionaryKey dict_key{dictionary.getHash(), dictionary.size()};
    if (dictionaries_state->lc_dict_states.contains(dict_key))
        return 0;

    return dictionary.size();
}

std::pair<IColumn::Filter, size_t> DistinctLowCardinalityFilter::buildMask(const ColumnLowCardinality & column, size_t num_rows)
{
    const auto & dictionary = column.getDictionary();
    const auto dict_size = dictionary.size();

    const DictionariesState::LCDictionaryKey dict_key{dictionary.getHash(), dict_size};

    /// The dictionary identity includes its size, so each bitmap is allocated once and never resized.
    auto [it, inserted] = dictionaries_state->lc_dict_states.try_emplace(dict_key, dict_size);
    auto & state = it->second;
    chassert(state.seen_indices.size() == dict_size);
    chassert(state.seen_count <= dict_size);
    if (inserted)
        total_byte_count += state.seen_indices.allocated_bytes();

    /// If we've already seen all dictionary indices for this dictionary, then no row in this chunk
    /// (and also other chunks with the same dictionary) can produce a new distinct value.
    if (state.seen_count == dict_size)
        return {{}, 0};

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

            mask[row] = 1;
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
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for LowCardinality column in DistinctLowCardinalityFilter");
    }

    return {std::move(mask), state.seen_count - seen_count_before};
}

namespace
{

/// Builds the `DISTINCT` filter for a chunk: `filter[i] == 1` for rows whose key was not in the set yet (the
/// rows are inserted into the set). `mask[i] == 0` marks rows excluded from the deduplication - known
/// duplicates by the `LowCardinality` dictionary index, or `NULL`-key rows in the `skip_null_keys` mode -
/// which are never inserted; `mask` may be `nullptr`.
template <typename Method>
void buildDistinctFilter(
    Method & method,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    IColumn::Filter & filter,
    const size_t rows,
    SetVariants & variants,
    const IColumn::Filter * mask)
{
    typename Method::State state(key_columns, key_sizes, /*context=*/ nullptr);

    if (mask)
    {
        for (size_t i = 0; i < rows; ++i)
        {
            if (!(*mask)[i])
            {
                /// The row is excluded from the deduplication, skip insertion.
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

/// Mark rows whose `LowCardinality` index is the dictionary's `NULL` entry with 0 in `keep`, allocating the
/// filter lazily on the first such row.
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for LowCardinality column in DistinctSetFilter");
    }
}

}

namespace
{

/// The two-level build is memory-bandwidth-bound (per-bucket allocate + zero-fill dominates), so
/// throughput saturates well before very high thread counts. Cap the build pool here regardless of
/// `max_threads` to avoid spawning threads that only add scheduling and allocator contention.
constexpr size_t MAX_TWO_LEVEL_BUILD_THREADS = 16;

using HashedTwoLevelMethod = SetMethodHashedTwoLevel<TwoLevelHashSet<UInt128, UInt128TrivialHash>>;
constexpr size_t two_level_num_fine_buckets = HashedTwoLevelMethod::Data::NUM_BUCKETS;

}

struct DistinctSetFilter::TwoLevelBuild
{
    explicit TwoLevelBuild(const DistinctTwoLevelBuildSettings & settings_)
        : settings(settings_)
        , pool(
              CurrentMetrics::DistinctThreads,
              CurrentMetrics::DistinctThreadsActive,
              CurrentMetrics::DistinctThreadsScheduled,
              std::min(settings_.max_threads, MAX_TWO_LEVEL_BUILD_THREADS))
    {
    }

    /// Number of workers the parallel build would use for `num_rows`. Shared by `shouldBuildParallel`
    /// (which rejects a single-worker chunk in favor of the cheaper serial path) and the build itself
    /// (which sizes its scratch to it), so the gate and the build never disagree on the worker count.
    size_t workerCount(size_t num_rows) const
    {
        /// Each worker should own at least `parallel_build_min_rows` rows (a floor division, so the
        /// per-worker slice never drops below the grain). `parallel_build_min_rows == 0` disables that
        /// minimum. Capped by the pool size and the bucket count.
        const size_t grain = std::max<size_t>(settings.parallel_build_min_rows, 1);
        const size_t work_workers = std::max<size_t>(num_rows / grain, 1);
        return std::min({pool.getMaxThreads(), two_level_num_fine_buckets, work_workers});
    }

    /// Parallelize only when the chunk is large enough to keep at least two workers busy. A chunk that
    /// would floor to a single worker takes the cheaper serial path instead of paying the two-phase
    /// scatter (partition + per-bucket emplace) for no parallelism.
    bool shouldBuildParallel(size_t num_rows) const { return workerCount(num_rows) > 1; }

    /// The worker set, started on the first parallel build and reused for every later chunk. One worker
    /// per bucket at most; `PhasedWorkers` clamps that to the pool's thread count, so a phase can never
    /// wait on a job the pool could not schedule. Only `workerCount` of them are active per chunk.
    PhasedWorkers & getWorkers()
    {
        if (!workers)
            workers = std::make_unique<PhasedWorkers>(pool, ThreadName::DISTINCT_FINAL, two_level_num_fine_buckets);
        return *workers;
    }

    size_t getArenasByteCount() const
    {
        size_t bytes = 0;
        for (const auto & arena : bucket_arenas)
            if (arena)
                bytes += arena->allocatedBytes();
        return bytes;
    }

    const DistinctTwoLevelBuildSettings settings;

    ThreadPool pool;

    /// Phase-A partition buffers, indexed `[worker * NUM_BUCKETS + bucket]`: each worker stores the row id
    /// and cached hash of its own rows (private, so no prefix-sum pass). Outer vectors sized once; inner
    /// arrays `clear()`-ed (capacity kept) per chunk, to avoid allocator churn.
    std::vector<PaddedPODArray<UInt32>> local_rows;
    std::vector<PaddedPODArray<UInt64>> local_hashes;

    /// Phase-A key bytes, `sizeof(KeyType)` per row, for the trivially-copyable (non-string) key families.
    /// Phase B reads them back instead of calling `getKeyHolder` again. This matters for the `hashed`
    /// carrier, whose key is `hash128` over every key column: without the cache that wide hash would run
    /// twice per row (once to bucket, once to emplace). Stored type-erased so the same buffers serve every
    /// key type across chunks; read back with `memcpy` (the byte offset is not aligned for
    /// `UInt128`/`UInt256`). Left empty for the string families.
    std::vector<PaddedPODArray<char>> local_keys;

    /// One arena per bucket for the string-key build, so each bucket persists its keys without contending
    /// on `SetVariants::string_pool`. Lazily built in phase B; single-writer per bucket, so the arena-backed
    /// `std::string_view` keys never dangle. They must outlive the set, which is why a set built this way
    /// cannot be handed over to a `KeyExtractor`.
    std::array<std::unique_ptr<Arena>, two_level_num_fine_buckets> bucket_arenas;

    /// Declared after `pool`, so that it is destroyed - which stops and joins the workers - before the
    /// pool they run on.
    std::unique_ptr<PhasedWorkers> workers;
};

DistinctSetFilter::DistinctSetFilter(
    const Block & header, const Names & columns, const SizeLimits & set_size_limits_, bool skip_null_keys_)
    : key_columns_pos(calculateDistinctKeyColumnsPositions(header, columns))
    , data(std::make_unique<SetVariants>())
    , set_size_limits(set_size_limits_)
    , skip_null_keys(skip_null_keys_)
{
    key_types.reserve(key_columns_pos.size());
    for (const auto pos : key_columns_pos)
        key_types.push_back(header.getByPosition(pos).type);

    if (skip_null_keys)
    {
        /// A constant `NULL` key component is not a key column (constants are excluded above), but it makes
        /// every key contain a `NULL`, so with the skipping enabled nothing can be emitted at all.
        const size_t num_columns = columns.empty() ? header.columns() : columns.size();
        for (size_t i = 0; i < num_columns; ++i)
        {
            const auto pos = columns.empty() ? i : header.getPositionByName(columns[i]);
            const auto & col = header.getByPosition(pos).column;
            if (col && isColumnConst(*col) && col->isNullAt(0))
                has_const_null_key = true;
        }
    }
}

DistinctSetFilter::~DistinctSetFilter() = default;

void DistinctSetFilter::enableTwoLevelParallelBuild(const DistinctTwoLevelBuildSettings & settings)
{
    if (settings.max_threads > 1)
        two_level_build = std::make_unique<TwoLevelBuild>(settings);
}

size_t DistinctSetFilter::getTotalRowCount() const
{
    return data->getTotalRowCount();
}

size_t DistinctSetFilter::getTotalByteCount() const
{
    /// The per-bucket string arenas of the two-level parallel build live outside `SetVariants::string_pool`,
    /// so `SetVariants::getTotalByteCount` does not see them; without them a two-level string `DISTINCT`
    /// would undercount and could slip past `max_bytes_in_distinct`.
    size_t bytes = data->getTotalByteCount() + lc_filter.getTotalByteCount();
    if (two_level_build)
        bytes += two_level_build->getArenasByteCount();
    return bytes;
}

/// Build the chunk's distinctness filter against a two-level set, parallelized by bucket. Two barriers:
/// A. Each worker scans its own row slice once, appending `(row, hash)` into its own per-bucket buffers
///    (`local_rows`/`local_hashes[w * NUM_BUCKETS + b]`) - private, so no contention or prefix-sum pass.
/// B. One task per bucket emplaces every worker's slice for that bucket. Buckets are disjoint, so it is
///    lock-free; the key is re-derived from the row for strings (the non-string keys are read back from the
///    phase-A cache) and phase-A hashes are reused, prefetching ~16 entries ahead.
/// String keys (`KeyType == std::string_view`) still point into the transient chunk, so phase B copies the
/// bytes into this bucket's own arena before emplacing - the stored key outlives the chunk. One worker per
/// bucket means each arena is single-writer.
/// Called only from `filter` (one chunk at a time), so the scratch is never accessed concurrently.
template <typename Method>
void DistinctSetFilter::buildTwoLevelParallelFilter(
    Method & method, const ColumnRawPtrs & key_columns, IColumn::Filter & filter_values, const size_t rows)
{
    using BucketData = std::decay_t<decltype(method.data)>;
    constexpr size_t NUM_BUCKETS = BucketData::NUM_BUCKETS;
    static_assert(NUM_BUCKETS == two_level_num_fine_buckets);
    static_assert(NUM_BUCKETS <= 256);

    using KeyType = typename BucketData::key_type;

    auto & build = *two_level_build;

    /// `shouldBuildParallel` gates the dispatch on the same helper returning > 1, so a single-worker chunk
    /// never reaches here - it takes the cheaper serial path instead.
    const size_t num_workers = build.workerCount(rows);
    if (num_workers == 0 || rows == 0)
        return;

    /// The key cache is only populated for the trivially-copyable key families; string keys re-derive
    /// their (cheap) view in phase B and persist it through a per-bucket arena.
    constexpr bool cache_keys = !std::is_same_v<KeyType, std::string_view>;

    const size_t num_slots = num_workers * NUM_BUCKETS;
    if (build.local_rows.size() < num_slots)
    {
        build.local_rows.resize(num_slots);
        build.local_hashes.resize(num_slots);
        build.local_keys.resize(num_slots);
    }
    for (size_t slot = 0; slot < num_slots; ++slot)
    {
        build.local_rows[slot].clear();
        build.local_hashes[slot].clear();
        if constexpr (cache_keys)
            build.local_keys[slot].clear();
    }

    const auto worker_range = [rows, num_workers](size_t w)
    {
        const size_t per_worker = (rows + num_workers - 1) / num_workers;
        const size_t lo = std::min(w * per_worker, rows);
        const size_t hi = std::min(lo + per_worker, rows);
        return std::pair{lo, hi};
    };

    auto & workers = build.getWorkers();

    /// Phase A: hash + partition each worker's row-slice into its own per-bucket buffers.
    auto phase_a = [&](size_t w)
    {
        typename Method::State state(key_columns, key_sizes, nullptr);
        Arena unused_pool;
        PaddedPODArray<UInt32> * rows_buf = &build.local_rows[w * NUM_BUCKETS];
        PaddedPODArray<UInt64> * hash_buf = &build.local_hashes[w * NUM_BUCKETS];
        PaddedPODArray<char> * keys_buf = &build.local_keys[w * NUM_BUCKETS];
        const auto [lo, hi] = worker_range(w);
        for (size_t i = lo; i < hi; ++i)
        {
            auto key_holder = state.getKeyHolder(i, unused_pool);
            const auto & key = keyHolderGetKey(key_holder);
            const auto hash = method.data.hash(key);
            const auto bucket = method.data.getBucketFromHash(hash);
            rows_buf[bucket].push_back(static_cast<UInt32>(i));
            hash_buf[bucket].push_back(hash);
            if constexpr (cache_keys)
            {
                const char * key_bytes = reinterpret_cast<const char *>(&key);
                keys_buf[bucket].insert(key_bytes, key_bytes + sizeof(KeyType));
            }
        }
    };
    PhaseBodyOf<decltype(phase_a)> phase_a_body{phase_a};
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
            auto & arena_ptr = build.bucket_arenas[bucket];
            if (!arena_ptr)
                arena_ptr = std::make_unique<Arena>();
            bucket_arena = arena_ptr.get();
        }

        /// Only the string families re-derive the key here, and building a `Method::State` is not free:
        /// `HashMethodHashed` copies the key columns and fills a `FixedSizeKeySlices` cache in its
        /// constructor. The non-string families read the key back from the phase-A cache instead.
        std::optional<typename Method::State> state;
        [[maybe_unused]] Arena unused_pool;
        if constexpr (std::is_same_v<KeyType, std::string_view>)
            state.emplace(key_columns, key_sizes, nullptr);

        for (size_t w = 0; w < num_workers; ++w)
        {
            const auto & rows_buf = build.local_rows[w * NUM_BUCKETS + bucket];
            const auto & hash_buf = build.local_hashes[w * NUM_BUCKETS + bucket];
            [[maybe_unused]] const auto & keys_buf = build.local_keys[w * NUM_BUCKETS + bucket];
            const size_t n = rows_buf.size();
            for (size_t j = 0; j < n; ++j)
            {
                if (j + prefetch_dist < n)
                    impl.prefetchByHash(hash_buf[j + prefetch_dist]);

                const UInt32 row = rows_buf[j];
                bool inserted = false;
                if constexpr (std::is_same_v<KeyType, std::string_view>)
                {
                    /// `ArenaKeyHolder` copies the key into the bucket arena only when it is actually
                    /// inserted, so a duplicate row adds no bytes and the arena stays proportional to the
                    /// distinct keys, matching the serial path.
                    auto key_holder = state->getKeyHolder(row, unused_pool);
                    KeyType key = keyHolderGetKey(key_holder);
                    ArenaKeyHolder arena_key_holder{key, *bucket_arena};
                    impl.emplace(arena_key_holder, it, inserted, hash_buf[j]);
                }
                else
                {
                    /// Reuse the key computed in phase A instead of re-deriving it, so the `hashed` carrier
                    /// does not run its `hash128` over every key column a second time.
                    KeyType key{};
                    memcpy(&key, keys_buf.data() + j * sizeof(KeyType), sizeof(KeyType));
                    impl.emplace(key, it, inserted, hash_buf[j]);
                }
                filter_values[row] = inserted;
            }
        }
    };
    PhaseBodyOf<decltype(phase_b)> phase_b_body{phase_b};
    workers.runDispatch(phase_b_body, num_workers, NUM_BUCKETS);
}

void DistinctSetFilter::maybeConvertToTwoLevel(const ColumnRawPtrs & key_columns, size_t num_rows)
{
    /// Promote single-level -> two-level, which unlocks the per-bucket parallel build.
    ///
    /// The trigger is aligned to the single-level table's own growth. `HashTable::resize` rehashes every
    /// cell into the enlarged buffer, and building the two-level table rehashes every cell into the 256
    /// sub-tables - the same amount of work. So converting *instead of* resizing costs approximately
    /// nothing: the extra rehash is paid for by the resize it replaces. Converting at an arbitrary set size
    /// would instead add a full O(set size) rehash on top, which is pure overhead for a query whose set
    /// stops growing shortly afterwards.
    ///
    /// A single-level table grows once its element count passes half of its cell capacity
    /// (`HashTableGrower::maxFill`), so `rows_in_set + num_rows > cells / 2` says this chunk is about to
    /// push it over. `num_rows` over-estimates the insertions, since duplicate rows do not insert, so the
    /// conversion can fire one chunk early; that is harmless, because the rehash it pays still scales with
    /// the current set size and the resize is still skipped.
    ///
    /// `threshold` is a minimum set size, not the trigger: below it the set holds too little data to be
    /// worth spreading over 256 sub-tables. `threshold_bytes` stays an independent trigger - it fires early
    /// for expensive keys, where a set of few but long keys crosses it while the table itself is still
    /// small, and the parallel build already pays off. A threshold of 0 disables that trigger; both 0
    /// disables promotion entirely.
    if (!SetVariants::isConvertibleToTwoLevel(data->type))
        return;

    const auto & settings = two_level_build->settings;
    const size_t rows_in_set = data->getTotalRowCount();

    /// About to rehash anyway: fold the two-level split into the growth it replaces.
    bool convert = settings.threshold != 0
        && rows_in_set + num_rows >= settings.threshold
        && rows_in_set + num_rows > data->getBufferSizeInCells() / 2;

    if (!convert && settings.threshold_bytes != 0)
    {
        size_t projected_bytes = data->getTotalByteCount();
        for (const auto * column : key_columns)
            projected_bytes += column->byteSize();
        convert = projected_bytes >= settings.threshold_bytes;
    }

    if (convert)
    {
        data->convertToTwoLevel();
        ProfileEvents::increment(ProfileEvents::DistinctHashTablesInitializedAsTwoLevel);
    }
}

namespace
{

/// Keeps the set alive while a typed iterator materializes owning columns one batch at a time.
template <typename Method>
class KeyExtractorImpl final : public DistinctSetFilter::KeyExtractor
{
public:
    KeyExtractorImpl(
        const Method & method, std::unique_ptr<SetVariants> data_, DataTypes key_types_, Sizes key_sizes_)
        : data(std::move(data_))
        , key_types(std::move(key_types_))
        , key_sizes(std::move(key_sizes_))
        , position(method.data.begin())
        , end(method.data.end())
    {
        if constexpr (requires { Method::State::packedKeysOrder(key_sizes); })
            unpack_order = Method::State::packedKeysOrder(key_sizes);
    }

    MutableColumns next(size_t max_rows, size_t max_bytes) override
    {
        chassert(max_rows > 0);

        if (!data)
            return {};

        MutableColumns columns;
        std::vector<IColumn *> raw_columns;
        columns.reserve(key_types.size());
        raw_columns.reserve(key_types.size());
        for (const auto & key_type : key_types)
        {
            columns.push_back(key_type->createColumn());
            raw_columns.push_back(columns.back().get());
        }

        size_t rows = 0;
        while (position != end && rows < max_rows)
        {
            if constexpr (requires { Method::State::packedKeysOrder(key_sizes); })
                Method::insertKeyIntoColumns(
                    position->getValue(), raw_columns, key_sizes, unpack_order ? &*unpack_order : nullptr);
            else
                Method::insertKeyIntoColumns(position->getValue(), raw_columns, key_sizes);

            ++position;
            ++rows;

            if (max_bytes)
            {
                size_t bytes = 0;
                for (const auto & column : columns)
                    bytes += column->allocatedBytes();
                if (bytes >= max_bytes)
                    break;
            }
        }

        /// Returned columns own their values, including strings extracted from the arena.
        if (position == end)
            data.reset();

        return columns;
    }

private:
    std::unique_ptr<SetVariants> data;
    const DataTypes key_types;
    const Sizes key_sizes;
    typename Method::Data::const_iterator position;
    const typename Method::Data::const_iterator end;
    std::optional<Sizes> unpack_order;
};

}

DistinctKeyRepresentation DistinctSetFilter::getKeyRepresentation() const
{
    chassert(!data->empty());
    return data->type == SetVariants::Type::hashed || data->type == SetVariants::Type::hashed_two_level
        ? DistinctKeyRepresentation::Hash128
        : DistinctKeyRepresentation::Columns;
}

std::unique_ptr<DistinctSetFilter::KeyExtractor> DistinctSetFilter::extractKeys() &&
{
    chassert(!skip_null_keys);
    chassert(getTotalRowCount() > 0);

    if (getKeyRepresentation() == DistinctKeyRepresentation::Hash128)
        key_types = {std::make_shared<DataTypeUInt128>()};

    auto create_extractor = [this]<typename Method>(const Method & method) -> std::unique_ptr<KeyExtractor>
    {
        return std::make_unique<KeyExtractorImpl<Method>>(
            method, std::move(data), std::move(key_types), std::move(key_sizes));
    };

    switch (data->type)
    {
        case SetVariants::Type::EMPTY:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Keys cannot be extracted from an uninitialized DISTINCT set");

#define M(NAME) \
        case SetVariants::Type::NAME: \
            return create_extractor(*data->NAME);
        APPLY_FOR_SET_VARIANTS_SINGLE_LEVEL(M)
#undef M
        /// Only the two-level parallel build produces these, and its string keys live in arenas that the
        /// set does not own (see `enableTwoLevelParallelBuild`). Listed so the switch stays exhaustive.
#define M_TWO_LEVEL(NAME) case SetVariants::Type::NAME:
        APPLY_FOR_SET_VARIANTS_TWO_LEVEL(M_TWO_LEVEL)
#undef M_TWO_LEVEL
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Keys cannot be extracted from a two-level DISTINCT set");
    }

    UNREACHABLE();
}

ColumnRawPtrs DistinctSetFilter::getKeyColumns(const Columns & columns) const
{
    ColumnRawPtrs key_columns;
    key_columns.reserve(key_columns_pos.size());
    for (const auto pos : key_columns_pos)
        key_columns.push_back(columns[pos].get());
    return key_columns;
}

void DistinctSetFilter::initialize(const ColumnRawPtrs & key_columns)
{
    data->init(SetVariants::chooseMethod(key_columns, key_sizes));
}

void DistinctSetFilter::prepareForInsert(Chunk & chunk)
{
    chassert(hasKeyColumns());
    chassert(!skip_null_keys);

    materializeChunk(chunk);
    if (data->empty())
        initialize(getKeyColumns(chunk.getColumns()));
}

size_t DistinctSetFilter::estimateGrowthMemory(const Chunk & chunk) const
{
    size_t growth_memory = data->estimateGrowthMemory(getKeyColumns(chunk.getColumns()), chunk.getNumRows());
    if (key_columns_pos.size() == 1)
    {
        const size_t bitmap_growth = lc_filter.estimateGrowthMemory(*chunk.getColumns()[key_columns_pos.front()]);
        if (common::addOverflow(growth_memory, bitmap_growth, growth_memory))
            return std::numeric_limits<size_t>::max();
    }
    return growth_memory;
}

size_t DistinctSetFilter::estimateFilteringMemory(const Chunk & chunk) const
{
    chassert(!skip_null_keys);

    /// The output filter and the optional `LowCardinality` mask can coexist with the copied columns.
    /// Round up both masks to cover the allocation rounding used when the latter is resized.
    const size_t mask_bytes = roundUpToPowerOfTwoOrZero(
        chunk.getNumRows() * sizeof(IColumn::Filter::value_type) + IColumn::Filter::pad_left + IColumn::Filter::pad_right);
    return chunk.allocatedBytes() + 2 * mask_bytes;
}

Chunk DistinctSetFilter::filter(Chunk chunk)
{
    /// The hash-set methods require materialized columns.
    materializeChunk(chunk);

    const auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    auto column_ptrs = getKeyColumns(columns);

    /// The consumer skips rows with a `NULL` in any key component, so they carry no value downstream.
    /// Instead of pre-filtering the chunk, the `NULL` rows are masked out of the deduplication: they are
    /// neither inserted into the set nor selected for the output, and they leave the chunk together with
    /// the duplicates in the single filtering at the end. `extractNestedColumnsAndNullMap` also replaces
    /// the nullable key pointers with their nested columns, so the keys are hashed by the nested values,
    /// the same way the set fill hashes them (the values at the masked rows are never read).
    ColumnPtr null_map_holder;

    /// Declared outside of the branch: the deduplication mask below may point at it.
    IColumn::Filter keep;
    if (skip_null_keys)
    {
        ConstNullMapPtr null_map = nullptr;
        null_map_holder = extractNestedColumnsAndNullMap(column_ptrs, null_map);

        if (null_map && !memoryIsZero(null_map->data(), 0, num_rows))
        {
            keep.resize(num_rows);
            for (size_t i = 0; i < num_rows; ++i)
                keep[i] = !(*null_map)[i];
        }

        /// `LowCardinality(Nullable)` keys are not unwrapped by `extractNestedColumnsAndNullMap`: their
        /// `NULL` rows are the rows referencing the dictionary's `NULL` entry.
        for (const auto * column : column_ptrs)
            if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(column);
                low_cardinality && low_cardinality->nestedIsNullable())
                markLowCardinalityNullRows(*low_cardinality, keep, num_rows);
    }

    std::optional<IColumn::Filter> lc_mask;

    if (key_columns_pos.size() == 1)
    {
        lc_mask = lc_filter.buildMaskIfApplicable(*column_ptrs[0], num_rows);

        /// An empty mask means that this chunk contains no candidate rows.
        if (lc_mask && lc_mask->empty())
            return {};
    }

    /// The `NULL`-key rows and the rows that are known duplicates by their `LowCardinality` index are
    /// masked out of the deduplication the same way.
    const IColumn::Filter * mask = nullptr;
    if (lc_mask && !keep.empty())
    {
        for (size_t i = 0; i < num_rows; ++i)
            (*lc_mask)[i] &= keep[i];
        mask = &*lc_mask;
    }
    else if (lc_mask)
        mask = &*lc_mask;
    else if (!keep.empty())
        mask = &keep;

    if (data->empty())
        initialize(column_ptrs);

    /// Promotion to two-level exists only to unlock the parallel build, so it is gated on exactly the
    /// condition the dispatch below uses to choose that build: a chunk too small to keep two workers busy,
    /// or a mask (the `LowCardinality` first-occurrence mask or the `NULL`-key mask, which only the serial
    /// build consumes), would leave a promoted set paying the conversion and never using what it bought.
    /// Probing in parallel with a `LowCardinality` mask would also re-hash every duplicate row and turn the
    /// O(dictionary size) fast path back into O(rows).
    const bool build_parallel = two_level_build && !mask && two_level_build->shouldBuildParallel(num_rows);
    if (build_parallel)
        maybeConvertToTwoLevel(column_ptrs, num_rows);

    const auto old_set_size = data->getTotalRowCount();
    IColumn::Filter filter_values(num_rows);

    switch (data->type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case SetVariants::Type::NAME: \
            buildDistinctFilter(*data->NAME, column_ptrs, key_sizes, filter_values, num_rows, *data, mask); \
        break;
        APPLY_FOR_SET_VARIANTS_SINGLE_LEVEL(M)
#undef M
        /// Two-level families: parallel build when the chunk is large enough, else serial.
#define M_TWO_LEVEL(NAME) \
        case SetVariants::Type::NAME: \
            if (build_parallel) \
            { \
                ProfileEvents::increment(ProfileEvents::DistinctTwoLevelParallelFilterBuilds); \
                buildTwoLevelParallelFilter(*data->NAME, column_ptrs, filter_values, num_rows); \
            } \
            else \
            { \
                ProfileEvents::increment(ProfileEvents::DistinctTwoLevelSerialFilterBuilds); \
                buildDistinctFilter(*data->NAME, column_ptrs, key_sizes, filter_values, num_rows, *data, mask); \
            } \
            break;
        APPLY_FOR_SET_VARIANTS_TWO_LEVEL(M_TWO_LEVEL)
#undef M_TWO_LEVEL
    }

    const auto new_set_size = data->getTotalRowCount();
    const size_t num_selected = new_set_size - old_set_size;

    /// A `LowCardinality` dictionary can grow the retained bitmap memory without adding new keys. With the
    /// 'throw' overflow mode `check` throws; with 'break' it returns false: the limit is recorded (see
    /// `isLimitReached`), but the new rows of the current chunk are still returned - their keys are already
    /// in the set, and 'break' means return a partial result as if the source data ran out, not discard it.
    if (!set_size_limits.check(new_set_size, getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        limit_reached = true;

    if (num_selected == 0)
        return {};

    /// When every row is a new distinct value, the columns are kept unchanged, without copying.
    if (num_selected != num_rows)
    {
        for (auto & column : columns)
            column = column->filter(filter_values, num_selected);
    }

    chunk.setColumns(std::move(columns), num_selected);
    return chunk;
}

}
