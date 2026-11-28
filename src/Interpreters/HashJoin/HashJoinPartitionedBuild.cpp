#include <Interpreters/HashJoin/HashJoinPartitionedBuild.h>

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoinPartitionedBuildKeys.h>
#include <base/getL1CacheSize.h>
#include <base/getL2CacheSize.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <mutex>
#include <tuple>
#include <utility>

namespace CurrentMetrics
{
extern const Metric HashJoinBuildThreads;
extern const Metric HashJoinBuildThreadsActive;
extern const Metric HashJoinBuildThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace JoinPartitionedBuild
{

namespace
{

using detail::KeyKind;

/// The skip bytes of the rows of the key columns: 1 for a row with a NULL key, a row that the `ON` mask filters out, and a
/// row that `selector` does not list (such as a row with a NULL `ASOF` value). Returns nullptr when no row is skipped.
/// Sets `skipped_null_key` when a listed row has a NULL key.
const UInt8 * buildSkip(
    const WaitingBlockRecord::Clause & clause,
    const ScatteredBlock::Selector & selector,
    size_t num_rows,
    PaddedPODArray<UInt8> & skip,
    bool & skipped_null_key)
{
    IColumn::Filter buffer;
    const UInt8 * base = buildRowSkipData(clause.null_map, clause.join_mask, buffer, 0, num_rows);
    const bool all_listed = selector.isContinuousRange() && selector.getRange() == ScatteredBlock::Selector::Range{0, num_rows};

    skipped_null_key = false;
    if (clause.null_map)
    {
        const UInt8 * null_map = clause.null_map->data();
        UInt8 any_null = 0;
        if (all_listed)
            for (size_t i = 0; i < num_rows; ++i)
                any_null |= null_map[i];
        else
            for (const size_t row : selector)
                any_null |= null_map[row];
        skipped_null_key = any_null != 0;
    }

    if (all_listed && !base)
        return nullptr;

    skip.resize(num_rows);
    if (all_listed)
        memcpy(skip.data(), base, num_rows);
    else
    {
        std::fill(skip.begin(), skip.end(), UInt8(1));
        for (const size_t row : selector)
            skip[row] = base ? base[row] : 0;
    }

    UInt8 any = 0;
    for (size_t i = 0; i < num_rows; ++i)
        any |= skip[i];
    if (!any)
    {
        skip.clear();
        return nullptr;
    }
    return skip.data();
}

/// Calls `row(i)` for each row that is inserted, `skipped(i)` for the others. Two loops, so that the usual case, with no
/// skipped row, has no test per row.
template <typename Row, typename Skipped>
ALWAYS_INLINE void forRows(size_t num_rows, const UInt8 * skip, Row && row, Skipped && skipped)
{
    if (skip)
    {
        for (size_t i = 0; i < num_rows; ++i)
        {
            if (skip[i])
                skipped(i);
            else
                row(i);
        }
    }
    else
    {
        for (size_t i = 0; i < num_rows; ++i)
            row(i);
    }
}

/// Hashes the key of each row with the map's own hash and calls `sink(i, h)` for the rows that are inserted and
/// `skipped(i)` for the others. `key128` receives the keys of `hashed` and packed `String` maps.
template <HashJoin::Type TYPE, typename Maps, typename Sink, typename Skipped>
void hashRows(
    const HashJoinTable & table,
    const WaitingBlockRecord::Clause & clause,
    const ScatteredBlock::Selector & selector,
    size_t num_rows,
    const UInt8 * skip,
    PaddedPODArray<UInt128> & key128,
    Sink && sink,
    Skipped && skipped)
{
    constexpr KeyKind kind = detail::keyKindOf<TYPE>();
    constexpr bool is_asof_join = std::is_same_v<Maps, HashJoin::MapsAsof>;
    using Map = JoinPartMapOf<TYPE, Maps>;

    if constexpr (kind == KeyKind::Number)
    {
        using Key = typename Map::key_type;
        const auto * keys = reinterpret_cast<const Key *>(clause.key_columns.at(0)->getRawData().data());
        const detail::MapHash<Map> hash{};
        forRows(num_rows, skip, [&](size_t i) { sink(i, hash(keys[i])); }, skipped);
    }
    else if constexpr (kind == KeyKind::Prepared)
    {
        using KeyGetter = typename KeyGetterForType<TYPE, Map>::Type;
        KeyGetter key_getter = createJoinKeyGetter<KeyGetter, is_asof_join>(clause.key_columns, table.keySizes());
        key_getter.buildAllKeys(selector, key128);
        const UInt128 * keys = key128.data();
        const detail::MapHash<Map> hash{};
        forRows(num_rows, skip, [&](size_t i) { sink(i, hash(keys[i])); }, skipped);
    }
    else if constexpr (kind == KeyKind::Packed || kind == KeyKind::String)
    {
        using KeyGetter = typename KeyGetterForType<TYPE, Map>::Type;
        KeyGetter key_getter = createJoinKeyGetter<KeyGetter, is_asof_join>(clause.key_columns, table.keySizes());
        /// The key holders are hashed and dropped, so the pool stays empty.
        Arena pool;
        const detail::MapHash<Map> hash{};
        forRows(
            num_rows,
            skip,
            [&](size_t i)
            {
                auto key_holder = key_getter.getKeyHolder(i, pool);
                sink(i, hash(keyHolderGetKey(key_holder)));
            },
            skipped);
    }
    else
        static_assert(kind != KeyKind::None, "Only the fill pass of a partitioned key type hashes rows");
}

template <HashJoin::Type TYPE, typename Maps>
void fillTyped(const HashJoinTable & table, WaitingBlockRecord::Clause & clause, const ScatteredBlock::Selector & selector,
               JoinKeySketch & sketch)
{
    constexpr KeyKind kind = detail::keyKindOf<TYPE>();
    const size_t num_rows = clause.key_columns.at(0)->size();
    const UInt8 * skip = buildSkip(clause, selector, num_rows, clause.skip, clause.skipped_null_key);

    clause.route.resize(num_rows);
    UInt16 * route = clause.route.data();
    const auto skipped = [&](size_t i) { route[i] = 0; };

    UInt64 * hash64 = nullptr;
    if constexpr (kind == KeyKind::String)
    {
        clause.hash64.resize(num_rows);
        hash64 = clause.hash64.data();
    }
    hashRows<TYPE, Maps>(table, clause, selector, num_rows, skip, clause.key128,
        [&](size_t i, UInt64 h)
        {
            route[i] = static_cast<UInt16>(static_cast<UInt32>(h) >> 16);
            /// The owner's emplace takes the saved hash, so a string is hashed once.
            if constexpr (kind == KeyKind::String)
                hash64[i] = h;
            sketch.insert(static_cast<UInt32>(h));
        },
        skipped);
}

template <HashJoin::Type TYPE, typename Maps>
void sketchTyped(const HashJoinTable & table, const WaitingBlockRecord::Clause & clause, const ScatteredBlock::Selector & selector,
                 JoinKeySketch & sketch)
{
    constexpr KeyKind kind = detail::keyKindOf<TYPE>();
    const size_t num_rows = clause.key_columns.at(0)->size();
    PaddedPODArray<UInt8> skip_buffer;
    bool skipped_null_key = false;
    const UInt8 * skip = buildSkip(clause, selector, num_rows, skip_buffer, skipped_null_key);
    const auto skipped = [](size_t) {};

    if constexpr (TYPE == HashJoin::Type::low_cardinality_key_string || TYPE == HashJoin::Type::low_cardinality_key_fixed_string)
    {
        /// The `LowCardinality` maps hold the values with the hash of the `String` map.
        const detail::MapHash<JoinPartMapOf<HashJoin::Type::key_string, Maps>> hash{};
        const IColumn & column = *clause.key_columns.at(0);
        forRows(num_rows, skip, [&](size_t i) { sketch.insert(static_cast<UInt32>(hash(column.getDataAt(i)))); }, skipped);
    }
    else if constexpr (kind == KeyKind::None)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The join key type {} has no key sketch", static_cast<int>(TYPE));
    else
    {
        PaddedPODArray<UInt128> key128;
        hashRows<TYPE, Maps>(table, clause, selector, num_rows, skip, key128,
            [&](size_t, UInt64 h) { sketch.insert(static_cast<UInt32>(h)); }, skipped);
    }
}

template <HashJoin::Type TYPE, typename Maps>
size_t countPartitionKeys(const HashJoinTable & table, const HashJoinBuildState & state, size_t clause_idx, size_t partitions)
{
    constexpr KeyKind kind = detail::keyKindOf<TYPE>();
    using Map = JoinPartMapOf<TYPE, Maps>;
    using Key = typename Map::key_type;
    constexpr bool saved_key = kind == KeyKind::Number || kind == KeyKind::Prepared;
    VectorWithMemoryTracking<size_t> counts(partitions);
    [[maybe_unused]] UInt64 min_key = std::numeric_limits<UInt64>::max(), max_key = 0;
    /// A repeated full key in a partition adds no cells, including across block boundaries.
    VectorWithMemoryTracking<Key> last(saved_key ? partitions : 0);
    const auto route_shift = 16 - std::countr_zero(partitions);
    const bool saved_routes = state.partitioned.load(std::memory_order_relaxed);
    PaddedPODArray<UInt8> skip_buffer;
    PaddedPODArray<UInt128> prepared;
    for (const auto & stream : state.streams)
    {
        for (const auto & record : stream.waiting)
        {
            const auto & clause = record.clauses[clause_idx];
            const auto & column = *clause.key_columns.front();
            [[maybe_unused]] const Key * numbers = nullptr;
            if constexpr (kind == KeyKind::Number)
                numbers = reinterpret_cast<const Key *>(column.getRawData().data());
            const auto count = [&](size_t i, size_t p)
            {
                if constexpr (saved_key)
                {
                    Key key;
                    if constexpr (kind == KeyKind::Number)
                        key = numbers[i];
                    else
                        key = (saved_routes ? clause.key128 : prepared)[i];
                    if (key == Key{} || last[p] == key)
                        return;
                    last[p] = key;
                    if constexpr (kind == KeyKind::Number)
                    {
                        min_key = std::min(min_key, UInt64{key});
                        max_key = std::max(max_key, UInt64{key});
                    }
                }
                ++counts[p];
            };
            if (saved_routes)
            {
                const UInt8 * skip = clause.skip.empty() ? nullptr : clause.skip.data();
                forRows(clause.route.size(), skip, [&](size_t i) { count(i, clause.route[i] >> route_shift); }, [](size_t) {});
            }
            else
            {
                const auto & selector = record.stored->selector;
                bool skipped_null_key = false;
                const UInt8 * skip = buildSkip(clause, selector, column.size(), skip_buffer, skipped_null_key);
                hashRows<TYPE, Maps>(
                    table, clause, selector, column.size(), skip, prepared,
                    [&](size_t i, UInt64 hash) { count(i, static_cast<UInt32>(hash) >> (route_shift + 16)); }, [](size_t) {});
            }
        }
    }
    if constexpr (kind == KeyKind::Number)
    {
        /// Interleaved repetitions can span many blocks. Distinct integers cannot exceed their nonzero key range.
        const UInt64 span = min_key > max_key ? 0 : max_key - min_key + 1;
        for (auto & count : counts)
            count = std::min(count, static_cast<size_t>(span));
    }
    if (table.publishedPartitionCapacity())
        table.addPartitionNonzeroCounts(counts);
    return *std::ranges::max_element(counts);
}

}

void fillClause(const HashJoinTable & table, bool is_asof, WaitingBlockRecord::Clause & clause,
                const ScatteredBlock::Selector & selector, JoinKeySketch & sketch)
{
    if (is_asof != table.holds<HashJoin::MapsAsof>())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The fill pass of an ASOF join got a table of other maps, or the reverse");
    detail::dispatchMaps(table, [&]<typename Maps>()
    {
        table.dispatchType([&]<HashJoin::Type TYPE>()
        {
            if constexpr (detail::keyKindOf<TYPE>() == KeyKind::None)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The join key type {} is not built in partitions", static_cast<int>(TYPE));
            else
                fillTyped<TYPE, Maps>(table, clause, selector, sketch);
        });
    });
}

void sketchClause(const HashJoinTable & table, bool is_asof, const WaitingBlockRecord::Clause & clause,
                  const ScatteredBlock::Selector & selector, JoinKeySketch & sketch)
{
    if (is_asof != table.holds<HashJoin::MapsAsof>())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The key sketch of an ASOF join got a table of other maps, or the reverse");
    detail::dispatchMaps(table, [&]<typename Maps>()
    {
        table.dispatchType([&]<HashJoin::Type TYPE>() { sketchTyped<TYPE, Maps>(table, clause, selector, sketch); });
    });
}

size_t maxPartitionKeysAtFinish(const HashJoinTable & table, const HashJoinBuildState & state, size_t clause_idx, size_t partitions)
{
    return detail::dispatchMaps(table, [&]<typename Maps>()
    {
        return table.dispatchType([&]<HashJoin::Type TYPE>() -> size_t
        {
            if constexpr (detail::keyKindOf<TYPE>() == KeyKind::None)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The join key type {} is not built in partitions", static_cast<int>(TYPE));
            else
                return countPartitionKeys<TYPE, Maps>(table, state, clause_idx, partitions);
        });
    });
}

void runWorkers(size_t num_workers, std::atomic<bool> & stop, absl::FunctionRef<void(size_t)> work)
{
    if (num_workers <= 1)
    {
        work(0);
        return;
    }

    std::mutex exception_mutex;
    std::exception_ptr first_exception;
    auto guarded = [&](size_t worker)
    {
        try
        {
            work(worker);
        }
        catch (...)
        {
            stop.store(true, std::memory_order_relaxed);
            std::lock_guard lock(exception_mutex);
            if (!first_exception)
                first_exception = std::current_exception();
        }
    };

    {
        /// At build finish every filling thread but this one has finished, so the build takes threads of its own, outside
        /// `ConcurrencyControl`, as the parallel free of the right table does.
        ThreadPool pool(
            CurrentMetrics::HashJoinBuildThreads,
            CurrentMetrics::HashJoinBuildThreadsActive,
            CurrentMetrics::HashJoinBuildThreadsScheduled,
            num_workers - 1);
        auto thread_group = CurrentThread::getGroup();
        for (size_t worker = 1; worker < num_workers; ++worker)
            pool.scheduleOrThrowOnError(
                [&guarded, worker, thread_group]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::HASH_JOIN_BUILD);
                    guarded(worker);
                });
        guarded(0);
        pool.wait();
    }

    if (first_exception)
        std::rethrow_exception(first_exception);
}
}

size_t maxJoinPartitions()
{
    /// Keep the partition cap used with the original 16-byte descriptors.
    const size_t l1_bytes = std::max<size_t>(getL1CacheSize(), 32 << 10);
    const size_t partitions = std::bit_floor(l1_bytes / 4 / 16);
    /// Partition ids are `UInt16`, and the scatter takes id P for the rows that are not inserted.
    return std::min<size_t>(partitions, size_t(1) << 15);
}

JoinPartitionPlan choosePartitionPlan(
    size_t estimate,
    const char * estimate_source,
    size_t waiting_rows,
    size_t num_build_streams,
    size_t parallel_hash_join_threshold,
    bool early,
    bool robin_hood_setting,
    size_t (*buffer_bytes_for_reserve)(size_t keys, bool robin_hood))
{
    const size_t cap = maxJoinPartitions();
    JoinPartitionPlan plan;
    plan.estimate = estimate;
    plan.estimate_source = estimate_source;

    /// Routing and cell indexing share the low 32 bits. A larger estimate cannot increase physical capacity.
    estimate = std::min(estimate, size_t{1} << 32);

    /// Allow a 20% margin when choosing the partition count. Linear probing allocates from the mean key count below.
    const auto reserve_for = [&](size_t partitions) { return (estimate * 6 + 5 * partitions - 1) / (5 * partitions); };
    /// A Robin Hood table fills up to 0.9 of its cells, a power of two, so a reserve of 1.2 times the share often doubles the
    /// cells, and the memory that the mode saves. Its reserve is the most keys that the buffer for 1/16 more than its share
    /// holds, at most the reserve above: the room up to that fill is its margin.
    const auto robin_hood_reserve_for = [&](size_t partitions)
    {
        const size_t share = (estimate + partitions - 1) / partitions;
        size_t lo = share + share / 16;
        size_t hi = reserve_for(partitions);
        const size_t bytes = buffer_bytes_for_reserve(lo, /*robin_hood=*/true);
        if (lo >= hi || buffer_bytes_for_reserve(hi, /*robin_hood=*/true) == bytes)
            return hi;
        while (lo + 1 < hi)
        {
            const size_t mid = lo + (hi - lo) / 2;
            if (buffer_bytes_for_reserve(mid, /*robin_hood=*/true) == bytes)
                lo = mid;
            else
                hi = mid;
        }
        return lo;
    };
    /// The reserve of a table for `partitions` tables, and its mode. A reserve of more than 2^15 keys gives the grower 2^16
    /// cells or more, where it switches to the Robin Hood mode.
    const auto sizing_for = [&](size_t partitions) -> std::pair<size_t, bool>
    {
        if (robin_hood_setting)
            if (const size_t r = robin_hood_reserve_for(partitions); r > 32768)
                return {r, true};
        return {reserve_for(partitions), false};
    };

    const auto choose_partitions = [&]
    {
        if (early)
            return std::min(std::max<size_t>(256, std::bit_ceil(2 * std::max<size_t>(num_build_streams, 1))), cap);

        /// The fewest partitions whose table buffer for its reserve, in its mode, fits 0.8 of the L2. The rest of the L2 holds the
        /// piece that the owner reads and its arena.
        const size_t l2_share = std::max<size_t>(getL2CacheSize(), 1 << 20) * 4 / 5;
        size_t bits = 0;
        while (bits < 15)
        {
            const auto [r, robin_hood] = sizing_for(size_t(1) << bits);
            if (buffer_bytes_for_reserve(r, robin_hood) <= l2_share)
                break;
            ++bits;
        }

        const size_t streams = std::bit_ceil(std::max<size_t>(num_build_streams, 1));
        size_t partitions = size_t(1) << bits;
        if (bits > 0)
            /// Every build stream gets a partition to fill.
            partitions = std::max(partitions, streams);
        else if (waiting_rows >= parallel_hash_join_threshold)
            /// Few keys and many rows: without this floor, one thread would insert all the rows.
            partitions = std::min(streams, std::bit_ceil(std::max<size_t>(estimate, 1)));
        /// At least two partitions: the parallel layout never has a single table.
        partitions = std::min(std::max<size_t>(partitions, 2), cap);

        return partitions;
    };

    const size_t partitions = choose_partitions();
    plan.partition_bits = std::countr_zero(partitions);
    const auto [reserve, robin_hood] = sizing_for(partitions);
    HashJoin::JoinPartitionGrower grower;
    grower.setRobinHoodAllowed(robin_hood);
    grower.set(robin_hood ? reserve : (estimate + partitions - 1) / partitions);
    plan.robin_hood = grower.isRobinHood();
    plan.cells_per_partition = std::min(grower.bufSize(), (size_t{1} << 32) / partitions);
    return plan;
}

}
