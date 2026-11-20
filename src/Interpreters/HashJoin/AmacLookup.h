#pragma once

#include <Interpreters/HashJoin/AmacRing.h>
#include <Interpreters/HashJoin/LookupMethod.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <type_traits>
#include <utility>

/// The AMAC lookup method of the hash join probe: `amacRun` (`AmacRing.h`) drives a find policy that walks the cells of the
/// `TwoLevelHashTable` maps of the join.
namespace DB::JoinProbe
{

/// Whether `Cell` saves the hash of its key. The string-key cells do, and compare the saved hash before
/// the key. Recomputing a string hash at every visit is expensive, so a ring over such cells keeps the
/// hash in its slots. Every other cell ignores the hash argument of `keyEquals`, so its ring does not
/// store the hash.
template <typename Cell>
constexpr bool cell_stores_hash = requires(const Cell & cell) { cell.saved_hash; };

/// What `on_row` gets from the ring for a row whose key is in `cell`. A found and a missing key have two types, so that
/// `isFound` is a constant where `on_row` tests it.
template <typename Cell>
struct AmacFound
{
    const Cell * cell;
    size_t offset;

    static constexpr bool isFound() { return true; }
    [[nodiscard]] decltype(auto) getMapped() const { return cell->getMapped(); }
    [[nodiscard]] size_t getOffset() const { return offset; }
};

/// What `on_row` gets from the ring for a row whose key is not in the map, is NULL, or is masked out.
template <typename Cell>
struct AmacNotFound
{
    static constexpr bool isFound() { return false; }
    [[nodiscard]] decltype(std::declval<const Cell &>().getMapped()) getMapped() const { UNREACHABLE(); }
    static constexpr size_t getOffset() { return 0; }
};

/// The order in which the ring calls `on_row`.
enum class AmacRetire : UInt8
{
    /// As each lookup finishes.
    OutOfOrder,
    /// In row order. A finished lookup waits in a reorder window until every earlier row has retired (`amac_reorder_window`).
    /// The window keeps no key offset: `getOffset` of a found key gives 0.
    InOrder,
    /// In row order, for a join with a used flag per key. The window keeps the key offsets. The flag of a found key is
    /// prefetched when the key is found, and the row retires only after `amac_flag_lag_lookups` more lookups have finished,
    /// so that the flag is in the cache by then.
    InOrderWithFlags,
};

/// With `AmacRetire::InOrderWithFlags`: how many lookups finish after a found key before its row retires. The oldest row of
/// the window usually retires as soon as it finishes, so without the wait its flag would have no time to arrive.
constexpr UInt32 amac_flag_lag_lookups = 16;

/** The policy that `amacRun` drives to find keys in a `TwoLevelHashTable` map of the join. The lookups finish out of order.
  * A visit learns the result of its row when it reads its key's cell or learns that the key is missing, so nothing reads the
  * cell again. Out of order, the visit calls `on_row` then. In order, it puts the result into the window, and `on_row` sees
  * the rows when they retire, in row order.
  *
  * With one bucket, a visit wraps at the bounds that the policy holds in its frame. With several buckets, a slot also has the
  * index of its bucket, and a visit reads the bounds from `buckets`. With `robin_hood`, a slot also has the distance that it
  * has walked. `row_list` is a template parameter, so `indexAt` has no branch.
  */
template <typename KeyGetter, typename Map, bool robin_hood, bool fast_path, bool row_list, AmacRetire retire, typename OnRow>
struct AmacFindPolicy
{
    static constexpr bool in_order = retire != AmacRetire::OutOfOrder;
    static constexpr bool with_flags = retire == AmacRetire::InOrderWithFlags;
    using Cell = typename Map::cell_type;
    using Bucket = typename Map::BucketCells;
    static constexpr bool one_bucket = Map::NUM_BUCKETS == 1;
    static constexpr bool store_hash = cell_stores_hash<Cell>;
    static constexpr HashTableNoState no_state{};
    /// `AmacLookup::run` splits the rows into chunks of at most this many rows, so that the row index in a slot fits in 16
    /// bits. Any size up to 65536 would do.
    static constexpr size_t chunk_rows_max = 1uz << 13;
    static_assert(Map::NUM_BUCKETS <= 256);
    static_assert(!robin_hood || !one_bucket, "Only the buckets of a map of several buckets switch to the Robin Hood mode");

    /// A slot can keep a string key, a view into the key column, because the string holder copies a key into the arena only
    /// for an insert.
    using StoredKey = KeyOfGetter<KeyGetter>;
    static_assert(std::is_trivially_copyable_v<StoredKey>);
    /// For a key type that builds its keys, `run` gives the policy the keys that the prepare step built for the chunk, in
    /// `ready_keys`. Then `start` only loads a key and calls nothing, and the policy needs no key getter (`NoKeyGetter`).
    static constexpr bool reads_ready_keys = BuildsKeys<KeyGetter>;
    struct NoKeyGetter
    {
    };

    /** The state of the ring. A slot is inactive when its `cell` is null, so a value-initialized ring has no active slot and
      * `row` needs no reserved value. A slot does not store the offset of its cell: `hit` computes it from the cell pointer.
      * The key is stored in the slot, so that a visit does not fetch it through the row again (`keyHolderAt`).
      */
    struct RingBase
    {
        std::array<const Cell *, amac_ring_size> cell{}; /// the cell the next visit reads
        std::array<UInt16, amac_ring_size> row{};        /// chunk-local probe row
        alignas(64) std::array<StoredKey, amac_ring_size> key{};

        [[nodiscard]] bool isActive(size_t s) const { return cell[s] != nullptr; }
        void deactivate(size_t s) { cell[s] = nullptr; }
    };
    struct RingWithHash : public RingBase
    {
        std::array<size_t, amac_ring_size> hash{};
    };
    using TableRing = std::conditional_t<store_hash, RingWithHash, RingBase>;

    /// A map of several buckets: the bucket that the slot walks, an index into `buckets`.
    struct BucketRing : public TableRing
    {
        std::array<UInt8, amac_ring_size> bucket{};
    };

    /// Buckets in the Robin Hood mode: how many cells the slot has walked from the home cell of its key.
    struct RobinHoodRing : public BucketRing
    {
        std::array<UInt32, amac_ring_size> distance{};
    };

    using SlotRing = std::conditional_t<
        one_bucket,
        TableRing,
        std::conditional_t<robin_hood, RobinHoodRing, BucketRing>>;

    /// In order: the finished rows that wait for an earlier row. Entry `i % amac_reorder_window` belongs to row `i`: 0 until
    /// the row finishes, then 1 for a missing key, or the address of the key's cell with bit 0 set.
    struct Window
    {
        std::array<uintptr_t, amac_reorder_window> done{};
        size_t retired = 0; /// the oldest chunk row that `on_row` has not seen
    };
    /// With flags: also the key offset of each row, and how many lookups had finished when the row finished.
    struct WindowWithFlags : public Window
    {
        std::array<size_t, amac_reorder_window> offset{};
        std::array<UInt32, amac_reorder_window> finished_at{};
        UInt32 finished = 0;
    };
    struct RingWithWindow : public SlotRing
    {
        std::conditional_t<with_flags, WindowWithFlags, Window> window;
    };
    using Ring = std::conditional_t<in_order, RingWithWindow, SlotRing>;
    static_assert(!in_order || alignof(Cell) >= 2, "Bit 0 of a cell address marks a found key");

    /// By value where possible. The key-column pointer is then a field of the frame-local policy. Behind a reference,
    /// reading it would take two dependent loads.
    using KeyGetterField = std::conditional_t<
        reads_ready_keys,
        NoKeyGetter,
        std::conditional_t<std::is_trivially_copyable_v<KeyGetter>, KeyGetter, KeyGetter &>>;
    KeyGetterField key_getter;
    const Map & map;
    /// `Map::bucket_cells`, which `computeBucketPrefix` fills when the table is frozen. For a map of one bucket, `table` is
    /// a copy of its only entry, so that a visit reads the bounds from the frame.
    const Bucket * buckets = nullptr;
    Bucket table{};
    size_t first = 0;              /// a range: the block row of the chunk's first row
    const UInt64 * list = nullptr; /// a row list: the block rows of the chunk
    size_t position_base = 0;      /// the position of the chunk's first row in `FindKeysArgs::rows`
    const UInt8 * skip_data = nullptr; /// indexed by the block row, not by the chunk; null on the fast path
    const StoredKey * ready_keys = nullptr; /// with `reads_ready_keys`: `[i]` is the key of the chunk's row `i`
    Arena & pool;
    OnRow on_row;
    const UInt8 * found_flags = nullptr; /// with `with_flags`: the used flags of the join, indexed by the key offset

    [[nodiscard]] ALWAYS_INLINE const Bucket & bucketAt(size_t bucket [[maybe_unused]]) const
    {
        if constexpr (one_bucket)
            return table;
        else
            return buckets[bucket];
    }

    [[nodiscard]] ALWAYS_INLINE const Bucket & bucketOf(const Ring & ring [[maybe_unused]], size_t s [[maybe_unused]]) const
    {
        if constexpr (one_bucket)
            return table;
        else
            return buckets[ring.bucket[s]];
    }

    /// What `Cell::getHash` gives: the hash saved in the cell, or the hash of its key.
    [[nodiscard]] ALWAYS_INLINE size_t hashOf(const Cell & cell) const
    {
        if constexpr (store_hash)
            return cell.saved_hash;
        else
            return map.hash(cell.getKey());
    }

    [[nodiscard]] ALWAYS_INLINE size_t indexAt(size_t i) const
    {
        if constexpr (row_list)
            return list[i];
        else
            return first + i;
    }

    ALWAYS_INLINE decltype(auto) keyHolderAt(size_t i [[maybe_unused]], size_t ind [[maybe_unused]])
    {
        if constexpr (reads_ready_keys)
            return ready_keys[i];
        else
            return key_getter.getKeyHolder(ind, pool);
    }

    ALWAYS_INLINE void miss(Ring & ring, size_t s)
    {
        const size_t i = ring.row[s];
        if constexpr (in_order)
            finish(ring, i, 1, 0);
        else
            on_row(position_base + i, indexAt(i), AmacNotFound<Cell>{});
    }

    /// The cell is not the zero cell, so its offset is its position over all buckets plus one, as `offsetInternal` gives.
    ALWAYS_INLINE void hit(Ring & ring, size_t s, const Cell * cell)
    {
        const size_t i = ring.row[s];
        if constexpr (retire == AmacRetire::InOrder)
        {
            finish(ring, i, reinterpret_cast<uintptr_t>(cell) | 1, 0);
            return;
        }
        const Bucket & bucket = bucketOf(ring, s);
        size_t offset = static_cast<size_t>(cell - bucket.cells) + 1;
        if constexpr (!one_bucket)
            offset += bucket.prefix;
        if constexpr (with_flags)
        {
            /// For reading: the join reads a flag first, and writes it only if no other row has claimed the key.
            __builtin_prefetch(found_flags + offset, /*read*/ 0, /*locality*/ 3);
            finish(ring, i, reinterpret_cast<uintptr_t>(cell) | 1, offset);
        }
        else
            on_row(position_base + i, indexAt(i), AmacFound<Cell>{cell, offset});
    }

    /// In order: puts the result of row `i` into the window, and retires the rows that are ready if `i` is the oldest. With
    /// flags, any finished lookup may let the oldest row retire, because it counts towards the wait of that row.
    ALWAYS_INLINE void finish(Ring & ring, size_t i, uintptr_t done, size_t offset)
    {
        if constexpr (with_flags)
            ++ring.window.finished;
        storeFinished(ring, i, done, offset);
        if (with_flags || i == ring.window.retired)
            retireReady(ring);
    }

    /// In order: a row that `start` answers at once only goes into the window. `mayStart`, a later `finish` or `finishRun`
    /// retires it.
    ALWAYS_INLINE static void storeFinished(Ring & ring, size_t i, uintptr_t done, size_t offset [[maybe_unused]])
    {
        const size_t w = i % amac_reorder_window;
        ring.window.done[w] = done;
        if constexpr (with_flags)
        {
            ring.window.offset[w] = offset;
            ring.window.finished_at[w] = ring.window.finished;
        }
    }

    /// In order: calls `on_row` for the oldest rows that have finished, in row order, up to the first that has not. With
    /// flags, also up to the first found key that waits for its flag, unless `all` is set.
    ALWAYS_INLINE void retireReady(Ring & ring, bool all = false)
    {
        auto & window = ring.window;
        size_t r = window.retired;
        while (true)
        {
            const size_t w = r % amac_reorder_window;
            const uintptr_t done = window.done[w];
            if (!done)
                break;
            size_t offset = 0;
            if constexpr (with_flags)
            {
                if (!all && done != 1 && window.finished - window.finished_at[w] < amac_flag_lag_lookups)
                    break;
                offset = window.offset[w];
            }
            window.done[w] = 0;
            if (done == 1)
                on_row(position_base + r, indexAt(r), AmacNotFound<Cell>{});
            else
                on_row(position_base + r, indexAt(r), AmacFound<Cell>{reinterpret_cast<const Cell *>(done - 1), offset});
            ++r;
        }
        window.retired = r;
    }

    /// In order, for `amacRun`: whether row `row` may start. If the window is full, the rows that are ready retire first,
    /// which includes the rows that `start` answered at once and the found keys that wait for their flags.
    ALWAYS_INLINE bool mayStart(Ring & ring, size_t row)
    {
        if (row - ring.window.retired < amac_reorder_window)
            return true;
        retireReady(ring, /*all=*/true);
        return row - ring.window.retired < amac_reorder_window;
    }

    /// In order, for `amacRun` after the last lookup finished: the rows that `start` answered at once and the found keys
    /// that wait for their flags may still be in the window.
    ALWAYS_INLINE void finishRun(Ring & ring, size_t rows [[maybe_unused]])
    {
        retireReady(ring, /*all=*/true);
        chassert(ring.window.retired == rows);
    }

    ALWAYS_INLINE bool start(Ring & ring, size_t s, size_t i)
    {
        const size_t ind = indexAt(i);
        if constexpr (!fast_path)
        {
            if (skip_data && skip_data[ind])
            {
                if constexpr (in_order)
                    storeFinished(ring, i, 1, 0);
                else
                    on_row(position_base + i, ind, AmacNotFound<Cell>{});
                return false;
            }
        }
        auto && key_holder = keyHolderAt(i, ind);
        const auto & key = keyHolderGetKey(key_holder);
        if (unlikely(Cell::isZero(key, no_state)))
        {
            /// A lookup of the zero key reads only the zero-value cell, so it has no walk to overlap with other lookups.
            const Cell * cell = map.find(key);
            if constexpr (in_order)
                storeFinished(ring, i, cell ? reinterpret_cast<uintptr_t>(cell) | 1 : 1, cell ? map.offsetInternal(cell) : 0);
            else if (cell)
                on_row(position_base + i, ind, AmacFound<Cell>{cell, map.offsetInternal(cell)});
            else
                on_row(position_base + i, ind, AmacNotFound<Cell>{});
            return false;
        }
        const size_t hash = map.hash(key);
        ring.key[s] = key;
        size_t bucket_no = 0;
        if constexpr (!one_bucket)
        {
            bucket_no = Map::getBucketFromHash(hash);
            ring.bucket[s] = static_cast<UInt8>(bucket_no);
        }
        if constexpr (robin_hood)
            ring.distance[s] = 0;
        const Bucket & bucket = bucketAt(bucket_no);
        const Cell * cell = bucket.cells + (hash & bucket.mask);
        ring.cell[s] = cell;
        ring.row[s] = static_cast<UInt16>(i);
        if constexpr (store_hash)
            ring.hash[s] = hash;
        prefetchCell(cell);
        return true;
    }

    static ALWAYS_INLINE void prefetchCell(const Cell * cell)
    {
        /// Locality 3, although a lookup reads a cell only once. On AArch64, locality 1 compiles to `prfm pldl3keep`, which
        /// brings the line only to L3, and the visit that reads the cell would still wait for it.
        __builtin_prefetch(cell, 0, 3);
        /// In an array of 40-byte cells, half of the cells cross a cache line boundary. So for a cell wider than 24 bytes, the
        /// last byte is prefetched too; otherwise the compare of the key would wait for the second line.
        if constexpr (sizeof(Cell) > 24)
            __builtin_prefetch(reinterpret_cast<const char *>(cell) + sizeof(Cell) - 1, 0, 3);
    }

    ALWAYS_INLINE AmacStepResult step(Ring & ring, size_t s)
    {
        const Cell * cell = ring.cell[s];
        if (cell->isZero(no_state))
        {
            miss(ring, s);
            return AmacStepResult::Done;
        }
        const StoredKey & key = ring.key[s];
        /// Only the saved-hash cells read `hash`; see `cell_stores_hash`.
        size_t hash = 0;
        if constexpr (store_hash)
            hash = ring.hash[s];
        if (cell->keyEquals(key, hash, no_state))
        {
            hit(ring, s, cell);
            return AmacStepResult::Done;
        }
        const Bucket & bucket = bucketOf(ring, s);
        if constexpr (robin_hood)
        {
            /// The miss rule of `HashTable::findCellRobinHood`. The keys of a chain are in the order of their home cells. So
            /// the key is not in the table if this cell's key is closer to its home than the key would be here. The rule
            /// does not apply in the key's home cell.
            const size_t place = static_cast<size_t>(cell - bucket.cells);
            const size_t distance = ring.distance[s];
            if (distance && ((place - hashOf(*cell)) & bucket.mask) < distance)
            {
                miss(ring, s);
                return AmacStepResult::Done;
            }
            ring.distance[s] = static_cast<UInt32>(distance + 1);
        }
        if (++cell == bucket.cells_end) [[unlikely]]
            cell = bucket.cells;
        ring.cell[s] = cell;
        prefetchCell(cell);
        return AmacStepResult::Advance;
    }
};

/// The `TwoLevelHashTable` maps of the join, with one bucket or several: `freezeForProbing` lists the cells of their
/// buckets. The direct-addressed maps (`key8`, `key16`, `range*`) have no collision chain to overlap.
template <typename Map>
concept AmacWalkableMap = requires(const Map & map)
{
    { map.bucket_cells.data() } -> std::same_as<const typename Map::BucketCells *>;
};

/// Whether the buckets of the map can switch to the Robin Hood mode.
template <typename Map>
constexpr bool amac_buckets_switch_to_robin_hood = false;
template <AmacWalkableMap Map>
constexpr bool amac_buckets_switch_to_robin_hood<Map> = GrowerWithRobinHood<typename Map::Impl::grower_type>;

/// Whether the ring can look up the keys of `KeyGetter` in `Map`. The LowCardinality getter stays on `PrefetchingLookup`: it
/// deduplicates lookups per dictionary index through its own cache, and a ring would bypass that cache. For the same reason,
/// the getter turns off the look-ahead prefetch of `PrefetchingLookup`.
template <typename KeyGetter, typename Map, bool robin_hood>
constexpr bool amac_probe_supported = AmacWalkableMap<Map> && (!robin_hood || amac_buckets_switch_to_robin_hood<Map>)
    && !is_low_cardinality_join_key_getter<KeyGetter>;

/// Looks the keys up with the AMAC ring. Rows retire in the order that `retire` gives. Only for a table with
/// `HashJoinTable::takesAmacLookup`, and `robin_hood` must equal `bucketsAreRobinHood` of the table. For a key type that
/// builds its keys (`BuildsKeys`), the caller must run the prepare step of the rows first (`PrepareKeysFn`).
template <bool robin_hood, AmacRetire retire = AmacRetire::OutOfOrder>
struct AmacLookup
{
    static constexpr bool retires_rows_in_order = retire != AmacRetire::OutOfOrder;

    template <HashJoin::Type TYPE, typename Maps>
    static constexpr bool supports = amac_probe_supported<
        typename KeyGetterForType<TYPE, const JoinMapOf<TYPE, Maps>>::Type, JoinMapOf<TYPE, Maps>, robin_hood>;

    /// No key getter for a key type whose keys the prepare step built. The other key getters that the ring supports hold
    /// no state for the block, so `keyGetterOf` builds them for each search.
    template <typename Policy, typename KeyGetter, bool is_asof_join>
    ALWAYS_INLINE static decltype(auto) keyGetterFor(const FindKeysArgs & args [[maybe_unused]])
    {
        if constexpr (Policy::reads_ready_keys)
            return typename Policy::NoKeyGetter{};
        else
            return keyGetterOf<KeyGetter, is_asof_join>(args);
    }

    /// With `AmacRetire::InOrderWithFlags`, `found_flags` are the used flags of the join, which the ring prefetches.
    template <HashJoin::Type TYPE, typename Maps, bool fast_path, bool row_list, typename OnRow>
    ALWAYS_INLINE static size_t run(const FindKeysArgs & args, OnRow && on_row, const UInt8 * found_flags = nullptr)
    {
        using Map = JoinMapOf<TYPE, Maps>;
        using Cell = typename Map::cell_type;
        using KeyGetter = typename KeyGetterForType<TYPE, const Map>::Type;
        using Policy = AmacFindPolicy<KeyGetter, Map, robin_hood, fast_path, row_list, retire, std::decay_t<OnRow>>;
        constexpr bool is_asof_join = std::is_same_v<Maps, HashJoin::MapsAsof>;
        static_assert(std::is_void_v<decltype(on_row(0uz, 0uz, AmacNotFound<Cell>{}))>, "The ring cannot stop at a row");

        chassert(row_list == (args.rows.rows != nullptr));
        chassert((retire == AmacRetire::InOrderWithFlags) == (found_flags != nullptr));
        const Map & map = args.table.mapOf<TYPE, Maps>();
        chassert(map.bucket_cells.size() == Map::NUM_BUCKETS, "The table was not frozen");
        chassert(map.robin_hood_buckets == (robin_hood ? Map::RobinHoodBuckets::All : Map::RobinHoodBuckets::None));
        decltype(auto) key_getter = keyGetterFor<Policy, KeyGetter, is_asof_join>(args);

        const size_t size = args.rows.size;
        const UInt8 * skip_data = nullptr;
        IColumn::Filter skip_buffer;
        if constexpr (!fast_path)
        {
            const auto row_of = [&](size_t i) -> size_t { return row_list ? args.rows.rows[i] : args.rows.first + i; };
            if (size)
                skip_data = args.keys.buildRowSkipData(skip_buffer, row_of(0), row_of(size - 1) + 1 - row_of(0));
        }
        Arena pool;
        /// `[i]` is the key of the `i`-th row of `args.rows`.
        const auto * ready_keys = static_cast<const typename Policy::StoredKey *>(args.keys.ready_keys);
        chassert(!Policy::reads_ready_keys || ready_keys || !size, "The prepare step of the rows did not run");

        for (size_t chunk_begin = 0; chunk_begin < size; chunk_begin += Policy::chunk_rows_max)
        {
            const size_t chunk_rows = std::min(Policy::chunk_rows_max, size - chunk_begin);
            Policy policy{
                .key_getter = key_getter,
                .map = map,
                .buckets = map.bucket_cells.data(),
                .table = map.bucket_cells.front(),
                .first = args.rows.first + chunk_begin,
                .list = row_list ? args.rows.rows + chunk_begin : nullptr,
                .position_base = chunk_begin,
                .skip_data = skip_data,
                .ready_keys = Policy::reads_ready_keys ? ready_keys + chunk_begin : nullptr,
                .pool = pool,
                .on_row = on_row,
                .found_flags = found_flags};
            /// Inlined, so that what `on_row` captured by reference stays in registers, like the fields of the frame-local policy.
            [[clang::always_inline]] amacRun(policy, chunk_rows);
        }
        return size;
    }
};

/// Instantiated in `FindKeysAmacMaps*.cpp`, one file per map.
extern template FindKeysFn selectFindKeys<HashJoin::MapsOne, AmacLookup<false>>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsOne, AmacLookup<true>>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsAll, AmacLookup<false>>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsAll, AmacLookup<true>>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsAsof, AmacLookup<false>>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsAsof, AmacLookup<true>>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsSet, AmacLookup<false>>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsSet, AmacLookup<true>>(HashJoin::Type, FindKeysMode, bool);

}
