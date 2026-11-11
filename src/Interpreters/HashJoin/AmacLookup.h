#pragma once

#include <Interpreters/HashJoin/AmacRing.h>
#include <Interpreters/HashJoin/LookupMethod.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/PODArray.h>

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
    decltype(auto) getMapped() const { return cell->getMapped(); }
    size_t getOffset() const { return offset; }
};

/// What `on_row` gets from the ring for a row whose key is not in the map, is NULL, or is masked out.
template <typename Cell>
struct AmacNotFound
{
    static constexpr bool isFound() { return false; }
    decltype(std::declval<const Cell &>().getMapped()) getMapped() const { UNREACHABLE(); }
    static constexpr size_t getOffset() { return 0; }
};

/** The policy that `amacRun` drives to find keys in a `TwoLevelHashTable` map of the join. The lookups finish out of order.
  * A visit calls `on_row` when it reads its key's cell or learns that the key is missing, so nothing reads the cell again.
  *
  * With one bucket, a visit wraps at the bounds that the policy holds in its frame. With several buckets, a slot also has the
  * index of its bucket, and a visit reads the bounds from `buckets`. With `robin_hood`, a slot also has the distance that it
  * has walked. `row_list` is a template parameter, so `indexAt` has no branch.
  */
template <typename KeyGetter, typename Map, bool robin_hood, bool fast_path, bool row_list, typename OnRow>
struct AmacFindPolicy
{
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

    /// The key as the map compares it: a fixed key by value, a string key as a view into the key column. A slot can keep the
    /// view, because the string holder copies a key into the arena only for an insert.
    using KeyHolder = std::remove_reference_t<decltype(std::declval<KeyGetter &>().getKeyHolder(0uz, std::declval<Arena &>()))>;
    using StoredKey = std::decay_t<decltype(keyHolderGetKey(std::declval<KeyHolder &>()))>;
    static_assert(std::is_trivially_copyable_v<StoredKey>);
    /// `HashMethodKeysFixed` packs the key columns of a row into one key: row by row, unless it packed the whole block into
    /// `prepared_keys` when it was built. For such a getter `run` gives the policy the keys of the chunk in `packed_keys`,
    /// so that `start` loads a key and calls nothing.
    static constexpr bool packs_keys = detail::PacksKeysIntoBlob<KeyGetter>;

    /** The state of the ring. A slot is inactive when its `cell` is null, so a value-initialized ring has no active slot and
      * `row` needs no reserved value. A slot does not store the offset of its cell: `hit` computes it from the cell pointer.
      * The key is stored in the slot, so that a visit does not fetch it through the row again (`keyHolderAt`).
      */
    struct RingBase
    {
        std::array<const Cell *, amac_ring_size> cell{}; /// the cell the next visit reads
        std::array<UInt16, amac_ring_size> row{};        /// chunk-local probe row
        alignas(64) std::array<StoredKey, amac_ring_size> key{};

        bool isActive(size_t s) const { return cell[s] != nullptr; }
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

    using Ring = std::conditional_t<
        one_bucket,
        TableRing,
        std::conditional_t<robin_hood, RobinHoodRing, BucketRing>>;

    /// By value where possible. The key-column pointer is then a field of the frame-local policy. Behind a reference,
    /// reading it would take two dependent loads.
    std::conditional_t<std::is_trivially_copyable_v<KeyGetter>, KeyGetter, KeyGetter &> key_getter;
    const Map & map;
    /// `Map::bucket_cells`, which `computeBucketPrefix` fills when the table is frozen. For a map of one bucket, `table` is
    /// a copy of its only entry, so that a visit reads the bounds from the frame.
    const Bucket * buckets = nullptr;
    Bucket table{};
    size_t first = 0;              /// a range: the block row of the chunk's first row
    const UInt32 * list = nullptr; /// a row list: the block rows of the chunk
    size_t position_base = 0;      /// the position of the chunk's first row in `FindKeysArgs::rows`
    const UInt8 * skip_data = nullptr; /// indexed by the block row, not by the chunk; null on the fast path
    const StoredKey * packed_keys = nullptr; /// with `packs_keys`: `[i]` is the key of the chunk's row `i`
    Arena & pool;
    OnRow on_row;

    ALWAYS_INLINE const Bucket & bucketAt(size_t bucket [[maybe_unused]]) const
    {
        if constexpr (one_bucket)
            return table;
        else
            return buckets[bucket];
    }

    ALWAYS_INLINE const Bucket & bucketOf(const Ring & ring [[maybe_unused]], size_t s [[maybe_unused]]) const
    {
        if constexpr (one_bucket)
            return table;
        else
            return buckets[ring.bucket[s]];
    }

    /// What `Cell::getHash` gives: the hash saved in the cell, or the hash of its key.
    ALWAYS_INLINE size_t hashOf(const Cell & cell) const
    {
        if constexpr (store_hash)
            return cell.saved_hash;
        else
            return map.hash(cell.getKey());
    }

    ALWAYS_INLINE size_t indexAt(size_t i) const
    {
        if constexpr (row_list)
            return list[i];
        else
            return first + i;
    }

    ALWAYS_INLINE decltype(auto) keyHolderAt(size_t i [[maybe_unused]], size_t ind [[maybe_unused]])
    {
        if constexpr (packs_keys)
            return packed_keys[i];
        else
            return key_getter.getKeyHolder(ind, pool);
    }

    ALWAYS_INLINE void miss(const Ring & ring, size_t s)
    {
        const size_t i = ring.row[s];
        on_row(position_base + i, indexAt(i), AmacNotFound<Cell>{});
    }

    /// The cell is not the zero cell, so its offset is its position over all buckets plus one, as `offsetInternal` gives.
    ALWAYS_INLINE void hit(const Ring & ring, size_t s, const Cell * cell)
    {
        const size_t i = ring.row[s];
        const Bucket & bucket = bucketOf(ring, s);
        size_t offset = static_cast<size_t>(cell - bucket.cells) + 1;
        if constexpr (!one_bucket)
            offset += bucket.prefix;
        on_row(position_base + i, indexAt(i), AmacFound<Cell>{cell, offset});
    }

    ALWAYS_INLINE bool start(Ring & ring, size_t s, size_t i)
    {
        const size_t ind = indexAt(i);
        if constexpr (!fast_path)
        {
            if (skip_data && skip_data[ind])
            {
                on_row(position_base + i, ind, AmacNotFound<Cell>{});
                return false;
            }
        }
        auto && key_holder = keyHolderAt(i, ind);
        const auto & key = keyHolderGetKey(key_holder);
        if (unlikely(Cell::isZero(key, no_state)))
        {
            /// A lookup of the zero key reads only the zero-value cell, so it has no walk to overlap with other lookups.
            if (const Cell * cell = map.find(key))
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
/// the getter turns off the look-ahead prefetch of `PrefetchingLookup`. A getter that builds the keys of many rows at once
/// (`PreparedJoinKeys`), such as the `hashed` getter and the getter of the packed `String` keys, stays there too: the ring
/// does not prepare keys, so it would build each key alone.
template <typename KeyGetter, typename Map, bool robin_hood>
constexpr bool amac_probe_supported = AmacWalkableMap<Map> && (!robin_hood || amac_buckets_switch_to_robin_hood<Map>)
    && !is_low_cardinality_join_key_getter<KeyGetter> && !PreparedKeyGetter<KeyGetter>;

/// Looks the keys up with the AMAC ring. Rows retire out of order. Only for a table with `HashJoinTable::takesAmacLookup`.
/// `robin_hood` must equal `bucketsAreRobinHood` of the table.
template <bool robin_hood>
struct AmacLookup
{
    static constexpr bool retires_rows_in_order = false;

    template <HashJoin::Type TYPE, typename Maps>
    static constexpr bool supports = amac_probe_supported<
        typename KeyGetterForType<TYPE, const JoinMapOf<TYPE, Maps>>::Type, JoinMapOf<TYPE, Maps>, robin_hood>;

    template <HashJoin::Type TYPE, typename Maps, bool fast_path, bool row_list, bool keep_key_getter = false, typename OnRow>
    ALWAYS_INLINE static size_t run(const FindKeysArgs & args, OnRow && on_row)
    {
        using Map = JoinMapOf<TYPE, Maps>;
        using Cell = typename Map::cell_type;
        using KeyGetter = typename KeyGetterForType<TYPE, const Map>::Type;
        using Policy = AmacFindPolicy<KeyGetter, Map, robin_hood, fast_path, row_list, std::decay_t<OnRow>>;
        constexpr bool is_asof_join = std::is_same_v<Maps, HashJoin::MapsAsof>;
        static_assert(std::is_void_v<decltype(on_row(0uz, 0uz, AmacNotFound<Cell>{}))>, "The ring cannot stop at a row");

        chassert(row_list == (args.rows.rows != nullptr));
        const Map & map = args.table.mapOf<TYPE, Maps>();
        chassert(map.bucket_cells.size() == Map::NUM_BUCKETS, "The table was not frozen");
        chassert(map.robin_hood_buckets == (robin_hood ? Map::RobinHoodBuckets::All : Map::RobinHoodBuckets::None));
        decltype(auto) key_getter = keyGetterOf<KeyGetter, is_asof_join, keep_key_getter>(args);

        const size_t size = args.rows.size;
        [[maybe_unused]] const auto row_of = [&](size_t i) -> size_t { return row_list ? args.rows.rows[i] : args.rows.first + i; };
        const UInt8 * skip_data = nullptr;
        IColumn::Filter skip_buffer;
        if constexpr (!fast_path)
        {
            if (size)
                skip_data = args.keys.buildRowSkipData(skip_buffer, row_of(0), row_of(size - 1) + 1 - row_of(0));
        }
        Arena pool;
        /// The getter was built for the columns of the whole block, so its prepared keys are indexed by the block row.
        [[maybe_unused]] const bool keys_are_prepared = [&]
        {
            if constexpr (Policy::packs_keys)
                return !row_list && !key_getter.prepared_keys.empty();
            else
                return false;
        }();
        [[maybe_unused]] PODArray<typename Policy::StoredKey> packed;

        for (size_t chunk_begin = 0; chunk_begin < size; chunk_begin += Policy::chunk_rows_max)
        {
            const size_t chunk_rows = std::min(Policy::chunk_rows_max, size - chunk_begin);
            const typename Policy::StoredKey * packed_keys = nullptr;
            if constexpr (Policy::packs_keys)
            {
                if (keys_are_prepared)
                    packed_keys = key_getter.prepared_keys.data() + row_of(chunk_begin);
                else
                {
                    packed.resize(chunk_rows);
                    for (size_t j = 0; j < chunk_rows; ++j)
                    {
                        auto key_holder = key_getter.getKeyHolder(row_of(chunk_begin + j), pool);
                        packed[j] = keyHolderGetKey(key_holder);
                    }
                    packed_keys = packed.data();
                }
            }
            Policy policy{
                .key_getter = key_getter,
                .map = map,
                .buckets = map.bucket_cells.data(),
                .table = map.bucket_cells.front(),
                .first = args.rows.first + chunk_begin,
                .list = row_list ? args.rows.rows + chunk_begin : nullptr,
                .position_base = chunk_begin,
                .skip_data = skip_data,
                .packed_keys = packed_keys,
                .pool = pool,
                .on_row = on_row};
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
