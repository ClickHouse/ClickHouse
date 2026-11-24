#pragma once

#include <Interpreters/HashJoin/AmacRing.h>
#include <Interpreters/HashJoin/LookupMethod.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <base/arithmeticOverflow.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <type_traits>
#include <utility>

/// The AMAC lookup method of the hash join probe: `amacRun` (`AmacRing.h`) drives a find policy that walks the cells of the
/// hash maps of the join, through their single or partitioned view (`JoinPartitionView.h`).
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

template <typename KeyGetter, typename Map, bool robin_hood, bool fast_path, bool row_list, typename OnRow, bool prefetch_used_flags>
struct AmacFindPolicy
{
    using Cell = typename Map::cell_type;
    using Partition = JoinPartitionDesc<Cell>;
    static constexpr bool one_partition = !Map::partitioned;
    static constexpr bool store_hash = cell_stores_hash<Cell>;
    static constexpr bool prefetch_asof = std::is_same_v<typename Map::mapped_type, AsofRowRefs>;
    static constexpr HashTableNoState no_state{};
    /// `AmacLookup::run` splits the rows into chunks of at most this many rows, so that the row index in a slot fits in 16
    /// bits. Any size up to 65536 would do.
    static constexpr size_t chunk_rows_max = 1uz << 13;
    static_assert(!robin_hood || !one_partition, "Only the maps of a partitioned view switch to the Robin Hood mode");

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

    /// A null cell marks an inactive slot. Keeping the key in the slot avoids reloading it from the input on each visit.
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

    /// A partitioned view: the partition that the slot walks, an index into `partitions`. Two bytes, for up to 2^15 partitions;
    /// the padded ring takes as many bytes as with one byte.
    struct PartitionRing : public TableRing
    {
        std::array<UInt16, amac_ring_size> partition{};
    };

    /// Partitions in the Robin Hood mode: how many cells the slot has walked from the home cell of its key.
    struct RobinHoodRing : public PartitionRing
    {
        std::array<UInt32, amac_ring_size> distance{};
    };

    using WalkRing = std::conditional_t<
        one_partition,
        TableRing,
        std::conditional_t<robin_hood, RobinHoodRing, PartitionRing>>;

    struct FlagRing : WalkRing
    {
        /// A nonzero offset identifies the cell whose used flag we prefetched.
        std::array<size_t, amac_ring_size> pending_offsets{};
    };
    enum class AsofStage : UInt8
    {
        FindKey,
        PrefetchSearch,
        Match,
    };
    struct AsofRing : WalkRing
    {
        std::array<AsofStage, amac_ring_size> stage{};
    };
    using Ring = std::conditional_t<prefetch_asof, AsofRing, std::conditional_t<prefetch_used_flags, FlagRing, WalkRing>>;

    /// By value where possible. The key-column pointer is then a field of the frame-local policy. Behind a reference,
    /// reading it would take two dependent loads.
    using KeyGetterField = std::conditional_t<
        reads_ready_keys,
        NoKeyGetter,
        std::conditional_t<std::is_trivially_copyable_v<KeyGetter>, KeyGetter, KeyGetter &>>;
    KeyGetterField key_getter;
    const Map & map;
    const Cell * const * partitions = nullptr;
    size_t shift = 0;
    size_t cell_bits = 0;
    Partition table{};
    size_t first = 0;              /// a range: the block row of the chunk's first row
    const UInt64 * list = nullptr; /// a row list: the block rows of the chunk
    size_t position_base = 0;      /// the position of the chunk's first row in `FindKeysArgs::rows`
    const UInt8 * skip_data = nullptr; /// indexed by the block row, not by the chunk; null on the fast path
    const StoredKey * ready_keys = nullptr; /// with `reads_ready_keys`: `[i]` is the key of the chunk's row `i`
    Arena & pool;
    OnRow on_row;
    const UInt8 * used_flags;

    [[nodiscard]] ALWAYS_INLINE Partition partitionAt(size_t partition [[maybe_unused]]) const
    {
        if constexpr (one_partition)
            return table;
        else
            return {partitions[partition], table.mask};
    }

    [[nodiscard]] ALWAYS_INLINE Partition partitionOf(const Ring & ring [[maybe_unused]], size_t s [[maybe_unused]]) const
    {
        if constexpr (one_partition)
            return table;
        else
            return partitionAt(ring.partition[s]);
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
        on_row(position_base + i, indexAt(i), AmacNotFound<Cell>{});
    }

    /// A nonzero key's offset is its one-based cell index across all partitions.
    [[nodiscard]] ALWAYS_INLINE size_t cellOffset(const Ring & ring, size_t s, const Cell * cell) const
    {
        size_t offset = static_cast<size_t>(cell - partitionOf(ring, s).cells) + 1;
        if constexpr (!one_partition)
            offset += size_t{ring.partition[s]} << cell_bits;
        return offset;
    }

    ALWAYS_INLINE AmacStepResult emitHit(const Ring & ring, size_t s, const Cell * cell, size_t offset)
    {
        const size_t i = ring.row[s];
        on_row(position_base + i, indexAt(i), AmacFound<Cell>{cell, offset});
        return AmacStepResult::Done;
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
            const Cell * cell = map.find(key);
            if (cell)
                on_row(position_base + i, ind, AmacFound<Cell>{cell, map.offsetInternal(cell)});
            else
                on_row(position_base + i, ind, AmacNotFound<Cell>{});
            return false;
        }
        const size_t hash = map.hash(key);
        ring.key[s] = key;
        size_t partition_no = 0;
        if constexpr (!one_partition)
        {
            partition_no = static_cast<UInt32>(hash) >> shift;
            ring.partition[s] = static_cast<UInt16>(partition_no);
        }
        if constexpr (robin_hood)
            ring.distance[s] = 0;
        const Partition partition = partitionAt(partition_no);
        const Cell * cell = partition.cells + (hash & partition.mask);
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
        if constexpr (prefetch_asof)
        {
            switch (ring.stage[s])
            {
                case AsofStage::PrefetchSearch:
                    cell->getMapped()->prefetchSearch();
                    ring.stage[s] = AsofStage::Match;
                    return AmacStepResult::Advance;
                case AsofStage::Match:
                    ring.stage[s] = AsofStage::FindKey;
                    return emitHit(ring, s, cell, cellOffset(ring, s, cell));
                case AsofStage::FindKey:
                    break;
            }
        }
        else if constexpr (prefetch_used_flags)
        {
            if (const size_t offset = ring.pending_offsets[s])
            {
                ring.pending_offsets[s] = 0;
                return emitHit(ring, s, cell, offset);
            }
        }
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
            if constexpr (prefetch_asof)
            {
                __builtin_prefetch(cell->getMapped().get(), 0, 3);
                ring.stage[s] = AsofStage::PrefetchSearch;
                return AmacStepResult::Advance;
            }
            else if constexpr (prefetch_used_flags)
            {
                const size_t offset = cellOffset(ring, s, cell);
                __builtin_prefetch(used_flags + offset, 0, 3);
                ring.pending_offsets[s] = offset;
                return AmacStepResult::Advance;
            }
            else
                return emitHit(ring, s, cell, cellOffset(ring, s, cell));
        }
        const Partition partition = partitionOf(ring, s);
        if constexpr (robin_hood)
        {
            /// The miss rule of `HashTable::findCellRobinHood`. The keys of a chain are in the order of their home cells. So
            /// the key is not in the table if this cell's key is closer to its home than the key would be here. The rule
            /// does not apply in the key's home cell.
            const size_t place = static_cast<size_t>(cell - partition.cells);
            const size_t distance = ring.distance[s];
            /// The subtraction wraps when the home cell is after `place` in the buffer; the mask takes it modulo the table size.
            if (distance && (common::subIgnoreOverflow(place, hashOf(*cell)) & partition.mask) < distance)
            {
                miss(ring, s);
                return AmacStepResult::Done;
            }
            ring.distance[s] = static_cast<UInt32>(distance + 1);
        }
        /// The end comes from the descriptor. Wrapping by masking the cell index instead would cost a division by the cell size.
        if (++cell == partition.cells + partition.mask + 1) [[unlikely]]
            cell = partition.cells;
        ring.cell[s] = cell;
        prefetchCell(cell);
        return AmacStepResult::Advance;
    }
};

/// The views of the hash maps of the join: `freezeForProbing` fills their descriptors. The direct-addressed maps (`key8`,
/// `key16`, `range*`) have no collision chain to overlap. The partitions of 8- and 16-bit keys hold at most 65536 keys, far
/// fewer than fill the L2 and the L3 that the ring needs.
template <typename Map>
concept AmacWalkableMap = requires(const Map & map)
{
    { map.desc(0) } -> std::convertible_to<JoinPartitionDesc<typename Map::cell_type>>;
    { Map::partitioned } -> std::convertible_to<bool>;
    requires sizeof(typename Map::key_type) > 2;
};

/// Whether the maps of the view can switch to the Robin Hood mode: only those of a partitioned view.
template <typename Map>
constexpr bool amac_partitions_switch_to_robin_hood = false;
template <typename Map>
constexpr bool amac_partitions_switch_to_robin_hood<JoinPartitionedView<Map>> = GrowerWithRobinHood<typename Map::grower_type>;

/// Whether the ring can look up the keys of `KeyGetter` in `Map`. The LowCardinality getter stays on `PrefetchingLookup`: it
/// deduplicates lookups per dictionary index through its own cache, and a ring would bypass that cache. For the same reason,
/// the getter turns off the look-ahead prefetch of `PrefetchingLookup`.
template <typename KeyGetter, typename Map, bool robin_hood>
constexpr bool amac_probe_supported = AmacWalkableMap<Map> && (!robin_hood || amac_partitions_switch_to_robin_hood<Map>)
    && !is_low_cardinality_join_key_getter<KeyGetter>;

/// The table must be frozen and its partition mode must match `robin_hood`.
/// For `BuildsKeys` getters, the caller prepares the keys before each search.
template <bool robin_hood, bool prefetch_used_flags = false>
struct AmacLookup
{
    static constexpr bool retires_rows_in_order = false;

    template <HashJoin::Type TYPE, typename Maps>
    using KeyGetterOf = typename KeyGetterForType<TYPE, const JoinMapOf<TYPE, Maps>>::Type;

    template <HashJoin::Type TYPE, typename Maps>
    static constexpr bool supports = amac_probe_supported<KeyGetterOf<TYPE, Maps>, JoinMapOf<TYPE, Maps>, robin_hood>
        || amac_probe_supported<KeyGetterOf<TYPE, Maps>, JoinPartitionedFormOf<TYPE, Maps>, robin_hood>;

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

    /// The form of the table (single or partitioned) is chosen here, once per search.
    template <HashJoin::Type TYPE, typename Maps, bool fast_path, bool row_list, typename OnRow>
    ALWAYS_INLINE static size_t run(const FindKeysArgs & args, OnRow && on_row)
    {
        return args.table.visitForm<TYPE, Maps>([&]<typename Form>(const Form & map) ALWAYS_INLINE -> size_t
        {
            if constexpr (!amac_probe_supported<KeyGetterOf<TYPE, Maps>, Form, robin_hood>)
                UNREACHABLE();
            else
                return runOn<TYPE, Maps, fast_path, row_list>(args, map, on_row);
        });
    }

    template <HashJoin::Type TYPE, typename Maps, bool fast_path, bool row_list, typename Map, typename OnRow>
    ALWAYS_INLINE static size_t runOn(const FindKeysArgs & args, const Map & map, OnRow && on_row)
    {
        using Cell = typename Map::cell_type;
        using KeyGetter = KeyGetterOf<TYPE, Maps>;
        using Policy = AmacFindPolicy<KeyGetter, Map, robin_hood, fast_path, row_list, std::decay_t<OnRow>, prefetch_used_flags>;
        constexpr bool is_asof_join = std::is_same_v<Maps, HashJoin::MapsAsof>;
        static_assert(std::is_void_v<decltype(on_row(0uz, 0uz, AmacNotFound<Cell>{}))>, "The ring cannot stop at a row");

        chassert(row_list == (args.rows.rows != nullptr));
        chassert(map.isFrozen(), "The table was not frozen");
        if constexpr (Map::partitioned)
            chassert(map.isRobinHood() == robin_hood);
        decltype(auto) key_getter = keyGetterFor<Policy, KeyGetter, is_asof_join>(args);

        const size_t size = args.rows.size;
        const UInt8 * skip_data = nullptr;
        IColumn::Filter skip_buffer;
        if constexpr (!fast_path)
        {
            const auto row_of = [&](size_t i) -> size_t { return row_list ? args.rows.rows[i] : args.rows.first + i; };
            if (size)
                skip_data = args.keys().buildRowSkipData(skip_buffer, row_of(0), row_of(size - 1) + 1 - row_of(0));
        }
        Arena pool;
        /// `[i]` is the key of the `i`-th row of `args.rows`.
        const auto * ready_keys = static_cast<const typename Policy::StoredKey *>(args.keys().ready_keys);
        chassert(!Policy::reads_ready_keys || ready_keys || !size, "The prepare step of the rows did not run");

        for (size_t chunk_begin = 0; chunk_begin < size; chunk_begin += Policy::chunk_rows_max)
        {
            const size_t chunk_rows = std::min(Policy::chunk_rows_max, size - chunk_begin);
            Policy policy{
                .key_getter = key_getter,
                .map = map,
                .table = map.desc(0),
                .first = args.rows.first + chunk_begin,
                .list = row_list ? args.rows.rows + chunk_begin : nullptr,
                .position_base = chunk_begin,
                .skip_data = skip_data,
                .ready_keys = Policy::reads_ready_keys ? ready_keys + chunk_begin : nullptr,
                .pool = pool,
                .on_row = on_row,
                .used_flags = args.used_flags};
            if constexpr (Map::partitioned)
            {
                policy.partitions = map.cellBases();
                policy.cell_bits = map.cellBits();
                policy.shift = map.partitionShift();
            }
            /// Inlined, so that what `on_row` captured by reference stays in registers, like the fields of the frame-local policy.
            [[clang::always_inline]] amacRun(policy, chunk_rows);
        }
        return size;
    }
};

/// Instantiated in `FindKeysAmacMaps*.cpp`, one file per map. An ASOF join takes only the single pass, so there is no instance
/// for `MapsAsof`.
extern template FindKeysFn selectFindKeys<HashJoin::MapsOne, AmacLookup<false>>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsOne, AmacLookup<true>>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsAll, AmacLookup<false>>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsAll, AmacLookup<true>>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsSet, AmacLookup<false>>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsSet, AmacLookup<true>>(HashJoin::Type, FindKeysMode, bool);

}
