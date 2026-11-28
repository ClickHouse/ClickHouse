#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/JoinUtils.h>
#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Common/HashTable/Prefetching.h>

#include <absl/functional/function_ref.h>

#include <any>
#include <functional>
#include <optional>
#include <span>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/// Prefetching doesn't make sense for small hash tables, because they fit in caches entirely.
/// Returns the threshold (in bytes) above which prefetching is enabled in JOIN.
size_t getMinBytesForPrefetchInJoin();

/// Check if the hash table type supports the prefetch interface.
template <typename Map, typename KeyHolder>
concept HasPrefetchMemberFunc = requires
{
    {std::declval<Map>().prefetch(std::declval<KeyHolder>())};
};

/// True when software prefetch in the JOIN probe loop is feasible for the given key
/// getter / map combination: the key getter must compute keys cheaply and the map
/// must expose a `prefetch` member that takes the key holder produced by the getter.
template <typename KeyGetter, typename Map>
constexpr bool join_prefetch_supported = KeyGetter::has_cheap_key_calculation
    && HasPrefetchMemberFunc<
        std::remove_const_t<Map>,
        decltype(std::declval<KeyGetter &>().getKeyHolder(std::declval<size_t>(), std::declval<Arena &>()))>;

/// Decide at runtime whether prefetching should actually fire for a given map: the user
/// must have it enabled and the map must be large enough that we expect non-trivial
/// cache misses to amortize the prefetch cost.
template <typename Map>
ALWAYS_INLINE bool shouldUseJoinPrefetch(bool enable_prefetch, const Map * map)
{
    return enable_prefetch && map != nullptr
        && map->getBufferSizeInBytes() > getMinBytesForPrefetchInJoin();
}

/// Drives the adaptive software prefetch logic in the hash join probe loop.
template <typename PrefetchAction>
struct JoinPrefetcher
{
    bool use_prefetch = false;
    size_t total = 0;
    PrefetchAction prefetch_action;
    PrefetchingHelper prefetching{};
    size_t prefetch_look_ahead = PrefetchingHelper::getInitialLookAheadValue();

    ALWAYS_INLINE void prefetchAt(size_t i)
    {
        if (!use_prefetch)
            return;

        /// Estimate optimal look-ahead distance once we have measured iteration latency.
        if (i == PrefetchingHelper::iterationsToMeasure())
            prefetch_look_ahead = prefetching.calcPrefetchLookAhead();

        const size_t prefetch_idx = i + prefetch_look_ahead;
        if (prefetch_idx < total)
            prefetch_action(prefetch_idx);
    }
};

template <typename PrefetchAction>
ALWAYS_INLINE auto makeJoinPrefetcher(bool use_prefetch, size_t total, PrefetchAction && prefetch_action)
{
    return JoinPrefetcher<std::decay_t<PrefetchAction>>{
        use_prefetch, total, std::forward<PrefetchAction>(prefetch_action)};
}

/// For a direct-addressed map: the minimum key and the range size from the minimum to the maximum key.
struct JoinKeyRange
{
    UInt64 min_key = 0;
    UInt64 size = 0;
};

template <typename KeyGetter, bool is_asof_join>
KeyGetter createJoinKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, JoinKeyRange key_range = {})
{
    KeyGetter getter = [&]()
    {
        if constexpr (is_asof_join)
        {
            auto key_column_copy = key_columns;
            auto key_size_copy = key_sizes;
            key_column_copy.pop_back();
            key_size_copy.pop_back();
            return KeyGetter(key_column_copy, key_size_copy, nullptr);
        }
        else
            return KeyGetter(key_columns, key_sizes, nullptr);
    }();

    if constexpr (ColumnsHashing::IsHashMethodInRange<KeyGetter>::value)
    {
        getter.min_key = static_cast<decltype(getter.min_key)>(key_range.min_key);
        getter.range_size = static_cast<decltype(getter.range_size)>(key_range.size);
    }

    return getter;
}

/// The table object of the key type `TYPE`: a `JoinMapForms`, or a map that has only the single form.
template <HashJoin::Type TYPE, typename Maps>
using JoinTableOf = typename std::remove_cvref_t<decltype(std::declval<Maps &>().template mapOf<TYPE>())>::element_type;

namespace detail
{
template <typename Table>
struct JoinFormsOf
{
    using Single = Table;
    using Partitioned = void;
};
template <typename Single_, typename Partitioned_>
struct JoinFormsOf<JoinMapForms<Single_, Partitioned_>>
{
    using Single = Single_;
    using Partitioned = Partitioned_;
};
template <typename Form>
struct JoinPartMapOfForm
{
    using Type = Form;
};
template <typename Map>
struct JoinPartMapOfForm<JoinPartitionedView<Map>>
{
    using Type = Map;
};

}

/// The single form of the key type `TYPE`: a `JoinSingleView` of a hash map, or a direct-addressed map.
template <HashJoin::Type TYPE, typename Maps>
using JoinMapOf = typename detail::JoinFormsOf<JoinTableOf<TYPE, Maps>>::Single;

/// The partitioned form of the key type `TYPE`: a `JoinPartitionedView`, or `void` for a key type that has no partitioned
/// form.
template <HashJoin::Type TYPE, typename Maps>
using JoinPartitionedFormOf = typename detail::JoinFormsOf<JoinTableOf<TYPE, Maps>>::Partitioned;

/// The map of one partition of the key type `TYPE`.
template <HashJoin::Type TYPE, typename Maps>
using JoinPartMapOf = typename detail::JoinPartMapOfForm<JoinPartitionedFormOf<TYPE, Maps>>::Type;

namespace detail
{

/// The bytes of one key column inside a packed map key, and the output column they go to.
struct PackedKeyColumn
{
    size_t output_pos;
    size_t offset;
    size_t width;
};

/// How the key columns are recovered from a map key. A map of one key column gives the whole key. A map that packs all the
/// key columns into one fixed-width blob gives one byte range per column.
struct KeyLayout
{
    std::optional<size_t> whole_key_pos;
    std::vector<PackedKeyColumn> packed;
};

/// The order in which the key columns occupy bytes of a packed key. `packed_sizes` is what
/// `HashMethodKeysFixed::shuffleKeyColumns` reports: the widths in packed order, or nothing for the order of the clause.
/// `shuffleKeyColumns` keeps columns of one width in the order of the clause, so the mapping is unique.
std::vector<size_t> packedKeyOrder(const Sizes & clause_sizes, const std::optional<Sizes> & packed_sizes);

/// `HashMethodKeysFixed` is the only join key getter that packs key columns into one blob, and the only one that declares
/// `shuffleKeyColumns`, so this selects exactly the `keysN` maps.
template <typename KeyGetter>
concept PacksKeysIntoBlob = requires(std::vector<IColumn *> & columns, const Sizes & sizes) {
    { KeyGetter::shuffleKeyColumns(columns, sizes) } -> std::same_as<std::optional<Sizes>>;
};

template <typename Key>
std::string_view keyBytes(const Key & key)
{
    if constexpr (std::is_same_v<Key, std::string_view>)
    {
        /// An empty string must have a non-null pointer, because `ColumnNullable::insertData` treats nullptr as `NULL`. The empty
        /// `std::string_view` of the zero key of a hash table has `data() == nullptr`, but it is an empty string, not a `NULL`.
        return key.data() ? key : std::string_view("");
    }
    else
        return {reinterpret_cast<const char *>(&key), sizeof(Key)};
}

void insertKey(MutableColumns & columns, const KeyLayout & layout, std::string_view key, size_t times);

}

namespace detail
{

/// A map whose grower can be stepped through the sizes the map takes as it grows.
template <typename Map>
concept GrowerCanBeStepped = requires(typename Map::grower_type grower)
{
    grower.overflow(size_t{});
    grower.increaseSize();
    typename Map::cell_type;
};

}

/// The hash table of one `ON` disjunct of a `HashJoin`. All the tables of a join have one key type. `visitForm` takes that type
/// as a template argument, so the probe switches on it and on the form (single or partitioned) once per block. The other
/// methods switch on the stored type.
class HashJoinTable
{
public:
    using Type = HashJoin::Type;
    /// Where a walk over the cells resumes.
    using Cursor = std::any;

    /// The table starts with one empty map in the single form. `robin_hood_allowed` is the setting
    /// `enable_robin_hood_hash_for_joins`: the map of a partition may switch to the Robin Hood mode when it grows large.
    HashJoinTable(
        Type key_class,
        Sizes key_sizes_,
        JoinKind kind,
        JoinStrictness strictness,
        JoinMapsKind maps_kind,
        const HashJoinBuildLayout & layout,
        bool robin_hood_allowed);

    /// The key type of the maps. Only `tryConvertToFixedHashMap` changes it.
    Type type() const { return key_type; }

    /// The key range of a direct-addressed map. Only `tryConvertToFixedHashMap` sets it.
    const JoinKeyRange & keyRange() const { return key_range; }

    /// The sizes of the key columns, the `ASOF` column last.
    const Sizes & keySizes() const { return key_sizes; }

    /// The number of keys inserted so far. Other threads may be inserting.
    size_t keys() const { return num_keys.load(std::memory_order_relaxed); }
    /// Adds `n` to the key count, for a filler that inserts into the maps directly.
    void addKeys(size_t n) { num_keys.fetch_add(n, std::memory_order_relaxed); }

    /// Switches an empty table to the partitioned form with `2^partition_bits` partitions. The maps of the partitions are
    /// created by their fillers, and the rest by `freezeForProbing`. A table of a LowCardinality key keeps the single form.
    void usePartitions(size_t partition_bits);
    bool isPartitioned() const { return num_partitions > 1; }
    size_t numPartitions() const { return num_partitions; }
    size_t publishedPartitionCapacity() const { return partition_capacity.load(std::memory_order_acquire); }
    size_t publishedPartitionBytes() const { return partition_bytes.load(std::memory_order_relaxed); }
    void publishPartitionCapacity(size_t cells, size_t bytes)
    {
        partition_bytes.store(bytes, std::memory_order_relaxed);
        partition_capacity.store(cells, std::memory_order_release);
    }
    /// Called after all fill streams have stopped, while a spill is still possible.
    void addPartitionNonzeroCounts(std::span<size_t> counts) const;

    /// The partitioned form of the key type `TYPE`, a `JoinPartitionedView`. Only when `isPartitioned`.
    template <Type TYPE, typename Maps>
    auto & partitionedView()
    {
        chassert(key_type == TYPE && isPartitioned());
        return *tableOf<TYPE, Maps>(*this).partitioned;
    }
    /// The single form of the key type `TYPE`. Only when not `isPartitioned`.
    template <Type TYPE, typename Maps>
    auto & singleView()
    {
        chassert(key_type == TYPE && !isPartitioned());
        auto & table = tableOf<TYPE, Maps>(*this);
        if constexpr (is_join_map_forms<std::remove_cvref_t<decltype(table)>>)
            return *table.single;
        else
            return table;
    }
    /// Calls `f(form)` with the form of the key type `TYPE` that the table has, a const reference.
    template <Type TYPE, typename Maps, typename F>
    ALWAYS_INLINE decltype(auto) visitForm(F && f) const
    {
        chassert(key_type == TYPE);
        const auto & table = tableOf<TYPE, Maps>(*this);
        if constexpr (is_join_map_forms<std::remove_cvref_t<decltype(table)>>)
            return table.visit(std::forward<F>(f));
        else
            return std::forward<F>(f)(table);
    }

    /// Calls `f.template operator()<TYPE>()` with the key type of the table.
    template <typename F>
    decltype(auto) dispatchType(F && f) const
    {
        switch (key_type)
        {
#define M(TYPE) \
    case Type::TYPE: \
        return f.template operator()<Type::TYPE>();
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
        }
        UNREACHABLE();
    }

    template <typename Maps>
    bool holds() const { return std::holds_alternative<Maps>(maps); }

    /// The arena of build stream `stream`. No other call may use it at the same time.
    Arena & arena(size_t stream) { return *arenas[stream]; }
    size_t arenaBytes() const;
    /// Notes the chunk that each arena that holds memory allocates next. Only while no stream uses an arena.
    void noteNextArenaChunks();
    /// The sum of the chunks that the arenas of build streams [0, `streams`) allocate next, as of the last
    /// `noteNextArenaChunks`. Can be called while the arenas grow.
    size_t nextArenaChunks(size_t streams) const;
    /// Moves out the arenas that hold memory. They are freed when the caller drops the result.
    std::vector<std::unique_ptr<Arena>> releaseArenas();

    /// The rows of a stored block to insert.
    struct InsertRows
    {
        const ColumnRawPtrs & key_columns;
        const ScatteredBlock::Selector & selector;
        UInt32 stored_block_no;
        ConstNullMapPtr null_map;
        const JoinCommon::JoinMask & join_mask;
    };

    /// What `insert` reads from the join.
    struct InsertSettings
    {
        bool any_take_last_row;
        bool enable_software_prefetch;
        bool must_keep_right_blocks;
        std::optional<TypeIndex> asof_type;
        ASOFJoinInequality asof_inequality;
    };

    /// Inserts `rows` from one build stream. On the partitioned form, each row goes to the partition of its hash. Keys and row
    /// lists go to the arena of build stream `stream`, which no other call uses at the same time. Calls `add_bytes` with the
    /// growth of the maps and of the arena. Defined in `InsertKeys.h` and instantiated in `InsertKeysMaps*.cpp`, one file per map.
    template <typename Maps>
    BuildResult insert(const InsertSettings & settings, const InsertRows & rows, size_t stream, absl::FunctionRef<void(size_t)> add_bytes);

    /// Reserves `keys_to_reserve` keys in the map of the single form.
    void reserve(size_t keys_to_reserve);

    /// The keys that the map of each partition reserves when it is created; 0 for none.
    void setPartitionReserveOnFirstInsert(size_t keys_per_partition);

    /// The bytes that the scatter of a partitioned build copies per row: the key of a partition map, or the saved hash and the
    /// offset of a `String` key, whose characters come on top; a row locator or a retained set block number; and the
    /// `ASOF` value. 0 for a key type without a partitioned view.
    size_t scatterBytesPerRow(bool keep_right_blocks) const
    {
        return std::visit(
            [&]<typename Maps>(const Maps &) -> size_t
            {
                return dispatchType(
                    [&]<Type TYPE>() -> size_t
                    {
                        using Form = JoinPartitionedFormOf<TYPE, Maps>;
                        if constexpr (!is_join_partitioned_view<Form>)
                            return 0;
                        else
                        {
                            constexpr bool string_key = TYPE == Type::key_string || TYPE == Type::key_fixed_string;
                            size_t bytes = string_key ? 2 * sizeof(UInt64) : sizeof(typename Form::map_type::key_type);
                            if constexpr (Maps::has_mapped)
                                bytes += sizeof(UInt64);
                            else if (keep_right_blocks)
                                bytes += sizeof(UInt32);
                            if constexpr (std::is_same_v<Maps, HashJoin::MapsAsof>)
                                bytes += key_sizes.empty() ? sizeof(UInt64) : key_sizes.back();
                            return bytes;
                        }
                    });
            },
            maps);
    }

    /// The buffer bytes that the map of one partition allocates when it reserves `keys` keys, with or without the Robin Hood
    /// mode, for `choosePartitionPlan`. Null for a key type without a partitioned view (the range and LowCardinality maps).
    using BufferBytesForReserve = size_t (*)(size_t keys, bool robin_hood);
    BufferBytesForReserve partitionBufferBytesForReserve() const
    {
        return std::visit(
            [&]<typename Maps>(const Maps &) -> BufferBytesForReserve
            {
                return dispatchType([]<Type TYPE>() -> BufferBytesForReserve
                {
                    using Form = JoinPartitionedFormOf<TYPE, Maps>;
                    if constexpr (is_join_partitioned_view<Form>)
                        return [](size_t keys, bool robin_hood) -> size_t
                        {
                            using Map = typename Form::map_type;
                            typename Map::grower_type grower;
                            /// The grower's own default allows the Robin Hood mode, which fits more keys in a buffer.
                            if constexpr (GrowerWithRobinHood<typename Map::grower_type>)
                                grower.setRobinHoodAllowed(robin_hood);
                            grower.set(keys);
                            return grower.bufSize() * sizeof(typename Map::cell_type);
                        };
                    else
                        return nullptr;
                });
            },
            maps);
    }

    /// Project the growth of a single hash map from its initial reserve. Direct-addressed maps allocate all cells at creation.
    using BufferBytesForGrowth = size_t (*)(size_t keys, size_t reserved_keys);
    BufferBytesForGrowth singleBufferBytesForGrowth() const
    {
        return std::visit(
            [&]<typename Maps>(const Maps &) -> BufferBytesForGrowth
            {
                return dispatchType([]<Type TYPE>() -> BufferBytesForGrowth
                {
                    using Map = JoinMapOf<TYPE, Maps>;
                    if constexpr (detail::GrowerCanBeStepped<Map>)
                        return [](size_t keys, size_t reserved_keys)
                        {
                            typename Map::grower_type grower;
                            if (reserved_keys)
                                grower.set(reserved_keys);
                            while (grower.overflow(keys))
                                grower.increaseSize();
                            return grower.bufSize() * sizeof(typename Map::cell_type);
                        };
                    else
                        return nullptr;
                });
            },
            maps);
    }

    /// Scans for the non-joined rows of a join with a used flag per cell, as share `share` of `num_shares`. With at least as
    /// many partitions as shares, a share walks a contiguous group of partitions and skips a partition whose keys are all used;
    /// otherwise it walks a contiguous range of cells, and the share skips when every key of the table is used. Appends to
    /// `words` the refs of the cells whose flag is unset, from `cursor`, and stops once `max_rows` rows are appended. Returns
    /// the rows appended.
    template <typename Maps>
    size_t scanNotJoined(
        Cursor & cursor,
        size_t share,
        size_t num_shares,
        const JoinStuff::JoinUsedFlags & used_flags,
        size_t max_rows,
        PaddedPODArray<UInt64> & words) const;

    /// Calls `f(key, mapped)` for each cell from `cursor` on, until `f` has returned `max_rows` rows in total, and returns that total.
    template <typename Maps, typename F>
    size_t forEachCell(Cursor & cursor, size_t max_rows, F && f) const
    {
        return dispatchType([&]<Type TYPE>() { return forEachCellTyped<TYPE, Maps>(cursor, max_rows, std::forward<F>(f)); });
    }

    /// Like `forEachCell`, but the callback gets only the mapped value. `f(mapped)` returns a row count, and the table appends
    /// the key of the cell to `columns` that many times. `key_outputs[k]` is the position in `columns` of the `k`-th key column
    /// of the clause, if it is read. Throws `UNSUPPORTED_JOIN_KEYS` for a key type that cannot be recovered from the map, such
    /// as the hash of the keys.
    template <typename Maps, typename F>
    size_t readCells(
        Cursor & cursor, size_t max_rows, std::span<const std::optional<size_t>> key_outputs, MutableColumns & columns, F && f) const
    {
        switch (key_type)
        {
#define M(TYPE) \
    case Type::TYPE: \
        return readCellsTyped<Type::TYPE, Maps>(cursor, max_rows, key_outputs, columns, f);
            APPLY_FOR_JOIN_VARIANTS_LIMITED(M)
#undef M

            default:
                throwKeysNotRecoverable(key_type);
        }
    }

    template <typename Maps, typename F>
    void forEachMapped(F && f)
    {
        dispatchType([&]<Type TYPE>() { tableOf<TYPE, Maps>(*this).forEachMapped(f); });
    }

    /// Publish the cell pointers and shared mask of a partitioned map, or the bounds of a single map, after the last insert.
    void freezeForProbing();

    bool shouldUseAmacLookup(HashJoinAmacMode mode) const
    {
        if (mode == HashJoinAmacMode::OFF)
            return false;
        if (mode == HashJoinAmacMode::ON)
            return amac_support != AmacSupport::Unsupported;
        return amac_support == AmacSupport::LargeTable;
    }

    bool partitionsAreRobinHood() const { return partitions_are_robin_hood; }

    size_t bufferBytes() const;
    size_t bufferSizeInCells() const;
    size_t cellSize() const;

    /// Replaces the map of a `key32` or `key64` table in the single form with a direct-addressed one if its keys span a small
    /// enough range. That map reads keys relative to the key range. Sets the key type and the key range of the table, and returns
    /// whether it did. A `key8` or `key16` table built in partitions moves its keys to its direct-addressed single form.
    bool tryConvertToFixedHashMap(bool is_signed);

    /// For a direct-addressed map, returns a function that tells which values of a column are keys of the map, for a
    /// `SharedFixedHashTableRuntimeFilter`. For any other map, returns an empty function.
    std::function<ColumnPtr(const ColumnWithTypeAndName &)> makeSharedRuntimeFilter(bool build_signed) const;

    /// The partitions and keys that `clearInParallel` splits. An `ASOF` cell owns a heap lookup, so a large `ASOF` map is worth
    /// clearing in parallel. Zero for any other map, and for a table in the single form.
    struct ParallelClearWork
    {
        size_t partitions = 0;
        size_t keys = 0;
    };
    ParallelClearWork parallelClearWork() const;

    /// Clears the partitions of an `ASOF` map in `num_jobs` jobs and passes them to `schedule`.
    /// `schedule` must run them before the table is destroyed.
    void clearInParallel(size_t num_jobs, absl::FunctionRef<void(std::function<void()>)> schedule);

    /// Frees the maps. The table stays, so that a concurrent `keys` stays valid.
    void releaseMaps();

    /// The single form of the key type `TYPE`, which must be `type()`. Only when not `isPartitioned`.
    template <Type TYPE, typename Maps>
    const JoinMapOf<TYPE, Maps> & mapOf() const
    {
        return const_cast<HashJoinTable &>(*this).singleView<TYPE, Maps>();
    }

private:
    /// The table object of the key type `TYPE`: a `JoinMapForms`, or a map with the single form only.
    template <Type TYPE, typename Maps, typename Self>
    static auto & tableOf(Self & self)
    {
        return *std::get<Maps>(self.maps).template mapOf<TYPE>();
    }

    [[noreturn]] static void throwKeysNotRecoverable(Type type);

    /// Does `insert` for the key type `TYPE`, which must be `type()`. Keys and row lists go to `arena`.
    template <Type TYPE, typename Maps>
    BuildResult
    insertTyped(const InsertSettings & settings, const InsertRows & rows, Arena & arena, absl::FunctionRef<void(size_t)> add_bytes);
    /// Inserts into `map`, the single or the partitioned form of the table. Defined in `InsertKeys.h`.
    template <Type TYPE, typename Maps, typename Map>
    BuildResult insertInto(
        Map & map, const InsertSettings & settings, const InsertRows & rows, Arena & arena, absl::FunctionRef<void(size_t)> add_bytes);

    template <Type TYPE, typename Maps>
    size_t scanNotJoinedTyped(
        Cursor & cursor,
        size_t share,
        size_t num_shares,
        const JoinStuff::JoinUsedFlags & used_flags,
        size_t max_rows,
        PaddedPODArray<UInt64> & words) const;

    /// Where a walk over the partitioned form resumes: the partition, and the cell in its map.
    template <typename Iterator>
    struct PartitionCursor
    {
        size_t partition = 0;
        Iterator it;
    };

    template <Type TYPE, typename Maps, typename F>
    size_t forEachCellTyped(Cursor & cursor, size_t max_rows, F && f) const
    {
        const auto & table_ptr = std::get<Maps>(maps).template mapOf<TYPE>();
        if (!table_ptr)
            return 0;
        return visitForm<TYPE, Maps>(
            [&]<typename Form>(const Form & form) -> size_t
            {
                if constexpr (is_join_partitioned_view<Form>)
                {
                    using Iterator = typename Form::map_type::const_iterator;
                    if (!cursor.has_value())
                        cursor = std::make_any<PartitionCursor<Iterator>>();
                    auto & pc = std::any_cast<PartitionCursor<Iterator> &>(cursor);
                    size_t rows = 0;
                    for (; pc.partition < form.numPartitions(); ++pc.partition)
                    {
                        const auto * map = form.partitionOrNull(pc.partition);
                        if (!map)
                            continue;
                        /// A default-constructed iterator marks a partition that the walk has not entered yet.
                        if (pc.it == Iterator())
                            pc.it = map->begin();
                        for (auto end = map->end(); pc.it != end;)
                        {
                            rows += f(pc.it->getKey(), pc.it->getMapped());
                            ++pc.it;
                            if (rows >= max_rows)
                                return rows;
                        }
                        pc.it = Iterator();
                    }
                    return rows;
                }
                else
                {
                    using Iterator = typename Form::const_iterator;
                    if (!cursor.has_value())
                        cursor = std::make_any<Iterator>(form.begin());
                    Iterator & it = std::any_cast<Iterator &>(cursor);

                    size_t rows = 0;
                    for (auto end = form.end(); it != end;)
                    {
                        rows += f(it->getKey(), it->getMapped());
                        ++it;
                        if (rows >= max_rows)
                            break;
                    }
                    return rows;
                }
            });
    }

    template <Type TYPE, typename Maps>
    detail::KeyLayout makeKeyLayout(std::span<const std::optional<size_t>> key_outputs) const
    {
        using Map = JoinMapOf<TYPE, Maps>;
        using KeyGetter = typename KeyGetterForType<TYPE, std::remove_cvref_t<Map>>::Type;

        if (key_outputs.size() != key_sizes.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "StorageJoin has {} key names but {} key sizes", key_outputs.size(), key_sizes.size());

        detail::KeyLayout layout;
        /// `key32` and `keys32` (likewise `key64` and `keys64`) share one map type, so `TYPE` chooses the key getter: only the
        /// `keysN` ones pack several key columns into the map key.
        if constexpr (detail::PacksKeysIntoBlob<KeyGetter>)
        {
            /// Aggregation reports the packed order the same way (`Aggregator.cpp`: `shuffleKeyColumns` feeding the sizes to
            /// `insertKeyIntoColumns`), so the two cannot disagree about the layout.
            std::vector<IColumn *> unused_columns(key_sizes.size(), nullptr);
            const auto order = detail::packedKeyOrder(key_sizes, KeyGetter::shuffleKeyColumns(unused_columns, key_sizes));

            size_t offset = 0;
            std::unordered_set<size_t> emitted;
            for (size_t slot : order)
            {
                /// A key name repeated in the engine arguments (`Join(ALL, INNER, a, a, b)`) occupies one slot per argument,
                /// all holding the same value, but has one output column.
                if (key_outputs[slot] && emitted.insert(*key_outputs[slot]).second)
                    layout.packed.push_back({*key_outputs[slot], offset, key_sizes[slot]});
                offset += key_sizes[slot];
            }

            if (offset > sizeof(typename Map::key_type))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "StorageJoin keys occupy {} bytes but the map key holds {}",
                    offset,
                    sizeof(typename Map::key_type));
        }
        else
            layout.whole_key_pos = key_outputs[0];
        return layout;
    }

    template <Type TYPE, typename Maps, typename F>
    size_t readCellsTyped(
        Cursor & cursor, size_t max_rows, std::span<const std::optional<size_t>> key_outputs, MutableColumns & columns, F && f) const
    {
        const detail::KeyLayout layout = makeKeyLayout<TYPE, Maps>(key_outputs);
        return forEachCellTyped<TYPE, Maps>(
            cursor,
            max_rows,
            [&](const auto & key, const auto & mapped) -> size_t
            {
                const size_t rows = f(mapped);
                detail::insertKey(columns, layout, detail::keyBytes(key), rows);
                return rows;
            });
    }

    template <bool is_signed, typename Key, typename Maps>
    bool tryConvertToFixedHashMapImpl(Maps & maps_);

    template <Type TYPE, typename Maps>
    bool collapsePartitions(Maps & maps_);

    Type key_type;
    JoinKeyRange key_range;
    Sizes key_sizes;
    HashJoin::MapsVariant maps;
    /// One arena per build stream, for the strings of string keys and the nodes of the row ref lists. `Arena` is unsynchronized,
    /// and calls of one stream never overlap. Neither kind of allocation needs contiguity or rollback, so a key and its list can
    /// be in different arenas. The arenas are behind pointers so that `releaseArenas` can move them out and free them alone.
    std::vector<std::unique_ptr<Arena>> arenas;
    /// One per arena, see `noteNextArenaChunks`.
    std::unique_ptr<std::atomic<size_t>[]> next_arena_chunks;
    std::atomic<size_t> num_keys = 0;
    /// 1 in the single form.
    size_t num_partitions = 1;
    std::atomic<size_t> partition_capacity = 0;
    std::atomic<size_t> partition_bytes = 0;
    bool robin_hood_allowed = false;
    size_t partition_reserve_keys = 0;
    enum class AmacSupport : uint8_t
    {
        Unsupported,
        SmallTable,
        LargeTable,
    };
    AmacSupport amac_support = AmacSupport::Unsupported;
    bool partitions_are_robin_hood = false;
};

}
