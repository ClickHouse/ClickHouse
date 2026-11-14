#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/BuildSlots.h>
#include <Interpreters/HashJoin/EmplaceMethod.h>
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

template <HashJoin::Type TYPE, typename Maps>
using JoinMapOf = typename std::remove_cvref_t<decltype(std::declval<Maps &>().template mapOf<TYPE>())>::element_type;

/// The key getter that the build uses to insert into `JoinMapOf<TYPE, Maps>`.
template <HashJoin::Type TYPE, typename Maps>
using JoinKeyGetterOf = typename KeyGetterForType<TYPE, JoinMapOf<TYPE, Maps>>::Type;

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
        /// An empty string must have a non-null pointer, because `ColumnNullable::insertData` treats nullptr as NULL. The empty
        /// `std::string_view` of the zero key of a hash table has `data() == nullptr`, but it is an empty string, not a NULL.
        return key.data() ? key : std::string_view("");
    }
    else
        return {reinterpret_cast<const char *>(&key), sizeof(Key)};
}

void insertKey(MutableColumns & columns, const KeyLayout & layout, std::string_view key, size_t times);

}

/// The hash table of one ON disjunct of a `HashJoin`. All the tables of a join have one key type. `mapOf` takes that type
/// as a template argument, so the probe switches on it once per block. The other methods switch on the stored type.
class HashJoinTable
{
public:
    using Type = HashJoin::Type;
    /// Where a walk over the cells resumes.
    using Cursor = std::any;

    /// `key_class` is the key type of a one-bucket map. A two-level `layout` turns it into the two-level type.
    /// `robin_hood_allowed` is the setting `enable_robin_hood_hash_for_joins`: a two-level map may switch to the Robin Hood mode when it grows large.
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

    template <typename Maps>
    bool holds() const { return std::holds_alternative<Maps>(maps); }

    /// The arena of build stream `stream`. No other call may use it at the same time.
    Arena & arena(size_t stream) { return *arenas[stream]; }
    size_t arenaBytes() const;
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

    /// Routes `rows` over the slots and inserts the rows of each slot under its lock. Keys and row lists go to the arena of
    /// build stream `stream`, which no other call uses at the same time. Calls `add_bytes` under the lock with the growth of
    /// the slot's buffers, and again after the insert with the growth of the arena. Defined in `InsertKeys.h` and instantiated in
    /// `InsertKeysMaps*.cpp`, one file per map.
    template <typename Maps>
    BuildResult insert(const InsertSettings & settings, const InsertRows & rows, size_t stream, absl::FunctionRef<void(size_t)> add_bytes);

    /// Each slot reserves its share of `keys_to_reserve` at its first insert.
    void reserve(size_t keys_to_reserve) { slots.setReserve(keys_to_reserve); }

    /// Scans for the non-joined rows of a join with a used flag per cell. Walks the buckets `b` with `b % num_shares == share`
    /// from `cursor`. Appends to `words` the refs of the cells whose flag is unset, and stops once `max_rows` rows are appended.
    /// Skips the walk when every key of these buckets is used. Returns the rows appended.
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
        dispatchType([&]<Type TYPE>() { typedMap<TYPE, Maps>(*this).forEachMapped(std::forward<F>(f)); });
    }

    /// Computes the cell prefix sums and the min/max bounds of a direct-addressed map, and decides `takesAmacLookup`. Call it
    /// after the last insert.
    void freezeForProbing();

    /// Set by `freezeForProbing`. Whether an AMAC lookup of the probe (`AmacLookup.h`) may take this table: the map lists the
    /// cells of its buckets, the buckets are all in the Robin Hood mode or none is, and the cells take at least as many bytes
    /// as the L2 cache. False for a table that was never frozen, such as that of a `Join` engine table.
    bool takesAmacLookup() const { return takes_amac_lookup; }
    /// Meaningful only with `takesAmacLookup`: true if the buckets are all in the Robin Hood mode, false if none is.
    bool bucketsAreRobinHood() const { return buckets_are_robin_hood; }

    size_t bufferBytes() const;
    size_t bufferSizeInCells() const;

    /// The cell offset that `JoinProbe::findKeys` records for a key that is not found: one past the last cell. The used flags
    /// per key have a slot there, so the match step sets a flag without checking whether the row matched.
    size_t missingKeyOffset() const { return bufferSizeInCells() + 1; }
    /// The size of the used flags per key: one flag per cell offset and one at `missingKeyOffset`.
    size_t keyFlagsSize() const { return missingKeyOffset() + 1; }
    size_t cellSize() const;

    /// Replaces the map of a `key32` or `key64` table with a direct-addressed one if its keys span a small enough range. That
    /// map reads keys relative to the key range. Sets the key type and the key range of the table, and returns whether it did.
    bool tryConvertToFixedHashMap(bool is_signed);

    /// For a direct-addressed map, returns a function that tells which values of a column are keys of the map, for a
    /// `SharedFixedHashTableRuntimeFilter`. For any other map, returns an empty function.
    std::function<ColumnPtr(const ColumnWithTypeAndName &)> makeSharedRuntimeFilter(bool build_signed) const;

    /// The buckets and keys that `clearInParallel` splits. An `ASOF` cell owns a heap lookup, so a large `ASOF` map is worth
    /// clearing in parallel. Zero for any other map, and for a map whose buckets share one buffer.
    struct ParallelClearWork
    {
        size_t buckets = 0;
        size_t keys = 0;
    };
    ParallelClearWork parallelClearWork() const;

    /// Clears the buckets of an `ASOF` map in `num_jobs` jobs and passes them to `schedule`.
    /// `schedule` must run them before the table is destroyed.
    void clearInParallel(size_t num_jobs, absl::FunctionRef<void(std::function<void()>)> schedule);

    /// Frees the maps. The table stays, so that a concurrent `keys` stays valid.
    void releaseMaps();

    /// The map of the key type `TYPE`, which must be `type()`.
    template <Type TYPE, typename Maps>
    const JoinMapOf<TYPE, Maps> & mapOf() const
    {
        chassert(key_type == TYPE);
        return typedMap<TYPE, Maps>(*this);
    }

private:
    template <Type TYPE, typename Maps, typename Self>
    static auto & typedMap(Self & self)
    {
        return *std::get<Maps>(self.maps).template mapOf<TYPE>();
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

    [[noreturn]] static void throwKeysNotRecoverable(Type type);

    /// Does `insert` for the key type `TYPE`, which must be `type()`. Keys and row lists go to `arena`.
    template <Type TYPE, typename Maps>
    BuildResult
    insertTyped(const InsertSettings & settings, const InsertRows & rows, Arena & arena, absl::FunctionRef<void(size_t)> add_bytes);

    /// What `insertSlots` calls for the key type and map of a block. The map behind each `void *` is the `JoinMapOf` of that
    /// key type and map.
    struct InsertFunctions
    {
        JoinBuild::InsertKeysFn insert_keys[2]; /// Indexed by `row_list`.
        size_t (*slot_bytes)(const BuildSlots & slots, const void * map, size_t slot);
        size_t (*reserve_on_first_insert)(BuildSlots & slots, void * map, size_t slot);
    };

    /// Inserts the rows `per_slot[slot]` of each slot under the slot's lock, and calls `add_bytes` as `insert` does. `args` is
    /// complete except for `args.rows`, which this sets for each slot. Returns `result` after each slot ORs in its `is_inserted`
    /// and ANDs in its `all_values_unique`.
    BuildResult insertSlots(
        const InsertFunctions & functions,
        JoinBuild::InsertKeysArgs args,
        std::span<const ScatteredBlock::Selector> per_slot,
        BuildResult result,
        absl::FunctionRef<void(size_t)> add_bytes);

    template <Type TYPE, typename Maps>
    size_t scanNotJoinedTyped(
        Cursor & cursor,
        size_t share,
        size_t num_shares,
        const JoinStuff::JoinUsedFlags & used_flags,
        size_t max_rows,
        PaddedPODArray<UInt64> & words) const;

    template <Type TYPE, typename Maps, typename F>
    size_t forEachCellTyped(Cursor & cursor, size_t max_rows, F && f) const
    {
        using Map = JoinMapOf<TYPE, Maps>;
        using Iterator = typename Map::const_iterator;

        const auto & map_ptr = std::get<Maps>(maps).template mapOf<TYPE>();
        if (!map_ptr)
            return 0;
        const Map & map = *map_ptr;

        if (!cursor.has_value())
            cursor = std::make_any<Iterator>(map.begin());
        Iterator & it = std::any_cast<Iterator &>(cursor);

        size_t rows = 0;
        for (auto end = map.end(); it != end;)
        {
            rows += f(it->getKey(), it->getMapped());
            ++it;
            if (rows >= max_rows)
                break;
        }
        return rows;
    }

    template <Type TYPE, typename Maps>
    detail::KeyLayout makeKeyLayout(std::span<const std::optional<size_t>> key_outputs) const
    {
        using Map = JoinMapOf<TYPE, Maps>;
        using KeyGetter = typename KeyGetterForType<TYPE, std::remove_cvref_t<Map>>::Type;

        chassert(key_outputs.size() == key_sizes.size());

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

    Type key_type;
    JoinKeyRange key_range;
    Sizes key_sizes;
    HashJoin::MapsVariant maps;
    /// One arena per build stream, for the strings of string keys and the nodes of the row ref lists. `Arena` is unsynchronized,
    /// and calls of one stream never overlap. Neither kind of allocation needs contiguity or rollback, so a key and its list can
    /// be in different arenas. The arenas are behind pointers so that `releaseArenas` can move them out and free them alone.
    std::vector<std::unique_ptr<Arena>> arenas;
    BuildSlots slots;
    std::atomic<size_t> num_keys = 0;
    /// See `takesAmacLookup` and `bucketsAreRobinHood`.
    bool takes_amac_lookup = false;
    bool buckets_are_robin_hood = false;
};

}
