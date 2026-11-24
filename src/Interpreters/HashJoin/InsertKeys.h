#pragma once

#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/RowRefs.h>
#include <Common/ProfileEvents.h>
#include <base/scope_guard.h>

#include <atomic>
#include <type_traits>

namespace ProfileEvents
{
extern const Event JoinPackedStringKeysInline;
extern const Event JoinPackedStringKeysHashed;
}

/// The build insert of the hash join: the emplace step, `insertKeys`, which stores each row that the emplace step emplaces,
/// and the part of `HashJoinTable::insert` that depends on the key type.
namespace DB::JoinBuild
{

/// The rows of one insert: the range `[first, first + size)`, or, with `rows` set, the increasing rows `rows[0..size)`.
struct BuildRows
{
    size_t first = 0;
    const UInt64 * rows = nullptr;
    size_t size = 0;
};

/// One insert into one map: what it reads besides the map and the key getter, and where it writes. No other insert into the
/// map runs meanwhile.
struct InsertKeysArgs
{
    BuildRows rows;
    ConstNullMapPtr null_map = nullptr;
    const JoinCommon::JoinMask & join_mask;
    Arena & pool;
    bool enable_prefetch = false;
    /// The insert adds its count of new keys here, also when it throws: the keys added before the throw stay in the map.
    std::atomic<size_t> & num_keys;
    /// For a key getter that builds many keys at once (`PreparedJoinKeys`): the skip bytes of the block.
    const UInt8 * skip = nullptr;

    /// What storing a row reads. The `ASOF` fields are set only for `MapsAsof`.
    UInt32 block_no = 0;
    bool any_take_last_row = false;
    const IColumn * asof_column = nullptr;
    TypeIndex asof_type{};
    ASOFJoinInequality asof_inequality{};
};

/// Emplaces the selected rows in order, skipping NULL keys and rows excluded by the ON mask. Each call to `store` must
/// finish before the next emplace, which may move cells. Returns whether a NULL key was skipped.
/// Inline the loop so the values that `store` captures from `insertKeys` can stay in registers.
template <typename Maps, bool row_list, typename Map, typename KeyGetter, typename Store>
ALWAYS_INLINE bool emplaceWithPrefetch(Map & map, KeyGetter & key_getter_, const InsertKeysArgs & args, Store && store)
{
    /// Copy the getter when that is cheap: the loop reloads a getter behind a reference after every out-of-line `RowRefList::insert`.
    std::conditional_t<std::is_trivially_copyable_v<KeyGetter>, KeyGetter, KeyGetter &> key_getter = key_getter_;

    /// Locals, so that the stores in the loop do not make it reload them.
    const size_t size = args.rows.size;
    const size_t first [[maybe_unused]] = args.rows.first;
    const UInt64 * const list [[maybe_unused]] = args.rows.rows;
    const ConstNullMapPtr null_map = args.null_map;
    const JoinCommon::JoinMask & join_mask = args.join_mask;
    Arena & pool = args.pool;
    const auto row_of = [&](size_t i) __attribute__((always_inline)) -> size_t
    {
        if constexpr (row_list)
            return list[i];
        else
            return first + i;
    };

    constexpr bool can_prefetch = join_prefetch_supported<KeyGetter, Map>;

    /// One thread inserts into the map, so its size can be read here.
    bool use_prefetch = false;
    if constexpr (can_prefetch)
        use_prefetch = shouldUseJoinPrefetch(args.enable_prefetch, &map);

    bool skipped_null_key = false;

    /// A prepared key getter builds the keys of many rows at once, and builds no key for a row that the skip bytes of the
    /// block mark. Its keys are prepared before the prefetcher is created, so that the prefetcher times only the lookups
    /// when it calibrates its look-ahead. A row list is in ascending order.
    constexpr bool prepared = PreparedKeyGetter<KeyGetter>;
    [[maybe_unused]] const UInt8 * const skip = args.skip;
    if constexpr (prepared)
    {
        if constexpr (row_list)
            key_getter.prepareAscendingRows({list, size}, skip);
        else
            key_getter.prepareRange({first, first + size}, skip);

        /// The loop below skips the rows with a NULL key by the skip bytes. As with the other key getters, such a row still
        /// keeps the block. ALL and ASOF maps keep every block anyway.
        if constexpr (std::is_same_v<Maps, HashJoin::MapsOne>)
        {
            UInt8 any_null = 0;
            if (null_map)
                for (size_t i = 0; i < size; ++i)
                    any_null |= (*null_map)[row_of(i)];
            skipped_null_key = any_null != 0;
        }
    }

    auto prefetcher = makeJoinPrefetcher(
        use_prefetch,
        size,
        [&](size_t k) __attribute__((always_inline))
        {
            if constexpr (can_prefetch)
            {
                const size_t row = row_of(k);
                if (prepared && skip[row])
                    return;
                /// A partitioned view has no descriptors before `freeze`, so it prefetches through its maps.
                if constexpr (requires { map.prefetchForInsert(key_getter.getKeyHolder(row, pool)); })
                    map.prefetchForInsert(key_getter.getKeyHolder(row, pool));
                else
                    map.prefetch(key_getter.getKeyHolder(row, pool));
            }
        });

    for (size_t i = 0; i < size; ++i)
    {
        if constexpr (can_prefetch)
            prefetcher.prefetchAt(i);

        const size_t ind = row_of(i);

        if constexpr (prepared)
        {
            if (skip[ind])
                continue;
        }
        else
        {
            chassert(!null_map || ind < null_map->size());
            if (null_map && (*null_map)[ind])
            {
                skipped_null_key = true;
                continue;
            }

            if (join_mask.isRowFiltered(ind))
                continue;
        }

        auto emplace_result = key_getter.emplaceKey(map, ind, pool);
        store(ind, emplace_result);
    }
    return skipped_null_key;
}

/// Inserts the rows `args.rows` into `map`, and stores each row right after the emplace of its key. The `is_inserted` it returns
/// comes from these rows only.
///
/// Kept out of line: with the libc++ 23 headers the inliner folds the loop into its caller, which then overlaps fewer of its
/// independent cache misses with join software prefetch on (TPC-H Q9 1.3x slower at 4 to 16 threads on aarch64).
template <typename Maps, bool row_list, typename Map, typename KeyGetter>
NO_INLINE BuildResult insertKeys(Map & map, KeyGetter & key_getter, const InsertKeysArgs & args)
{
    using Mapped = typename Maps::MappedType;

    /// Locals, so that the stores and calls of the loop do not make it reload them from `args`.
    [[maybe_unused]] const UInt32 block_no = args.block_no;
    [[maybe_unused]] const bool any_take_last_row = args.any_take_last_row;
    [[maybe_unused]] Arena & pool = args.pool;
    [[maybe_unused]] const IColumn * const asof_column = args.asof_column;
    [[maybe_unused]] const TypeIndex asof_type = args.asof_type;
    [[maybe_unused]] const ASOFJoinInequality asof_inequality = args.asof_inequality;

    /// The store writes `result` only for a key that is already in the map. The loop has no register left for it, so a write on
    /// every row would cost a load and a store.
    BuildResult result;
    /// Also counts the keys inserted before a throw, as the caller does for the bytes.
    size_t new_keys = 0;
    SCOPE_EXIT({ args.num_keys.fetch_add(new_keys, std::memory_order_relaxed); });

    const bool skipped_null_key = emplaceWithPrefetch<Maps, row_list>(
        map,
        key_getter,
        args,
        [&]([[maybe_unused]] size_t ind, auto & emplace_result) __attribute__((always_inline))
        {
            new_keys += emplace_result.isInserted();

            if constexpr (std::is_same_v<Maps, HashJoin::MapsAsof>)
            {
                Mapped * time_series_map = &emplace_result.getMapped();
                if (emplace_result.isInserted())
                    time_series_map = new (time_series_map) Mapped(createAsofRowRef(asof_type, asof_inequality));
                (*time_series_map)->insert(*asof_column, block_no, ind);
            }
            else if constexpr (std::is_same_v<Maps, HashJoin::MapsOne>)
            {
                if (emplace_result.isInserted())
                    new (&emplace_result.getMapped()) Mapped(block_no, ind);
                else if (any_take_last_row)
                {
                    new (&emplace_result.getMapped()) Mapped(block_no, ind);
                    result.is_inserted = true;
                }
            }
            else if constexpr (std::is_same_v<Maps, HashJoin::MapsAll>)
            {
                if (emplace_result.isInserted())
                    new (&emplace_result.getMapped()) Mapped(block_no, ind);
                else
                {
                    /// A single ref is stored inline in the value of the hash table; the first duplicate
                    /// switches the value to a pointer to an arena-allocated list of refs.
                    emplace_result.getMapped().insert(RowRef(block_no, ind).encode(), pool);
                    result.all_values_unique = false;
                }
            }
            else
            {
                /// A set map holds no reference to a right row, so there is nothing to do beyond adding the key.
                /// The key is still counted: `~HashJoin` statistics and the runtime filter read the key count.
                static_assert(SetJoinMaps<Maps>);
            }
        });
    if constexpr (std::is_same_v<Maps, HashJoin::MapsOne> || SetJoinMaps<Maps>)
        result.is_inserted |= new_keys != 0;
    /// NULL keys are not inserted into the hash table; their block is kept for RIGHT and FULL joins. A set map is never used by
    /// one of those.
    if constexpr (!SetJoinMaps<Maps>)
        result.is_inserted |= skipped_null_key;
    return result;
}

}

namespace DB
{

template <typename Maps>
BuildResult
HashJoinTable::insert(const InsertSettings & settings, const InsertRows & rows, size_t stream, absl::FunctionRef<void(size_t)> add_bytes)
{
    /// An insert may move the cells of a partition, and the AMAC lookup reads them from where `freezeForProbing` listed them.
    chassert(amac_support == AmacSupport::Unsupported, "A hash join table that the AMAC lookup may take got an insert after `freezeForProbing`");
    return dispatchType([&]<Type TYPE>() { return insertTyped<TYPE, Maps>(settings, rows, arena(stream), add_bytes); });
}

template <HashJoin::Type TYPE, typename Maps>
BuildResult HashJoinTable::insertTyped(
    const InsertSettings & settings, const InsertRows & rows, Arena & arena, absl::FunctionRef<void(size_t)> add_bytes)
{
    /// The single form of the table, or its partitioned form, whose `emplace` routes each key into its partition: the
    /// parallel layout fed by one build stream, and the `Join` engine never.
    if constexpr (!std::is_void_v<JoinPartitionedFormOf<TYPE, Maps>>)
    {
        if (isPartitioned())
            return insertInto<TYPE, Maps>(partitionedView<TYPE, Maps>(), settings, rows, arena, add_bytes);
    }
    return insertInto<TYPE, Maps>(singleView<TYPE, Maps>(), settings, rows, arena, add_bytes);
}

template <HashJoin::Type TYPE, typename Maps, typename Map>
BuildResult HashJoinTable::insertInto(
    Map & map, const InsertSettings & settings, const InsertRows & rows, Arena & arena, absl::FunctionRef<void(size_t)> add_bytes)
{
    using KeyGetter = typename KeyGetterForType<TYPE, Map>::Type;
    constexpr bool is_asof_join = std::is_same_v<Maps, HashJoin::MapsAsof>;

    KeyGetter key_getter = createJoinKeyGetter<KeyGetter, is_asof_join>(rows.key_columns, key_sizes);

    JoinBuild::InsertKeysArgs args{
        .rows = {},
        .null_map = rows.null_map,
        .join_mask = rows.join_mask,
        .pool = arena,
        .enable_prefetch = settings.enable_software_prefetch,
        .num_keys = num_keys,
        .block_no = rows.stored_block_no,
        .any_take_last_row = settings.any_take_last_row,
    };
    if constexpr (is_asof_join)
    {
        args.asof_column = rows.key_columns.back();
        args.asof_type = *settings.asof_type;
        args.asof_inequality = settings.asof_inequality;
    }
    if constexpr (PreparedKeyGetter<KeyGetter>)
        args.skip = key_getter.skipOrAllSelected(
            buildRowSkipData(rows.null_map, rows.join_mask, key_getter.skip_buffer, 0, key_getter.num_rows));

    /// One thread inserts into the table, and no other call allocates in `arena` meanwhile. The guard also counts what was
    /// allocated before a throw: the rows before it stay in the map, and `StorageJoin` keeps serving them.
    const size_t arena_bytes_before = arena.allocatedBytes();
    const size_t buffer_bytes_before = map.getBufferSizeInBytes();
    SCOPE_EXIT({
        const size_t buffer_bytes = map.getBufferSizeInBytes();
        add_bytes(arena.allocatedBytes() - arena_bytes_before + buffer_bytes - buffer_bytes_before);
        if constexpr (is_join_partitioned_view<Map>)
            publishPartitionCapacity(map.commonCapacity(), buffer_bytes);
    });

    const ScatteredBlock::Selector & selector = rows.selector;
    BuildResult result;
    if (selector.isContinuousRange())
    {
        const auto range = selector.getRange();
        args.rows = {.first = range.first, .size = range.second - range.first};
        result = JoinBuild::insertKeys<Maps, /*row_list=*/false>(map, key_getter, args);
    }
    else
    {
        const auto & indexes = selector.getIndexes().getData();
        args.rows = {.rows = indexes.data(), .size = indexes.size()};
        result = JoinBuild::insertKeys<Maps, /*row_list=*/true>(map, key_getter, args);
    }
    if constexpr (SetJoinMaps<Maps>)
        result.is_inserted &= settings.must_keep_right_blocks;
    else if constexpr (!std::is_same_v<Maps, HashJoin::MapsOne>)
        result.is_inserted = true;

    if constexpr (requires { key_getter.hashed_keys; })
    {
        ProfileEvents::increment(ProfileEvents::JoinPackedStringKeysInline, key_getter.emplaced_keys - key_getter.hashed_keys);
        ProfileEvents::increment(ProfileEvents::JoinPackedStringKeysHashed, key_getter.hashed_keys);
    }
    return result;
}

}
