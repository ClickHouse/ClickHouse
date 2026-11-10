#pragma once

#include <Interpreters/HashJoin/EmplaceMethod.h>
#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/RowRefs.h>
#include <Common/ProfileEvents.h>
#include <base/scope_guard.h>

#include <type_traits>

namespace ProfileEvents
{
extern const Event JoinPackedStringKeysInline;
extern const Event JoinPackedStringKeysHashed;
}

/// The build insert of the hash join: the emplace method in use, `insertKeys`, which stores each row that any emplace method
/// emplaces, and the part of `HashJoinTable::insert` that depends on the key type.
namespace DB::JoinBuild
{

/// Emplaces in row order, with the adaptive software prefetch of the build.
struct PrefetchingEmplace
{
    template <HashJoin::Type TYPE, typename Maps, bool row_list, typename Store>
    ALWAYS_INLINE static bool run(const InsertKeysArgs & args, Store && store)
    {
        using Map = JoinMapOf<TYPE, Maps>;
        using KeyGetter = JoinKeyGetterOf<TYPE, Maps>;

        Map & map = *static_cast<Map *>(args.map);
        /// Copy the getter when that is cheap: the loop reloads a getter behind a reference after every out-of-line `RowRefList::insert`.
        std::conditional_t<std::is_trivially_copyable_v<KeyGetter>, KeyGetter, KeyGetter &> key_getter
            = *static_cast<KeyGetter *>(args.key_getter);

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

        /// Software prefetch during the build phase.
        constexpr bool can_prefetch = join_prefetch_supported<KeyGetter, Map>;

        /// A two-level map skips the size check of `shouldUseJoinPrefetch`: summing its buckets would race with the resizes that
        /// other threads do under their own slot locks.
        bool use_prefetch = false;
        if constexpr (can_prefetch)
            use_prefetch = Map::NUM_BUCKETS > 1 ? args.enable_prefetch : shouldUseJoinPrefetch(args.enable_prefetch, &map);

        bool skipped_null_key = false;

        /// A prepared key getter builds the keys of many rows at once, and builds no key for a row that the skip bytes of the
        /// block mark. Its keys are prepared before the prefetcher is created, see `PreparedJoinKeys::prepareRows`, unless the
        /// slot scatter has built them. A row list is in ascending order.
        constexpr bool prepared = PreparedKeyGetter<KeyGetter>;
        [[maybe_unused]] const UInt8 * const skip = args.skip;
        if constexpr (prepared)
        {
            if (!args.keys_are_prebuilt)
            {
                if constexpr (row_list)
                {
                    PaddedPODArray<UInt64> listed_rows(size);
                    for (size_t i = 0; i < size; ++i)
                        listed_rows[i] = list[i];
                    key_getter.prepareAscendingRows(listed_rows, skip);
                }
                else
                    key_getter.prepareRange({first, first + size}, skip);
            }

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

                /// Check condition for right table from ON section
                if (join_mask.isRowFiltered(ind))
                    continue;
            }

            auto emplace_result = key_getter.emplaceKey(map, ind, pool);
            store(ind, emplace_result);
        }
        return skipped_null_key;
    }
};

/// Inserts the rows `args.rows` of one slot, and stores each row right after the emplace of its key. The `is_inserted` it
/// returns comes from these rows only.
template <HashJoin::Type TYPE, typename Maps, bool row_list, EmplaceMethod Emplace>
NO_INLINE BuildResult insertKeys(const InsertKeysArgs & args)
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

    const bool skipped_null_key = Emplace::template run<TYPE, Maps, row_list>(
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
    /// A new key always stores its row.
    if constexpr (std::is_same_v<Maps, HashJoin::MapsOne>)
        result.is_inserted |= new_keys != 0;
    /// NULL keys are not inserted into the hash table; their block is kept for RIGHT and FULL joins. A set map is never used by
    /// one of those.
    if constexpr (!SetJoinMaps<Maps>)
        result.is_inserted |= skipped_null_key;
    return result;
}

/// The getter for a loop over the rows of `getter`: a copy, or `getter` itself if it cannot be copied or is a `keysN` getter,
/// which holds the packed keys of the whole block.
template <typename KeyGetter>
ALWAYS_INLINE decltype(auto) loopKeyGetter(KeyGetter & getter)
{
    using Getter = std::remove_const_t<KeyGetter>;
    if constexpr (std::is_copy_constructible_v<Getter> && !detail::PacksKeysIntoBlob<Getter>)
    {
        /// The copy keeps the virtual calls of a getter from making the loop reload its fields only if the copy does not
        /// escape. An out-of-line copy constructor builds an object that escapes. The inlined move builds one that does not.
        Getter copy = getter;
        return Getter(std::move(copy));
    }
    else
        return getter;
}

/// Returns the rows of `selector` for each of the `num_slots` slots, which must be a power of two. A row goes to the slot of
/// the bucket that `map` places its key in, by `map.hash` and `getBucketOfKey`.
/// A key getter that builds many keys at once (`PreparedJoinKeys`) builds the key of every row of `selector` into
/// `prebuilt_keys` here, and reads them from there afterwards: the rows are routed by these keys, and the slot inserts read
/// them too.
template <typename Map, typename KeyGetter>
NO_INLINE std::vector<ScatteredBlock::Selector> scatterBySlot(
    const Map & map,
    KeyGetter & key_getter_,
    const ScatteredBlock::Selector & selector,
    size_t num_slots,
    [[maybe_unused]] PaddedPODArray<UInt128> & prebuilt_keys)
{
    if constexpr (requires { KeyGetter::has_pre_computed_hashes; })
        static_assert(!KeyGetter::has_pre_computed_hashes, "Bucket routing assumes the map computes the hash it places by");

    if constexpr (PreparedKeyGetter<KeyGetter>)
        key_getter_.buildAllKeys(selector, prebuilt_keys);

    decltype(auto) key_getter = loopKeyGetter(key_getter_);

    /// Nothing here outlives the call: the key holders are read for their hash, never persisted.
    Arena scratch_pool;

    const size_t num_rows = selector.size();

    PODArray<UInt32> row_to_slot(num_rows);
    std::vector<size_t> counts(num_slots, 0);
    for (size_t i = 0; i < num_rows; ++i)
    {
        auto key_holder = key_getter.getKeyHolder(selector[i], scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);

        const size_t bucket = getBucketOfKey<Map>(key, map.hash(key));
        /// The slot count is a power of two, so this is `bucket % num_slots`.
        const auto slot = static_cast<UInt32>(bucket & (num_slots - 1));
        row_to_slot[i] = slot;
        ++counts[slot];
    }

    std::vector<ScatteredBlock::Selector::IndexesPtr> indexes;
    indexes.reserve(num_slots);
    for (size_t slot = 0; slot < num_slots; ++slot)
    {
        auto column = ScatteredBlock::Selector::Indexes::create();
        column->getData().reserve(counts[slot]);
        indexes.push_back(std::move(column));
    }

    for (size_t i = 0; i < num_rows; ++i)
        indexes[row_to_slot[i]]->getData().push_back(selector[i]);

    std::vector<ScatteredBlock::Selector> result;
    result.reserve(num_slots);
    for (auto & column : indexes)
        result.emplace_back(std::move(column));

    return result;
}

}

namespace DB
{

template <typename Maps>
BuildResult
HashJoinTable::insert(const InsertSettings & settings, const InsertRows & rows, size_t stream, absl::FunctionRef<void(size_t)> add_bytes)
{
    return dispatchType([&]<Type TYPE>() { return insertTyped<TYPE, Maps>(settings, rows, arena(stream), add_bytes); });
}

template <HashJoin::Type TYPE, typename Maps>
BuildResult HashJoinTable::insertTyped(
    const InsertSettings & settings, const InsertRows & rows, Arena & arena, absl::FunctionRef<void(size_t)> add_bytes)
{
    using Map = JoinMapOf<TYPE, Maps>;
    using KeyGetter = JoinKeyGetterOf<TYPE, Maps>;
    constexpr bool is_asof_join = std::is_same_v<Maps, HashJoin::MapsAsof>;

    Map & map = typedMap<TYPE, Maps>(*this);
    /// One getter serves the routing and every slot, because building a `keysN` getter packs the keys of the whole block.
    KeyGetter key_getter = createJoinKeyGetter<KeyGetter, is_asof_join>(rows.key_columns, key_sizes);

    std::vector<ScatteredBlock::Selector> scattered;
    std::span<const ScatteredBlock::Selector> per_slot(&rows.selector, 1);
    /// The keys that the scatter builds for a key getter that builds many keys at once. The slot inserts read them.
    [[maybe_unused]] PaddedPODArray<UInt128> prebuilt_keys;
    /// Only the parallel layout has more than one slot, and only its maps have more than one bucket.
    if constexpr (Map::NUM_BUCKETS > 1)
    {
        if (slots.size() > 1)
        {
            scattered = JoinBuild::scatterBySlot(map, key_getter, rows.selector, slots.size(), prebuilt_keys);
            per_slot = scattered;
        }
    }
    else
        chassert(slots.size() == 1);

    JoinBuild::InsertKeysArgs args{
        .map = &map,
        .key_getter = &key_getter,
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
    {
        /// The skip bytes of the block, for the inserts of all slots.
        args.skip = key_getter.skipOrAllSelected(
            buildRowSkipData(rows.null_map, rows.join_mask, key_getter.skip_buffer, 0, key_getter.num_rows));
        args.keys_are_prebuilt = !prebuilt_keys.empty();
    }

    BuildResult result;
    /// For ALL and ASOF join always insert values. A set map keeps no reference into the block, so unless
    /// the block has to be kept for another algorithm the caller drops it.
    if constexpr (SetJoinMaps<Maps>)
        result.is_inserted = settings.must_keep_right_blocks;
    else
        result.is_inserted = !std::is_same_v<Maps, HashJoin::MapsOne>;

    /// A new emplace method replaces `PrefetchingEmplace` here.
    const InsertFunctions functions{
        .insert_keys
        = {&JoinBuild::insertKeys<TYPE, Maps, false, JoinBuild::PrefetchingEmplace>,
           &JoinBuild::insertKeys<TYPE, Maps, true, JoinBuild::PrefetchingEmplace>},
        .slot_bytes
        = [](const BuildSlots & slots_, const void * map_, size_t slot) { return slots_.slotBytes(*static_cast<const Map *>(map_), slot); },
        .reserve_on_first_insert
        = [](BuildSlots & slots_, void * map_, size_t slot) { return slots_.reserveOnFirstInsert(*static_cast<Map *>(map_), slot); },
    };
    result = insertSlots(functions, args, per_slot, result, add_bytes);

    /// The slots share one key getter, so these counts cover all of them.
    if constexpr (requires { key_getter.hashed_keys; })
    {
        ProfileEvents::increment(ProfileEvents::JoinPackedStringKeysInline, key_getter.emplaced_keys - key_getter.hashed_keys);
        ProfileEvents::increment(ProfileEvents::JoinPackedStringKeysHashed, key_getter.hashed_keys);
    }
    return result;
}

}
