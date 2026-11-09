#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/HashJoin/LookupMethod.h>

/// The key search of the hash join probe: `PrefetchingLookup`, and `findKeys`, which writes what any lookup method finds.
namespace DB::JoinProbe
{

/// Looks the keys up in row order, with the adaptive software prefetch of the probe.
struct PrefetchingLookup
{
    static constexpr bool retires_rows_in_order = true;
    template <HashJoin::Type, typename>
    static constexpr bool supports = true;

    template <HashJoin::Type TYPE, typename Maps, bool fast_path, bool row_list, bool keep_key_getter = false, typename OnRow>
    ALWAYS_INLINE static size_t run(const FindKeysArgs & args, OnRow && on_row)
    {
        using Map = const JoinMapOf<TYPE, Maps>;
        using KeyGetter = typename KeyGetterForType<TYPE, Map>::Type;
        using FindResult = typename KeyGetter::FindResult;
        constexpr bool is_asof_join = std::is_same_v<Maps, HashJoin::MapsAsof>;

        chassert(row_list == (args.rows.rows != nullptr));
        const Map & map = args.table.mapOf<TYPE, Maps>();
        const JoinOnKeyColumns & keys = args.keys;
        decltype(auto) key_getter = keyGetterOf<KeyGetter, is_asof_join, keep_key_getter>(args);

        /// Locals, so that the stores in `on_row` do not make the loop reload them.
        const size_t size = args.rows.size;
        const size_t first [[maybe_unused]] = args.rows.first;
        const UInt32 * const list [[maybe_unused]] = args.rows.rows;
        const auto row_of = [&](size_t i) __attribute__((always_inline)) -> size_t
        {
            if constexpr (row_list)
                return list[i];
            else
                return first + i;
        };

        /// The skip pointer is a local so that it can stay in a register across the calls in
        /// the loop body (see `JoinOnKeyColumns::buildRowSkipData`).
        const UInt8 * skip_data = nullptr;
        IColumn::Filter skip_buffer;
        if constexpr (!fast_path)
        {
            if (size)
                skip_data = keys.buildRowSkipData(skip_buffer, row_of(0), row_of(size - 1) + 1 - row_of(0));
        }

        Arena pool;

        /// Software prefetch during the probe phase.
        constexpr bool can_prefetch = join_prefetch_supported<KeyGetter, Map>;

        bool use_prefetch = false;
        if constexpr (can_prefetch)
            use_prefetch = shouldUseJoinPrefetch(args.enable_prefetch, &map);

        /// A prepared key getter builds the keys of many rows at once, and builds no key for a row that its skip bytes mark.
        /// So this loop always gives it skip bytes. Its keys are prepared before the prefetcher is created (see
        /// `PreparedJoinKeys::prepareRows`).
        constexpr bool prepared = PreparedKeyGetter<KeyGetter>;
        if constexpr (prepared)
        {
            skip_data = key_getter.skipOrAllSelected(skip_data);
            if constexpr (row_list)
                prepareListedRows(key_getter, args.rows, skip_data);
            else
                key_getter.prepareRange({first, first + size}, skip_data);
        }

        auto prefetcher = makeJoinPrefetcher(
            use_prefetch,
            size,
            [&](size_t k) __attribute__((always_inline))
            {
                if constexpr (can_prefetch)
                {
                    const size_t row = row_of(k);
                    if (prepared && !fast_path && skip_data[row])
                        return;
                    map.prefetch(key_getter.getKeyHolder(row, pool));
                }
            });

        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (can_prefetch)
                prefetcher.prefetchAt(i);

            const size_t ind = row_of(i);

            bool skip_row = false;
            if constexpr (!fast_path)
                skip_row = (prepared || skip_data) && skip_data[ind];

            const FindResult result = skip_row ? FindResult() : key_getter.findKey(map, ind, pool);
            if constexpr (std::is_same_v<decltype(on_row(i, ind, result)), bool>)
            {
                if (!on_row(i, ind, result))
                    return i + 1;
            }
            else
                on_row(i, ind, result);
        }
        return size;
    }
};

template <typename Maps, typename FindResult>
ALWAYS_INLINE MatchWord matchWordOf(const FindResult & result)
{
    if (!result.isFound())
        return 0;
    if constexpr (std::is_same_v<Maps, HashJoin::MapsAsof>)
        return reinterpret_cast<UInt64>(result.getMapped().get());
    else if constexpr (SetJoinMaps<Maps>)
        return 1;
    else if constexpr (std::is_same_v<Maps, HashJoin::MapsAll>)
        return result.getMapped().word;
    else
        return result.getMapped().encode();
}

/// Writes only the slots of the rows that `Lookup` visits, when it visits them, so `Lookup` may visit the rows in any order.
template <HashJoin::Type TYPE, typename Maps, bool fast_path, FindKeysMode mode, LookupMethod Lookup>
NO_INLINE size_t findKeys(const FindKeysArgs & args)
{
    constexpr bool row_list = mode == FindKeysMode::WordsOfRowList;
    constexpr bool records_key_offsets
        = mode == FindKeysMode::WordsAndKeyOffsets || mode == FindKeysMode::WordsAndKeyOffsetsUpToRowLimit;
    constexpr bool up_to_row_limit = mode == FindKeysMode::WordsUpToRowLimit || mode == FindKeysMode::WordsAndKeyOffsetsUpToRowLimit;
    static_assert(!up_to_row_limit || std::is_same_v<Maps, HashJoin::MapsAll>, "Only a `RowRefList` word holds several rows");
    static_assert(!up_to_row_limit || Lookup::retires_rows_in_order, "The row limit counts the output in row order");

    /// Locals, so that the stores through them do not make the loop reload them.
    MatchWord * const words = args.words;
    size_t * const key_offsets [[maybe_unused]] = args.key_offsets;
    const size_t base [[maybe_unused]] = args.base;
    [[maybe_unused]] const size_t missing_key_offset = records_key_offsets ? args.table.missingKeyOffset() : 0;
    [[maybe_unused]] size_t output_rows_left = args.max_output_rows;

    return Lookup::template run<TYPE, Maps, fast_path, row_list, /*keep_key_getter=*/true>(
        args,
        [&](size_t i, size_t ind [[maybe_unused]], const auto & result) __attribute__((always_inline))
        {
            /// A range starts at block row `base`, so the position `i` of a row is its slot.
            const size_t slot = row_list ? ind - base : i;
            const MatchWord word = matchWordOf<Maps>(result);
            words[slot] = word;
            if constexpr (records_key_offsets)
                key_offsets[slot] = result.isFound() ? result.getOffset() : missing_key_offset;
            if constexpr (up_to_row_limit)
            {
                /// The row count in a word saturates at `RowRefList::COUNT_SAT`, so for a key with more rows it is too low. The
                /// search may then look up more rows than it needs, but the match step still stops at the limit.
                const size_t found_rows = refWordIsInline(word) ? 1 : (word >> RowRefList::COUNT_SHIFT) & RowRefList::COUNT_SAT;
                if (found_rows >= output_rows_left)
                    return false;
                output_rows_left -= found_rows;
                return true;
            }
        });
}

/// See `LookupMethod.h`. Only `MapsOne` and `MapsAll` record key offsets, only `MapsAll` stops at the row limit, and only a
/// join with several disjuncts, which never has an ASOF map, visits a row list.
template <typename Maps, LookupMethod Lookup>
FindKeysFn selectFindKeys(HashJoin::Type key_type, FindKeysMode mode, bool fast_path)
{
    constexpr bool mapped = std::is_same_v<Maps, HashJoin::MapsOne> || std::is_same_v<Maps, HashJoin::MapsAll>;
    constexpr bool maps_all = std::is_same_v<Maps, HashJoin::MapsAll>;
    constexpr bool asof = std::is_same_v<Maps, HashJoin::MapsAsof>;

    const auto of_type = [&]<HashJoin::Type TYPE>() -> FindKeysFn
    {
        if constexpr (!Lookup::template supports<TYPE, Maps>)
            return nullptr;
        else
        {
            const auto with = [&]<FindKeysMode m>() -> FindKeysFn
            {
                return fast_path ? &findKeys<TYPE, Maps, true, m, Lookup> : &findKeys<TYPE, Maps, false, m, Lookup>;
            };
            switch (mode)
            {
                case FindKeysMode::Words:
                    return with.template operator()<FindKeysMode::Words>();
                case FindKeysMode::WordsAndKeyOffsets:
                    if constexpr (mapped)
                        return with.template operator()<FindKeysMode::WordsAndKeyOffsets>();
                    break;
                case FindKeysMode::WordsUpToRowLimit:
                    if constexpr (!Lookup::retires_rows_in_order)
                        return nullptr;
                    else if constexpr (maps_all)
                        return with.template operator()<FindKeysMode::WordsUpToRowLimit>();
                    break;
                case FindKeysMode::WordsAndKeyOffsetsUpToRowLimit:
                    if constexpr (!Lookup::retires_rows_in_order)
                        return nullptr;
                    else if constexpr (maps_all)
                        return with.template operator()<FindKeysMode::WordsAndKeyOffsetsUpToRowLimit>();
                    break;
                case FindKeysMode::WordsOfRowList:
                    if constexpr (!asof)
                        return with.template operator()<FindKeysMode::WordsOfRowList>();
                    break;
            }
            UNREACHABLE();
        }
    };

    switch (key_type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        return of_type.template operator()<HashJoin::Type::TYPE>();
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    }
    UNREACHABLE();
}

/// Instantiated in `FindKeysMaps*.cpp`, one file per map.
extern template FindKeysFn selectFindKeys<HashJoin::MapsOne, PrefetchingLookup>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsAll, PrefetchingLookup>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsAsof, PrefetchingLookup>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsSet, PrefetchingLookup>(HashJoin::Type, FindKeysMode, bool);

}
