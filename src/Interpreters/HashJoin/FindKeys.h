#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/HashJoinTable.h>

#include <concepts>

/// The key search of the hash join probe, and everything a new lookup method needs.
namespace DB::JoinProbe
{

/// The rows of a block that one key search visits: the range `[first, first + size)`, or, with `rows` set, the
/// increasing rows `rows[0..size)`.
struct ProbeRows
{
    size_t first = 0;
    const UInt32 * rows = nullptr;
    size_t size = 0;
};

/// What the key search found for one left row in one ON disjunct: 0 if the key is not there, is NULL, or the ON section
/// masks the row out. Otherwise the `RowRef` word (`MapsOne`), the `RowRefList` word (`MapsAll`), the address of the key's
/// `SortedLookupVectorBase` (`MapsAsof`), or 1 (`MapsSet`).
using MatchWord = UInt64;

/// What `findKeys` writes besides the match words, and which rows it visits.
enum class FindKeysMode : uint8_t
{
    Words,
    /// Also the cell offset of each key, for a join with a used flag per key.
    WordsAndKeyOffsets,
    /// Stops after the row at which the right rows of the found keys reach `max_output_rows`.
    WordsUpToRowLimit,
    WordsAndKeyOffsetsUpToRowLimit,
    /// Visits the rows of the list `rows.rows`, not a range.
    WordsOfRowList,
};

/// One key search: what it reads and where it writes.
struct FindKeysArgs
{
    const HashJoinTable & table;
    const JoinOnKeyColumns & keys;
    ProbeRows rows;
    bool enable_prefetch;
    /// Block row `ind` writes `words[ind - base]`, and `key_offsets[ind - base]` if `key_offsets` is set.
    size_t base;
    MatchWord * words;
    size_t * key_offsets;
    size_t max_output_rows;
};

/// Returns the rows it visited: all of `rows`, unless the row limit stopped it.
using FindKeysFn = size_t (*)(const FindKeysArgs &);

/// A lookup method visits every row of `args.rows`, finds its key, and calls `on_row(i, ind, find_result)` for the position `i`
/// and the block row `ind`; `on_row` may return false to stop after that row. It returns the rows it visited. A row with a NULL
/// key or one the ON-section mask filters out gets a result that is not found; `fast_path` assumes there is none.
/// `retires_rows_in_order`: `on_row` comes in increasing `i`. A fused function and the row limit of `findKeys` need it, because
/// they count the output in row order.
template <typename Lookup>
concept LookupMethod = requires(const FindKeysArgs & args)
{
    { Lookup::retires_rows_in_order } -> std::convertible_to<bool>;
    {
        Lookup::template run<HashJoin::Type::key64, HashJoin::MapsOne, true, false>(args, [](size_t, size_t, const auto &) { })
    } -> std::same_as<size_t>;
};

/// Looks the keys up in row order, with the adaptive software prefetch of the probe.
struct PrefetchingLookup
{
    static constexpr bool retires_rows_in_order = true;

    template <HashJoin::Type TYPE, typename Maps, bool fast_path, bool row_list, typename OnRow>
    ALWAYS_INLINE static size_t run(const FindKeysArgs & args, OnRow && on_row)
    {
        using Map = const JoinMapOf<TYPE, Maps>;
        using KeyGetter = typename KeyGetterForType<TYPE, Map>::Type;
        using FindResult = typename KeyGetter::FindResult;
        constexpr bool is_asof_join = std::is_same_v<Maps, HashJoin::MapsAsof>;

        chassert(row_list == (args.rows.rows != nullptr));
        const Map & map = args.table.mapOf<TYPE, Maps>();
        const JoinOnKeyColumns & keys = args.keys;
        KeyGetter key_getter = createJoinKeyGetter<KeyGetter, is_asof_join>(keys.key_columns, keys.key_sizes, args.table.keyRange());

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

        auto prefetcher = makeJoinPrefetcher(
            use_prefetch,
            size,
            [&](size_t k) __attribute__((always_inline))
            {
                if constexpr (can_prefetch)
                    map.prefetch(key_getter.getKeyHolder(row_of(k), pool));
            });

        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (can_prefetch)
                prefetcher.prefetchAt(i);

            const size_t ind = row_of(i);

            bool skip_row = false;
            if constexpr (!fast_path)
                skip_row = skip_data && skip_data[ind];

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

/// Writes only the slots of the row it visits, so any lookup method can drive it.
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

    return Lookup::template run<TYPE, Maps, fast_path, row_list>(
        args,
        [&](size_t i, size_t ind [[maybe_unused]], const auto & result) __attribute__((always_inline))
        {
            /// A range starts at `base`, so its position is its slot.
            const size_t slot = row_list ? ind - base : i;
            const MatchWord word = matchWordOf<Maps>(result);
            words[slot] = word;
            if constexpr (records_key_offsets)
                key_offsets[slot] = result.isFound() ? result.getOffset() : missing_key_offset;
            if constexpr (up_to_row_limit)
            {
                /// A saturated count is too low, so the search may look up more rows than it needs; the match step still stops at
                /// the limit.
                const size_t found_rows = refWordIsInline(word) ? 1 : (word >> RowRefList::COUNT_SHIFT) & RowRefList::COUNT_SAT;
                if (found_rows >= output_rows_left)
                    return false;
                output_rows_left -= found_rows;
                return true;
            }
        });
}

/// The `findKeys` of `key_type`. Only `MapsOne` and `MapsAll` record key offsets, only `MapsAll` stops at the row limit, and
/// only a join with several disjuncts, which never has an ASOF map, visits a row list.
template <typename Maps>
FindKeysFn selectFindKeys(HashJoin::Type key_type, FindKeysMode mode, bool fast_path)
{
    constexpr bool mapped = std::is_same_v<Maps, HashJoin::MapsOne> || std::is_same_v<Maps, HashJoin::MapsAll>;
    constexpr bool maps_all = std::is_same_v<Maps, HashJoin::MapsAll>;
    constexpr bool asof = std::is_same_v<Maps, HashJoin::MapsAsof>;

    const auto of_type = [&]<HashJoin::Type TYPE>() -> FindKeysFn
    {
        const auto with = [&]<FindKeysMode m>() -> FindKeysFn
        {
            return fast_path ? &findKeys<TYPE, Maps, true, m, PrefetchingLookup> : &findKeys<TYPE, Maps, false, m, PrefetchingLookup>;
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
                if constexpr (maps_all)
                    return with.template operator()<FindKeysMode::WordsUpToRowLimit>();
                break;
            case FindKeysMode::WordsAndKeyOffsetsUpToRowLimit:
                if constexpr (maps_all)
                    return with.template operator()<FindKeysMode::WordsAndKeyOffsetsUpToRowLimit>();
                break;
            case FindKeysMode::WordsOfRowList:
                if constexpr (!asof)
                    return with.template operator()<FindKeysMode::WordsOfRowList>();
                break;
        }
        UNREACHABLE();
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
extern template FindKeysFn selectFindKeys<HashJoin::MapsOne>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsAll>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsAsof>(HashJoin::Type, FindKeysMode, bool);
extern template FindKeysFn selectFindKeys<HashJoin::MapsSet>(HashJoin::Type, FindKeysMode, bool);

}
