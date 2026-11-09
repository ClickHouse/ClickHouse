#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/HashJoinTable.h>

#include <concepts>
#include <memory>

/// What a lookup method of the hash join probe implements, and the arguments of a key search. A new lookup method includes
/// this header, not `FindKeys.h`.
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
    bool enable_prefetch = false;
    /// The search writes the result of block row `ind` to `words[ind - base]`, and to `key_offsets[ind - base]` if
    /// `key_offsets` is set.
    size_t base = 0;
    MatchWord * words = nullptr;
    size_t * key_offsets = nullptr;
    size_t max_output_rows = 0;
};

/// Returns how many rows it visited: `rows.size`, unless the row limit stopped it.
using FindKeysFn = size_t (*)(const FindKeysArgs &);

/// A lookup method has a `run` that visits every row of `args.rows`, finds its key, and calls `on_row(i, ind, find_result)`
/// for the position `i` and the block row `ind`. `on_row` may return false to stop after that row. `run` returns how many
/// rows it visited. A row with a NULL key, or one that the ON-section mask filters out, gets a result that is not found;
/// `fast_path` assumes there is no such row.
/// `run` has a fifth template argument, `keep_key_getter`, false by default. `findKeys` sets it, because it searches once per
/// batch. The method may then keep a key getter that is costly to build and use it again in the later batches, as
/// `keyGetterOf` does.
/// `retires_rows_in_order`: whether the method calls `on_row` in increasing `i`. A fused function and the row limit of
/// `findKeys` need that, because they count the output in row order.
template <typename Lookup>
concept LookupMethod = requires(const FindKeysArgs & args)
{
    { Lookup::retires_rows_in_order } -> std::convertible_to<bool>;
    /// Whether the method has a `run` for this key type and map.
    { Lookup::template supports<HashJoin::Type::key64, HashJoin::MapsOne> } -> std::convertible_to<bool>;
    {
        Lookup::template run<HashJoin::Type::key64, HashJoin::MapsOne, true, false>(args, [](size_t, size_t, const auto &) { })
    } -> std::same_as<size_t>;
};

/// Not inlined: it runs once per call and disjunct, so one copy serves every key search of the key getter type.
template <typename KeyGetter, bool is_asof_join>
NO_INLINE std::shared_ptr<void> makeKeyGetter(const FindKeysArgs & args)
{
    return std::make_shared<KeyGetter>(
        createJoinKeyGetter<KeyGetter, is_asof_join>(args.keys.key_columns, args.keys.key_sizes, args.table.keyRange()));
}

/// The key getter of a single LowCardinality key. It deduplicates the lookups of a block per dictionary index through its own
/// cache.
template <typename T>
inline constexpr bool is_low_cardinality_join_key_getter = false;
template <typename BaseMethod, typename Mapped>
inline constexpr bool is_low_cardinality_join_key_getter<LowCardinalityKeyGetterForJoin<BaseMethod, Mapped>> = true;

/// The key getters that hold state for the rows of a block: the keys that `HashMethodKeysFixed` packed, the buffers of a
/// getter that builds the keys of many rows at once (`PreparedJoinKeys`), and the lookup cache of a LowCardinality key. Every
/// other key getter only points into the key columns.
template <typename KeyGetter>
constexpr bool key_getter_holds_block_state
    = detail::PacksKeysIntoBlob<KeyGetter> || PreparedKeyGetter<KeyGetter> || is_low_cardinality_join_key_getter<KeyGetter>;

/// The key getter of the key columns `args.keys`, kept in `JoinOnKeyColumns::kept_key_getter`. Its first use in a call
/// builds it, and the later uses in the call get the same getter. The lookup methods that search the batches of one
/// disjunct share it, so every method must use the getter type `KeyGetterForType<TYPE, const JoinMapOf<TYPE, Maps>>::Type`.
template <typename KeyGetter, bool is_asof_join>
ALWAYS_INLINE KeyGetter & keptKeyGetter(const FindKeysArgs & args)
{
    if (!args.keys.kept_key_getter)
        args.keys.kept_key_getter = makeKeyGetter<KeyGetter, is_asof_join>(args);
    return *static_cast<KeyGetter *>(args.keys.kept_key_getter.get());
}

/// The key getter of a key search. With `keep_key_getter`, a getter that holds state for the rows of a block
/// (`key_getter_holds_block_state`) is the kept one (`keptKeyGetter`), returned by reference. Any other getter is built for
/// this search and returned by value.
template <typename KeyGetter, bool is_asof_join, bool keep_key_getter>
ALWAYS_INLINE decltype(auto) keyGetterOf(const FindKeysArgs & args)
{
    /// The block is searched in batches. A new getter would pack the keys of the block, or fill its buffers or its cache,
    /// again for every batch.
    if constexpr (keep_key_getter && key_getter_holds_block_state<KeyGetter>)
        return keptKeyGetter<KeyGetter, is_asof_join>(args);
    /// By value: the stores in the search loop could make the loop reload a getter that it reads through a reference.
    else
        return createJoinKeyGetter<KeyGetter, is_asof_join>(args.keys.key_columns, args.keys.key_sizes, args.table.keyRange());
}

/// Prepares the keys of a row list in a prepared key getter (`PreparedJoinKeys`). The list is in ascending
/// order. Drops the keys that the getter prepared before, also for an empty list, because the getter may be a kept one.
template <PreparedKeyGetter KeyGetter>
void prepareListedRows(KeyGetter & key_getter, const ProbeRows & rows, const UInt8 * skip)
{
    PaddedPODArray<UInt64> listed_rows(rows.size);
    for (size_t i = 0; i < rows.size; ++i)
        listed_rows[i] = rows.rows[i];
    key_getter.prepareRange({}, nullptr);
    key_getter.prepareAscendingRows(listed_rows, skip);
}

/// Returns the `findKeys` of `key_type` that searches with `Lookup`, or null if `Lookup` has none: for a key type that it
/// does not support, or for a mode that stops at the row limit when `Lookup` does not retire rows in order. Defined in
/// `FindKeys.h`. Each lookup method declares the instances of its maps `extern` and instantiates them in one file per map.
template <typename Maps, LookupMethod Lookup>
FindKeysFn selectFindKeys(HashJoin::Type key_type, FindKeysMode mode, bool fast_path);

}
