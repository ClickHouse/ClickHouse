#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/HashJoin/LeftBlockState.h>

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
    const UInt64 * rows = nullptr;
    size_t size = 0;
};

/// What `findKeys` writes besides the match words, and which rows it visits.
enum class FindKeysMode : uint8_t
{
    Words,
    /// Stops after the row at which the right rows of the found keys reach `max_output_rows`.
    WordsUpToRowLimit,
    /// Also the cell offset of each key, for a join with a used flag per key.
    WordsAndKeyOffsetsUpToRowLimit,
    /// Visits the rows of the list `rows.rows`, not a range.
    WordsOfRowList,
};

/// One key search: what it reads and where it writes.
struct FindKeysArgs
{
    const HashJoinTable & table;
    /// The key columns of the disjunct, read-only: the same object as `disjunct.columns`.
    const JoinOnKeyColumns & keys;
    DisjunctState & disjunct;
    ProbeRows rows;
    bool enable_prefetch = false;
    /// The search writes the result of block row `ind` to `words[ind - base]`, and with
    /// `FindKeysMode::WordsAndKeyOffsetsUpToRowLimit` also to `key_offsets[ind - base]`.
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
/// `fast_path` assumes there is no such row. A block is searched in several calls of `run`, so a key getter that is costly to
/// build may be kept and used again in the later calls, as `keyGetterOf` does.
/// `retires_rows_in_order`: whether the method calls `on_row` in increasing `i`. The single pass and the row limit of
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

/// Not inlined: it runs once per block and disjunct, so one copy serves every key search of the key getter type.
template <typename KeyGetter, bool is_asof_join>
NO_INLINE std::shared_ptr<void> makeKeyGetter(const FindKeysArgs & args)
{
    return std::make_shared<KeyGetter>(
        createJoinKeyGetter<KeyGetter, is_asof_join>(args.keys.key_columns, args.table.keySizes(), args.table.keyRange()));
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

/// The key getter of the key columns `args.keys`, kept in `args.disjunct` for the block. Its first use in the block
/// builds it, and every later use in the block gets the same getter. The lookup methods that search the batches of one
/// disjunct share it, so every method must use the getter type `KeyGetterForType<TYPE, const JoinMapOf<TYPE, Maps>>::Type`.
template <typename KeyGetter, bool is_asof_join>
ALWAYS_INLINE KeyGetter & keptKeyGetter(const FindKeysArgs & args)
{
    chassert(&args.keys == &args.disjunct.columns);
    std::shared_ptr<void> & kept = args.disjunct.key_getter;
    if (!kept)
        kept = makeKeyGetter<KeyGetter, is_asof_join>(args);
    return *static_cast<KeyGetter *>(kept.get());
}

/// The skip bytes of the disjunct of `args` for every row of the block, or null when no row is skipped (see
/// `JoinOnKeyColumns::buildRowSkipData`). The first search that reads them builds them. The later searches of the block
/// read the same bytes.
inline const UInt8 * skipBytesOf(const FindKeysArgs & args)
{
    chassert(&args.keys == &args.disjunct.columns);
    DisjunctState & disjunct = args.disjunct;
    if (!disjunct.skip)
        disjunct.skip = args.keys.buildRowSkipData(disjunct.skip_buffer, 0, args.keys.join_mask_column.getSize());
    return *disjunct.skip;
}

/// The key getter of a key search. A getter that holds state for the rows of a block (`key_getter_holds_block_state`) is
/// the kept one (`keptKeyGetter`), returned by reference. Any other getter is built for this search and returned by value.
template <typename KeyGetter, bool is_asof_join>
ALWAYS_INLINE decltype(auto) keyGetterOf(const FindKeysArgs & args)
{
    /// The block is searched in batches. A new getter would pack the keys of the block, or fill its buffers or its cache,
    /// again for every batch.
    if constexpr (key_getter_holds_block_state<KeyGetter>)
        return keptKeyGetter<KeyGetter, is_asof_join>(args);
    /// By value: the stores in the search loop could make the loop reload a getter that it reads through a reference.
    else
        return createJoinKeyGetter<KeyGetter, is_asof_join>(args.keys.key_columns, args.table.keySizes(), args.table.keyRange());
}

/// Prepares the keys of a row list in a prepared key getter (`PreparedJoinKeys`). The list is in ascending
/// order. Drops the keys that the getter prepared before, also for an empty list, because the getter may be a kept one.
template <PreparedKeyGetter KeyGetter>
void prepareListedRows(KeyGetter & key_getter, const ProbeRows & rows, const UInt8 * skip)
{
    key_getter.prepareRange({}, nullptr);
    key_getter.prepareAscendingRows({rows.rows, rows.size}, skip);
}

/// Returns the `findKeys` of `key_type` that searches with `Lookup`, or null if `Lookup` has none: for a key type that it
/// does not support, or for a mode that stops at the row limit when `Lookup` does not retire rows in order. Defined in
/// `FindKeys.h`. Each lookup method declares the instances of its maps `extern` and instantiates them in one file per map.
template <typename Maps, LookupMethod Lookup>
FindKeysFn selectFindKeys(HashJoin::Type key_type, FindKeysMode mode, bool fast_path);

}
