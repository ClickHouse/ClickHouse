#pragma once

#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/JoinUtils.h>

#include <atomic>
#include <concepts>

/// What an emplace method of the hash join build implements, and the arguments for inserting the rows of one slot. A new
/// emplace method includes this header, not `InsertKeys.h`.
namespace DB::JoinBuild
{

/// The rows of one slot: the range `[first, first + size)`, or, with `rows` set, the rows `rows[0..size)`.
struct SlotRows
{
    size_t first = 0;
    const UInt64 * rows = nullptr;
    size_t size = 0;
};

/// The insert of the rows of one slot: what it reads and where it writes. The caller holds the slot's lock.
struct InsertKeysArgs
{
    /// For `insertKeys<TYPE, Maps, ...>`, these point to `JoinMapOf<TYPE, Maps>` and `JoinKeyGetterOf<TYPE, Maps>`.
    void * map = nullptr;
    void * key_getter = nullptr;
    SlotRows rows;
    ConstNullMapPtr null_map = nullptr;
    const JoinCommon::JoinMask & join_mask;
    Arena & pool;
    bool enable_prefetch = false;
    /// The insert adds its count of new keys here, also when it throws: the keys added before the throw stay in the map.
    std::atomic<size_t> & num_keys;
    /// For a key getter that builds many keys at once (`PreparedJoinKeys`): the skip bytes of the block, and whether the slot
    /// scatter has built the keys of the rows already.
    const UInt8 * skip = nullptr;
    bool keys_are_prebuilt = false;

    /// What storing a row reads. The `ASOF` fields are set only for `MapsAsof`.
    UInt32 block_no = 0;
    bool any_take_last_row = false;
    const IColumn * asof_column = nullptr;
    TypeIndex asof_type{};
    ASOFJoinInequality asof_inequality{};
};

using InsertKeysFn = BuildResult (*)(const InsertKeysArgs &);

/// An emplace method emplaces the key of every row of `args.rows`, except the rows with a NULL key and the rows the ON-section
/// mask filters out. It calls `store(ind, emplace_result)` for the block row `ind` right after each emplace, before the next
/// one: an emplace can move cells, so an emplace result is valid only until the next emplace. The store calls `isInserted` of
/// the emplace result and, except for a set map, `getMapped`. `row_list` tells whether `args.rows.rows` is set. Returns whether
/// it skipped a NULL key.
///
/// `run` must be `ALWAYS_INLINE`: the store captures the locals of `insertKeys` by reference, and they stay in registers only
/// when `run` is inlined into it.
template <typename Emplace>
concept EmplaceMethod = requires(const InsertKeysArgs & args)
{
    {
        Emplace::template run<HashJoin::Type::key64, HashJoin::MapsOne, /*row_list=*/false>(args, [](size_t, auto &) { })
    } -> std::same_as<bool>;
};

}
