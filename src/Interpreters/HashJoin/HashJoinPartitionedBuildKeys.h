#pragma once

#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Common/HashTable/HashTable.h>

/// The key types of a partitioned build, shared by its fill pass and its rounds.
namespace DB::JoinPartitionedBuild::detail
{

/// How a key type is hashed, routed and scattered.
enum class KeyKind : uint8_t
{
    /// Not built in partitions: the range maps and the `LowCardinality` maps.
    None,
    /// `key8` to `key64`: the key column is the table key.
    Number,
    /// `key_string`, `key_fixed_string`: the bytes go through the multi-source scatter, the saved hash through the fixed one.
    String,
    /// `hashed`, `key_packed_string`: the keys are built once in the fill pass (`buildAllKeys`).
    Prepared,
    /// `keys32` to `keys256`: the key columns are packed for the hash, and packed again in the scatter.
    Packed,
};

template <HashJoin::Type TYPE>
consteval KeyKind keyKindOf()
{
    using enum HashJoin::Type;
    if constexpr (TYPE == key8 || TYPE == key16 || TYPE == key32 || TYPE == key64)
        return KeyKind::Number;
    else if constexpr (TYPE == key_string || TYPE == key_fixed_string)
        return KeyKind::String;
    else if constexpr (TYPE == hashed || TYPE == key_packed_string)
        return KeyKind::Prepared;
    else if constexpr (TYPE == keys32 || TYPE == keys64 || TYPE == keys128 || TYPE == keys256)
        return KeyKind::Packed;
    else
        return KeyKind::None;
}

/// The hash functor of a `HashTable`, so that the fill pass hashes a key exactly as the map does, without a map object.
template <typename Key, typename Cell, typename Hash, typename Grower, typename Allocator>
Hash mapHashOf(const HashTable<Key, Cell, Hash, Grower, Allocator> *);

template <typename Map>
using MapHash = decltype(mapHashOf(static_cast<const Map *>(nullptr)));

/// Calls `f.template operator()<Maps>()` with the maps kind of `table`.
template <typename F>
decltype(auto) dispatchMaps(const HashJoinTable & table, F && f)
{
    if (table.holds<HashJoin::MapsOne>())
        return f.template operator()<HashJoin::MapsOne>();
    if (table.holds<HashJoin::MapsAll>())
        return f.template operator()<HashJoin::MapsAll>();
    if (table.holds<HashJoin::MapsAsof>())
        return f.template operator()<HashJoin::MapsAsof>();
    return f.template operator()<HashJoin::MapsSet>();
}

}
