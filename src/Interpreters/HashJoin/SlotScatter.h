#pragma once

#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Columns/IColumn.h>

namespace DB
{

/// Needed only for the direct-addressed maps, which route in cache-line-sized blocks and so depend
/// on the cell size, hence on the mapped type.
enum class MapsKind : uint8_t
{
    One,
    All,
    Asof,
};

struct SlotScatter
{
    std::vector<ScatteredBlock::Selector> selectors;
    std::vector<Columns> dense_keys;
};

/// Lets the scatter be instantiated per key type instead of per (kind, strictness, mapped type).
/// `RepMap` stands in for the clause's map - any mapped type will do except for a direct-addressed
/// one, see `MapsKind`. Routing forwards to the map's own statics, so the two cannot disagree.
template <HashJoin::Type TYPE, typename RepMap> // NOLINT(readability-identifier-naming)
struct SlotScatterTraits
{
    using Map = RepMap;
    using KeyGetter = KeyGetterForType<TYPE, RepMap, false>::Type;

    template <typename K>
    static size_t hash(const K & key) { return RepMap::hash(key); }

    template <typename K>
    static size_t bucketRoutingHash(const K & key, size_t hash_value) { return RepMap::bucketRoutingHash(key, hash_value); }

    static size_t getBucketFromHash(size_t hash_value) { return RepMap::getBucketFromHash(hash_value); }
};

/// Scatters one clause's right-table rows into per-slot selectors. For narrow fixed-size keys it
/// also gathers the key columns per slot, so the insert loop reads them sequentially.
SlotScatter scatterBlockBySlot(
    HashJoin::Type type,
    MapsKind maps_kind,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    size_t num_slots,
    bool is_asof);

}
