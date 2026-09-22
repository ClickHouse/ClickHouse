#pragma once

#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Columns/IColumn.h>

namespace DB
{

/// Needed only for the fixed-range maps, which route in cache-line-sized blocks and so depend on the
/// cell size, hence on the mapped type.
enum class MapsKind : uint8_t
{
    One,
    All,
    Asof,
    Set,
};

struct SlotScatter
{
    std::vector<ScatteredBlock::Selector> selectors;
    std::vector<Columns> dense_keys;
};

/// Scatters one clause's right-table rows into per-slot selectors. For narrow fixed-size keys it
/// also gathers the key columns per slot, so the insert loop reads them sequentially.
SlotScatter scatterBlockBySlot(
    HashJoin::Type type,
    MapsKind maps_kind,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    size_t num_slots);

}
