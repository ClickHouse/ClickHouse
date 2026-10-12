#pragma once

#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Columns/IColumn.h>

namespace DB
{

struct SlotScatter
{
    std::vector<ScatteredBlock::Selector> selectors;
    std::vector<Columns> dense_keys;
};

/// Scatters one clause's right-table rows into per-slot selectors, by the bucket that `maps` puts each key in.
/// For narrow fixed-size keys it also gathers the key columns per slot, so the insert loop reads them sequentially.
/// Defined for `HashJoin::MapsOne`, `MapsAll`, `MapsAsof` and `MapsSet`.
template <typename Maps>
SlotScatter scatterBlockBySlot(
    HashJoin::Type type,
    const Maps & maps,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    size_t num_slots);

}
