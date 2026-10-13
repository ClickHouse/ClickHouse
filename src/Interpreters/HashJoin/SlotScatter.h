#pragma once

#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Columns/IColumn.h>

namespace DB
{

/// Scatters one clause's right-table rows into per-slot selectors, by the bucket that `maps` puts each key in.
/// Defined for `HashJoin::MapsOne`, `MapsAll`, `MapsAsof` and `MapsSet`.
template <typename Maps>
std::vector<ScatteredBlock::Selector> scatterBlockBySlot(
    HashJoin::Type type,
    const Maps & maps,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    size_t num_slots);

}
