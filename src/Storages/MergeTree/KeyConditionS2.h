#pragma once

/// S2 index pruning helpers for KeyCondition.
///
/// The `S2CoveringData` definition and the `<s2/s2cell_union.h>` include are
/// guarded by `#if USE_S2_GEOMETRY` so that non-S2 builds see only the
/// pimpl forward declaration from KeyCondition.h.

#include "config.h"

#include <Storages/MergeTree/KeyCondition.h>

#if USE_S2_GEOMETRY

#include <s2/s2cell_union.h>

namespace DB
{

/// Concrete definition of the pimpl struct forward-declared in KeyCondition.h.
/// Lives here (rather than in KeyCondition.h) so that S2 geometry headers
/// do not leak into every translation unit that includes KeyCondition.h.
struct KeyCondition::RPNElement::S2CoveringData
{
    S2CellUnion covering;
    String function_name;   /// for toString()
};

/// Try to build a FUNCTION_S2_COVERING atom from the function node `func`.
///
/// Called from KeyCondition::extractAtomFromTree at two structural positions:
///   1. Inside the `num_args == 2` branch -- for `s2CellsIntersect`.
///   2. Inside the `else` branch (num_args >= 3) -- for `s2RectContains` / `s2CapContains`.
///
/// On success, fills `out` (sets `out.function`, `out.key_columns`,
/// `out.s2_covering_data`) and returns true.
/// On failure (non-constant region args, key column not found, ...) returns false.
///
/// `key_columns` must be `KeyCondition::getKeyColumns()`.
/// `s2_max_covering_cells` is the value of the `s2_max_covering_cells` setting
/// clamped to int at KeyCondition construction time.
bool tryAnalyzeS2Covering(
    const RPNBuilderFunctionTreeNode & func,
    const KeyCondition::ColumnIndices & key_columns,
    int s2_max_covering_cells,
    KeyCondition::RPNElement & out);

/// Evaluate a previously-built FUNCTION_S2_COVERING atom against a granule's
/// key ranges.
///
/// Returns BoolMask{intersects, true} -- `can_be_false` is always true because
/// the covering is conservative (may produce false positives).
///
/// If `element.s2_covering_data` is null (should not happen in correct usage),
/// returns BoolMask{true, true} (conservative fallback).
BoolMask evalS2Covering(
    const KeyCondition::RPNElement & element,
    const Hyperrectangle & hyperrectangle);

} // namespace DB

#endif // USE_S2_GEOMETRY
