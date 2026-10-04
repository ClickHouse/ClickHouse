#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>

#include <functional>


namespace DB
{

/// getLeastSupertype + related column changes with an option to use variant as common type
ColumnWithTypeAndName getLeastSuperColumn(const VectorWithMemoryTracking<const ColumnWithTypeAndName *> & columns, bool use_variant_as_common_type = false);

/// Derives the header that `num_siblings` independently optimized branches of one relation can be
/// united under, starting from `reference`, the columns of one of them. A column stays Const only
/// where every branch is Const with the same value, since a converting step can turn a Const into a
/// full column but never into another branch's Const value. An aggregate-state Const never stays
/// Const even when the branches agree: comparing those values as `Field` throws when the aggregate
/// function type names differ, which sibling branches may legitimately do.
/// `lookup` returns branch `sibling`'s column, or nullptr when it has none; it gets both the position
/// and the name because callers differ in which of the two identifies the same column for them.
/// `materialized` reports whether any column lost its constness.
ColumnsWithTypeAndName reconcileConstness(
    const ColumnsWithTypeAndName & reference,
    size_t num_siblings,
    const std::function<const ColumnWithTypeAndName *(size_t sibling, size_t position, const String & name)> & lookup,
    bool * materialized = nullptr);

}
