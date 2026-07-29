#pragma once

#include <Columns/IColumn.h>
#include <Common/PODArray.h>

namespace DB
{

/// The fixed-width join-key fast path encodes a condition's key values once into UInt64 values
/// whose unsigned order reproduces the column's `compareAt(..., nan_direction_hint = 1)` order
/// exactly, including equality. The join's hot loops then compare plain integers instead of
/// calling the virtual comparator.

/// Whether the column has a fixed-width encoding (`Nullable` probes its nested column); lets
/// detection and build code decide encoded-vs-generic per column without encoding anything.
/// Guaranteed to agree with `tryAppendEncodedKeys` (both run the same type dispatch).
bool isJoinKeyColumnEncodable(const IColumn & column);

/// Append the column's keys, encoded and XOR-ed with `flip_mask` (all-ones folds a descending
/// sort direction into the unsigned order). The dispatch is on the column type: the order the
/// encoding must reproduce is the column's own `compareAt`, so it covers exactly the types
/// stored in these columns (integers, Date/DateTime/Enum/Bool over them, floats, Decimal32/64,
/// DateTime64). Nullable encodes its nested column: a NULL row's cell gets no sentinel, so the
/// caller must exclude NULL-keyed rows from comparisons (IEJoin's validity mask does). Returns
/// false when the column has no fixed-width encoding (the caller then keeps the generic
/// comparator).
bool tryAppendEncodedKeys(const IColumn & column, UInt64 flip_mask, PaddedPODArray<UInt64> & out);

}
