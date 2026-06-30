#pragma once

#include <Core/Block.h>
#include <Core/Types.h>
#include <Columns/IColumn.h>

#include <vector>
#include <string>

namespace DB::UniqueKeyEncoding
{

/// Order-preserving binary encoding for UNIQUE KEY row values.
///
/// Contract: for any two rows A, B in `columns`, `memcmp(encodeBlock(A),
/// encodeBlock(B))` agrees in sign with `IColumn::compareAt` applied
/// column-by-column in schema order.
///
/// Implementation delegates to IColumn::batchSerializeComparableIntoMemory
/// for each column type, which internally calls
/// IColumn::serializeValueIntoMemoryAsComparable per row.
/// Supported types: integers (UInt/Int 8-256),
/// Float32/64 (IEEE-754 total order), Decimal32/64/128/256,
/// Date/Date32/DateTime/DateTime64, String (NUL-escaped), FixedString,
/// UUID, Nullable(T) (1-byte null flag).
///
/// The encoding uses a column-outer loop: one virtual dispatch per column,
/// tight row loop per type. This avoids per-row virtual call overhead.

/// Encode all rows of `columns` into `out[row]`. `permutation`,
/// if non-null, drives iteration order so `out[i]` is the encoded form
/// of row `(*permutation)[i]`. Throws BAD_ARGUMENTS if any encoded row
/// exceeds `max_size`; `out` is left in an indeterminate state on throw.
void encodeBlock(
    const Columns & columns,
    const IColumn::Permutation * permutation,
    size_t max_size,
    std::vector<String> & out);

}
