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
/// Supported types: integers (UInt/Int 8/16/32/64/128/256), Float32/64
/// (IEEE-754 total order; NaN canonicalizes to all-`0xFF`; ±0.0 collapse),
/// Decimal32/64/128/256, Date / Date32 / DateTime / DateTime64, String
/// (`'\0'` escaped to `'\0\x01'` + `'\0\x00'` terminator), FixedString,
/// UUID, Nullable(T) (1-byte null flag — non-NULL=0x00, NULL=0x01;
/// NULL sorts AFTER non-NULL, matching `IColumn::compareAt` with
/// `nulls_direction=1` and the Float NaN-as-all-`0xFF` sentinel).
/// Compound keys concatenate per-column encodings; each per-column
/// encoding is prefix-free, so the concatenation is order-preserving.
/// Decoding is not provided — the probe path only compares.

/// Encode all rows of `columns` into `out[row]`, column-outer (one
/// dispatch per column, tight row loop per type). `permutation`,
/// if non-null, drives iteration order so `out[i]` is the encoded form
/// of row `(*permutation)[i]`. Throws BAD_ARGUMENTS if any encoded row
/// exceeds `max_size`; `out` is left in an indeterminate state on throw.
/// Misuse (mismatched column sizes or an invalid permutation) raises
/// LOGICAL_ERROR.
void encodeBlock(
    const Columns & columns,
    const IColumn::Permutation * permutation,
    size_t max_size,
    std::vector<String> & out);

}
