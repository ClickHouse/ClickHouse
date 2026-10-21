#pragma once

/// Host-side encoder and decoder for the `COLUMNAR_V1` frame (UserDefinedWebAssemblyColumnarWire.h).
/// Fixed-width columns and null maps are copied as is; const columns are sent as one row.

#include <Functions/UserDefined/UserDefinedWebAssemblyColumnarWire.h>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <Common/VectorWithMemoryTracking.h>

#include <cstring>
#include <span>
#include <utility>

namespace DB::ColumnarWire
{

inline void writeFrameHeader(uint8_t * dst, uint32_t num_rows, uint32_t num_cols)
{
    constexpr uint16_t reserved = 0;
    std::memcpy(dst,      &FRAME_MAGIC,    4);
    std::memcpy(dst + 4,  &FRAME_VERSION,  2);
    std::memcpy(dst + 6,  &reserved,       2);
    std::memcpy(dst + 8,  &num_rows,       4);
    std::memcpy(dst + 12, &num_cols,       4);
}

/// Alignment padding as (offset, length) pairs. `writeColData` does not write it,
/// so a caller with an unzeroed buffer must zero these ranges.
using PaddingGaps = VectorWithMemoryTracking<std::pair<uint64_t, uint64_t>>;

/// Throw INCORRECT_DATA unless `type` can be encoded. Called at CREATE FUNCTION time.
void validateColumnarWireSupportedType(const DataTypePtr & type, bool is_nested = false);

/// Rebuild every COL_LOWCARD dictionary in `col` of type `type` (top level, inside `Const`, or a
/// `Variant` alternative) to only the entries its indexes reference, copied byte for byte. `filter`
/// and `cut` keep the whole dictionary, so without this the guest could read values from rows it
/// was not given, such as rows a row policy removed. Nested LowCardinality is materialized row by
/// row and needs no help. Returns `col` itself when every entry is referenced.
ColumnPtr compactLowCardinalityDictionaries(const ColumnPtr & col, const DataTypePtr & type);

/// Data block of a COL_COMPLEX column's `n` rows, written at `dst`.
void writeComplexData(const IColumn & col, uint32_t n, uint8_t * dst);

/// Compute the layout of one column starting at `write_cursor` and fill `desc`.
/// Returns the next free offset; alignment padding is recorded in `gaps` when given.
uint64_t buildColDescriptor(
    const IColumn * col,
    bool is_const,
    bool is_nullable,
    uint32_t num_rows,
    uint64_t write_cursor,
    ColDescriptor & desc,
    PaddingGaps * gaps = nullptr);

/// Serialize a column into `buf` at the positions `desc` gives.
void writeColData(
    const IColumn * col,
    bool is_nullable,
    uint32_t num_rows,
    const ColDescriptor & desc,
    std::span<uint8_t> buf);

/// Decode one column of a frame from its descriptor; offsets in `desc` are absolute in `buf`.
MutableColumnPtr readColumnFromDesc(
    std::span<const uint8_t> buf,
    const ColDescriptor & desc,
    uint32_t num_rows,
    const DataTypePtr & result_type);

/// Decode a single-column frame a guest returned, checking it holds `expected_rows` rows.
MutableColumnPtr readColumnarOutput(
    std::span<const uint8_t> buf,
    const DataTypePtr & result_type,
    size_t expected_rows);

}
