#pragma once

/// Wire format helpers for the COLUMNAR_V1 binary format.
///
/// ── Purpose ─────────────────────────────────────────────────────────────────
/// COLUMNAR_V1 is a flat columnar encoding used by the ColumnBinary I/O format
/// and the WASM UDF ABI. It is designed for low-overhead host↔guest transfer:
/// fixed-width columns serialize as a single memcpy; variable-width columns
/// (strings) pay the unavoidable uint64_t offset conversion.
/// All functions are inline — safe to include
/// from multiple TUs.
///
/// ── Wire layout ─────────────────────────────────────────────────────────────
///
///   [ 4 B num_rows | 4 B num_cols ]       ← COLUMNAR_HEADER_BYTES = 8
///   [ ColDescriptor × num_cols    ]       ← COLUMNAR_DESC_BYTES = 40 each
///   [ column data blobs ...       ]       ← at offsets given by descriptors
///
/// ColDescriptor holds five uint64 fields (absolute byte offsets into the
/// buffer): type, null_offset, offsets_offset, data_offset, data_size.
/// Offsets are 0 when the field is absent (e.g. null_offset=0 → not nullable).
///
/// ── Column types ─────────────────────────────────────────────────────────────
///   COL_BYTES   (0) — variable-length byte strings (ColumnString)
///   COL_FIXED8/16/32/64 (1-4) — fixed-width scalars, one memcpy per column
///   COL_COMPLEX (5) — Array(T) or Tuple(T…), recursive layout; also used for
///                     Map(K, V), which is unwrapped to Array(Tuple(K, V)) at
///                     every touch point rather than getting its own tag
///   COL_VARIANT (6) — Variant(…), discriminated union with per-row offsets
///   COL_FIXEDN  (7) — fixed-width scalars of any other width (UUID, IPv6,
///                     Int128/UInt128, Decimal128/256) and FixedString(N);
///                     element width N recovered on read as data_size/num_rows
///                     (no offsets array needed since every row is N bytes)
///
/// Modifier bits (OR'd onto the base type):
///   COL_IS_NULLABLE (0x20) — null map at null_offset: u8[num_rows], 1=null 0=non-null
///   COL_IS_CONST    (0x80) — column is constant; only 1 row of data stored
///
/// ── Key design decisions ────────────────────────────────────────────────────
///
/// Minimal serialization: fixed-width columns (int, float, UUID…) are stored
/// as their raw PaddedPODArray bytes — one memcpy on both write and read.
/// There is no per-row metadata for these types.
///
/// Const column compaction: ClickHouse represents repeated values as
/// ColumnConst (a single stored value + a logical row count). COLUMNAR_V1
/// preserves this: COL_IS_CONST sets data for 1 row; the reader replicates it
/// to the full row count on decode. This avoids materializing, e.g., a million
/// identical literals just to serialize them.
///
/// Null map convention: nullable columns carry a u8[num_rows] map where
/// 1 means null and 0 means non-null — identical to ColumnNullable::getNullMapData().
/// This allows the null map to be memcpy'd directly on both read and write.
/// Zeroed memory reads as "all non-null" by default.
///
/// O(1) size precomputation: buildColDescriptor computes the exact byte size
/// of each column in O(1) without scanning rows — ColumnString uses
/// getChars().size() directly; fixed-width uses sizeOfValueIfFixed(). This
/// allows ColumnBinaryOutputFormat to pre-allocate the output buffer in a
/// single pass before writing, avoiding reallocation.
///
/// COL_BYTES wire layout omits null terminators. ColumnString internally has
/// no null terminators (see ColumnString.h); the wire matches exactly.
/// All offset arrays in the data blob (COL_BYTES, Array outer offsets,
/// recursive String/Array offsets inside COL_COMPLEX) are uint64.
///
/// ── Supported types ─────────────────────────────────────────────────────────
///
/// validateColumnarV1SupportedType is the single source of truth for which
/// ClickHouse types this wire format can represent; call it eagerly (at format
/// construction / CREATE FUNCTION time) rather than discovering a rejection
/// only once the first block is serialized. Not supported: LowCardinality (no
/// wire encoding at all — would need a dictionary + variable-width index, unlike
/// everything else here), Nullable(Array/Variant) (no wire slot for a top-level
/// null map on COL_COMPLEX/COL_VARIANT; Nullable(Tuple(...)) is supported instead,
/// via a top-level null map on COL_COMPLEX), and Variant nested inside Array/
/// Tuple. Nullable(T) nested inside Array/Tuple is supported (u8 null_map[n]
/// prepended to T's own layout, see complexDataSize/writeComplexData/the decode
/// lambda). Map(K, V) is supported with no dedicated wire encoding: it's Array
/// (Tuple(K, V)) under the hood (DataTypeMap::getNestedType() /
/// ColumnMap::getNestedColumn()), so it's unwrapped to that at every touch point
/// and goes through the existing Array/Tuple path. Any fixed-width type is
/// supported at any width (COL_FIXED8/16/32/64 for 1/2/4/8 bytes, COL_FIXEDN for
/// everything else — UUID, IPv6, Int128/UInt128, Decimal128/256), and so is
/// FixedString(N) of any length (also COL_FIXEDN). Any type kind this format has
/// no encoding for at all (Dynamic, JSON/Object, AggregateFunction(...), ...) is
/// also rejected.

#include <cstring>
#include <span>
#include <functional>
#include <utility>
#include <vector>

#include <base/unaligned.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace ColumnarV1
{

constexpr uint32_t COL_BYTES        = 0;
constexpr uint32_t COL_FIXED8       = 1;
constexpr uint32_t COL_FIXED16      = 2;  // UInt16/Int16: 2 bytes per element
constexpr uint32_t COL_FIXED32      = 3;
constexpr uint32_t COL_FIXED64      = 4;
constexpr uint32_t COL_COMPLEX      = 5;  // Array(T) / Tuple(T...) — recursive format
constexpr uint32_t COL_VARIANT      = 6;  // Variant(...) — discriminated union
constexpr uint32_t COL_FIXEDN       = 7;  // fixed-width of any other size, or FixedString(N);
                                           // element width recovered as data_size/num_rows
// Modifier flags (OR'd onto base type; base types 0–6, so bits 5-7 are free for flags).
constexpr uint32_t COL_IS_NULLABLE  = 0x20u; // Nullable(T); null_offset carries u8[row_count] null map
constexpr uint32_t COL_IS_CONST     = 0x80u;

constexpr uint32_t COLUMNAR_HEADER_BYTES = 8;
constexpr uint32_t COLUMNAR_DESC_BYTES   = 40;

struct ColDescriptor
{
    uint64_t type;
    uint64_t null_offset;
    uint64_t offsets_offset;
    uint64_t data_offset;
    uint64_t data_size;
};
static_assert(sizeof(ColDescriptor) == COLUMNAR_DESC_BYTES);

// Recursively check that `type` can round-trip through the COLUMNAR_V1 wire format,
// throwing INCORRECT_DATA immediately with the exact reason otherwise. Without this,
// callers only discover an unsupported signature (nested Nullable/Variant inside
// Array/Tuple, Map, LowCardinality, or a fixed-width type whose size isn't exactly
// 1/2/4/8 bytes such as UUID/IPv6/Int128/UInt128/Decimal128/256/FixedString) when the
// first block is actually serialized.
// is_nested is false only for the outermost call; Nullable/Variant are only
// disallowed once already inside an Array/Tuple (COL_COMPLEX), where there is no
// wire slot for a nested null map or discriminator.
inline void validateColumnarV1SupportedType(const DataTypePtr & type, bool is_nested = false)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const DataTypePtr & nested_type = nullable_type->getNestedType();
        // Top-level: only COL_BYTES/COL_FIXED*/COL_COMPLEX carry a COL_IS_NULLABLE null
        // map. COL_VARIANT already has its own per-row NULL representation (the 0xFF
        // discriminator sentinel) and has no wire slot for an additional top-level null
        // map, so Nullable(Variant(...)) is rejected. Nullable(Array(...)) is also
        // rejected, but only defensively: DataTypeArray::canBeInsideNullable() is false,
        // so ClickHouse can never actually construct this type. Nullable(Tuple(...)) is
        // real (reachable behind enable_nullable_tuple_type) and is supported via
        // buildColDescriptor/writeColData's top-level null map.
        //
        // Nested (inside Array/Tuple): complexDataSize/writeComplexData/the decode
        // lambda all support Nullable(T) generically (u8 null_map[n] prepended to T's
        // own layout), so no Array/Variant restriction applies there — Nullable(Array/
        // Variant) is still unreachable via ClickHouse's type system regardless.
        if (!is_nested
            && (typeid_cast<const DataTypeArray *>(nested_type.get())
                || typeid_cast<const DataTypeVariant *>(nested_type.get())))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1/ColumnBinary: Nullable(Array/Variant) is not supported: {}", type->getName());
        validateColumnarV1SupportedType(nested_type, is_nested);
        return;
    }
    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        if (is_nested)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1/ColumnBinary: nested Variant inside Array/Tuple is not supported: {}", type->getName());
        for (const auto & alt : variant_type->getVariants())
            validateColumnarV1SupportedType(alt, is_nested);
        return;
    }
    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        // Map(K, V) is Array(Tuple(K, V)) under the hood (DataTypeMap::getNestedType());
        // no dedicated wire encoding needed, just validate through the existing Array/
        // Tuple path.
        validateColumnarV1SupportedType(map_type->getNestedType(), is_nested);
        return;
    }
    if (typeid_cast<const DataTypeLowCardinality *>(type.get()))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1/ColumnBinary: LowCardinality is not supported: {}", type->getName());
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        validateColumnarV1SupportedType(array_type->getNestedType(), /* is_nested */ true);
        return;
    }
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        for (const auto & elem : tuple_type->getElements())
            validateColumnarV1SupportedType(elem, /* is_nested */ true);
        return;
    }
    if (typeid_cast<const DataTypeString *>(type.get()))
        return;
    if (type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
    {
        // Any fixed width is representable: COL_FIXED8/16/32/64 for exactly 1/2/4/8
        // bytes, COL_FIXEDN (buildColDescriptor) for everything else (UUID, IPv6,
        // Int128/UInt128, Decimal128/256, FixedString(N) of any length). Nested
        // (is_nested) elements never needed a tag at all — writeComplexData/decode
        // already move raw bytes sized from the DataType itself.
        return;
    }
    // Explicit deny-by-default: every type this wire format actually knows how to encode
    // returns above. Anything else (Dynamic, JSON/Object, AggregateFunction(...), and any
    // future type kind) must be rejected here too, or it silently falls through as
    // "supported" and only fails once buildColDescriptor reaches its fixed-width fallback
    // and calls sizeOfValueIfFixed()/getRawData() on a column that has neither.
    throw Exception(ErrorCodes::INCORRECT_DATA,
        "COLUMNAR_V1/ColumnBinary: type is not supported: {}", type->getName());
}

// ── COL_COMPLEX recursive helpers ────────────────────────────────────────────
//
// COL_COMPLEX data block layout (recursive, mirrors the output decoder):
//   Array(T):   uint64 offsets[n+1]  +  complexDataBlock(inner, total_elems)
//   Tuple(T..): complexDataBlock(field_0, n) + complexDataBlock(field_1, n) + ...
//   String:     uint64 offsets[n+1]  +  chars (no null terminators)
//   Fixed:      raw bytes[n * elem_bytes]

// Forward declaration — complexDataSize and writeComplexData are mutually recursive
// only via lambdas; we declare the inline wrappers here.
//
// complexDataSize/writeComplexData return/take byte *sizes* as uint64_t even though
// element counts (n) stay uint32_t: a single String or Array column can legitimately
// carry more than 4 GiB of payload in one row without needing a huge row count, so
// any uint32_t size intermediate here would silently wrap and underallocate the
// output frame.
inline uint64_t complexDataSize(const IColumn & col, uint32_t n);
inline void     writeComplexData(const IColumn & col, uint32_t n, uint8_t * dst);

inline uint64_t complexDataSize(const IColumn & col, uint32_t n)
{
    if (const auto * map_col = typeid_cast<const ColumnMap *>(&col))
        return complexDataSize(map_col->getNestedColumn(), n);
    if (const auto * arr = typeid_cast<const ColumnArray *>(&col))
    {
        uint32_t total = static_cast<uint32_t>(arr->getData().size());
        return (static_cast<uint64_t>(n) + 1u) * 8u + complexDataSize(arr->getData(), total);
    }
    if (const auto * tup = typeid_cast<const ColumnTuple *>(&col))
    {
        uint64_t sz = 0;
        for (const auto & field : tup->getColumns())
            sz += complexDataSize(*field, n);
        return sz;
    }
    if (typeid_cast<const ColumnString *>(&col))
    {
        const auto & str = assert_cast<const ColumnString &>(col);
        uint64_t chars = str.getChars().size();
        return (static_cast<uint64_t>(n) + 1u) * 8u + chars;
    }
    if (const auto * null_col = typeid_cast<const ColumnNullable *>(&col))
        // Nested Nullable(T): u8 null_map[n] prepended, then T's own complexData layout.
        return static_cast<uint64_t>(n) + complexDataSize(null_col->getNestedColumn(), n);
    if (typeid_cast<const ColumnVariant *>(&col))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1: nested Variant inside Array/Tuple is not supported in COL_COMPLEX; "
            "use a flat variant column or a different format");
    // Fixed-width fallback (ColumnVector<T>, ColumnUInt8, etc.)
    return static_cast<uint64_t>(n) * col.sizeOfValueIfFixed();
}

inline void writeComplexData(const IColumn & col, uint32_t n, uint8_t * dst)
{
    if (const auto * map_col = typeid_cast<const ColumnMap *>(&col))
    {
        writeComplexData(map_col->getNestedColumn(), n, dst);
        return;
    }
    if (const auto * arr = typeid_cast<const ColumnArray *>(&col))
    {
        const auto & ch_offs = arr->getOffsets();
        unalignedStore<uint64_t>(dst, 0ull);
        for (uint32_t i = 0; i < n; ++i)
            unalignedStore<uint64_t>(dst + (static_cast<uint64_t>(i) + 1u) * 8u, static_cast<uint64_t>(ch_offs[i]));
        uint32_t total = static_cast<uint32_t>(arr->getData().size());
        writeComplexData(arr->getData(), total, dst + (static_cast<uint64_t>(n) + 1u) * 8u);
        return;
    }
    if (const auto * tup = typeid_cast<const ColumnTuple *>(&col))
    {
        uint64_t pos = 0;
        for (const auto & field : tup->getColumns())
        {
            uint64_t field_sz = complexDataSize(*field, n);
            writeComplexData(*field, n, dst + pos);
            pos += field_sz;
        }
        return;
    }
    if (const auto * str = typeid_cast<const ColumnString *>(&col))
    {
        const auto & ch_offs = str->getOffsets();
        const auto & chars   = str->getChars();
        uint8_t  * chars_dst = dst + (static_cast<uint64_t>(n) + 1u) * 8u;
        unalignedStore<uint64_t>(dst, 0ull);
        uint64_t wire_pos = 0ull;
        uint64_t ch_pos   = 0ull;
        for (uint32_t i = 0; i < n; ++i)
        {
            uint64_t end = ch_offs[i];
            uint64_t len = end - ch_pos;
            std::memcpy(chars_dst + wire_pos, chars.data() + ch_pos, len);
            wire_pos += len;
            unalignedStore<uint64_t>(dst + (static_cast<uint64_t>(i) + 1u) * 8u, wire_pos);
            ch_pos = end;
        }
        return;
    }
    if (const auto * null_col = typeid_cast<const ColumnNullable *>(&col))
    {
        const auto & nm = null_col->getNullMapData();
        std::memcpy(dst, nm.data(), n);
        writeComplexData(null_col->getNestedColumn(), n, dst + n);
        return;
    }
    if (typeid_cast<const ColumnVariant *>(&col))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1: nested Variant inside Array/Tuple is not supported in COL_COMPLEX; "
            "use a flat variant column or a different format");
    // Fixed-width fallback
    std::memcpy(dst, col.getRawData().data(), n * col.sizeOfValueIfFixed());
}

// Compute byte layout for a single column and fill in desc.
// Returns the next free offset in the output buffer.
inline uint64_t buildColDescriptor(
    const IColumn * col,
    bool is_const,
    bool is_nullable,
    uint32_t num_rows,
    uint64_t write_cursor,
    ColDescriptor & desc)
{
    // COL_COMPLEX (Array/Tuple) carries its own top-level null map (reserved below,
    // guarded by is_nullable); unwrap here so the Variant/Array/Tuple dispatch below
    // sees the actual complex column regardless of Nullable wrapping. COL_VARIANT
    // already encodes NULL via its discriminator and is never reached here nullable
    // (validateColumnarV1SupportedType rejects Nullable(Variant(...))).
    if (const auto * top_null_col = typeid_cast<const ColumnNullable *>(col))
        col = &top_null_col->getNestedColumn();

    // Map(K, V) is Array(Tuple(K, V)) under the hood (ColumnMap::getNestedColumn());
    // unwrap so the Array branch below handles it with no dedicated wire format.
    if (const auto * map_col = typeid_cast<const ColumnMap *>(col))
        col = &map_col->getNestedColumn();

    // ── Variant column → COL_VARIANT ─────────────────────────────────────────
    // Wire layout:
    //   null_offset    → discriminators[num_rows]    uint8 (global discr; 0xFF=NULL)
    //   offsets_offset → row_offsets[num_rows]       uint32 (pos within sub-column)
    //   data_offset    → variant header:
    //     uint32 K                                   (number of present sub-variants)
    //     K × { uint8 global_discriminator           (4-byte aligned record)
    //           uint8[3] pad
    //           ColDescriptor inner_desc }           (40 bytes, abs buffer offsets)
    //     (sub-column data at positions given by inner_desc)
    if (const auto * var_col = typeid_cast<const ColumnVariant *>(col))
    {
        desc.type = COL_VARIANT | (is_const ? COL_IS_CONST : 0u);

        desc.null_offset = write_cursor;
        write_cursor += num_rows;

        write_cursor = (write_cursor + 3ull) & ~3ull;
        desc.offsets_offset = write_cursor;
        write_cursor += num_rows * sizeof(uint32_t);

        write_cursor = (write_cursor + 3ull) & ~3ull;
        desc.data_offset = write_cursor;

        // Count non-empty sub-variants.
        uint32_t k = 0;
        uint32_t num_variants = static_cast<uint32_t>(var_col->getNumVariants());
        for (uint32_t local = 0; local < num_variants; ++local)
            if (!var_col->getVariantByLocalDiscriminator(local).empty())
                ++k;

        // Reserve header: uint32 K + K × 24 bytes (discr+pad+ColDescriptor)
        write_cursor += 4u + k * (4u + COLUMNAR_DESC_BYTES);

        // Now allocate space for each non-empty sub-column.
        for (uint32_t local = 0; local < num_variants; ++local)
        {
            const IColumn & sub = var_col->getVariantByLocalDiscriminator(local);
            if (sub.empty())
                continue;
            ColDescriptor inner_desc{};
            uint32_t sub_rows = static_cast<uint32_t>(sub.size());
            write_cursor = buildColDescriptor(&sub, false, false, sub_rows, write_cursor, inner_desc);
        }

        desc.data_size = write_cursor - desc.data_offset;
        return write_cursor;
    }

    // ── Array column → COL_COMPLEX ────────────────────────────────────────────
    if (const auto * arr_col = typeid_cast<const ColumnArray *>(col))
    {
        desc.type           = COL_COMPLEX | (is_const ? COL_IS_CONST : 0u) | (is_nullable ? COL_IS_NULLABLE : 0u);
        desc.offsets_offset = 0;  // unused for Array; outer offsets are at data_offset

        if (is_nullable)
        {
            desc.null_offset = write_cursor;
            write_cursor += num_rows;
        }
        else
        {
            desc.null_offset = 0;
        }

        write_cursor = (write_cursor + 7ull) & ~7ull;
        desc.data_offset = write_cursor;

        const IColumn & nested = arr_col->getData();
        uint32_t total_elems = static_cast<uint32_t>(nested.size());

        // Sequential layout: uint64 offsets[num_rows+1] followed by nested complexData.
        desc.data_size = (num_rows + 1u) * sizeof(uint64_t) + complexDataSize(nested, total_elems);
        write_cursor  += desc.data_size;
        return write_cursor;
    }

    // ── Tuple column → COL_COMPLEX (no outer offsets, fields concatenated) ───
    if (const auto * tup_col = typeid_cast<const ColumnTuple *>(col))
    {
        desc.type           = COL_COMPLEX | (is_const ? COL_IS_CONST : 0u) | (is_nullable ? COL_IS_NULLABLE : 0u);
        desc.offsets_offset = 0;

        if (is_nullable)
        {
            desc.null_offset = write_cursor;
            write_cursor += num_rows;
            write_cursor = (write_cursor + 7ull) & ~7ull; // realign after null map
        }
        else
        {
            desc.null_offset = 0;
        }

        desc.data_offset    = write_cursor;
        desc.data_size      = complexDataSize(*tup_col, num_rows);
        write_cursor       += desc.data_size;
        return write_cursor;
    }

    const ColumnString * str_col = typeid_cast<const ColumnString *>(col);
    const ColumnNullable * null_col = typeid_cast<const ColumnNullable *>(col);

    if (null_col)
        str_col = typeid_cast<const ColumnString *>(&null_col->getNestedColumn());

    if (str_col)
    {
        uint32_t base_type = COL_BYTES | (is_nullable ? COL_IS_NULLABLE : 0u);

        desc.type = base_type | (is_const ? COL_IS_CONST : 0u);

        if (is_nullable)
        {
            desc.null_offset = write_cursor;
            write_cursor += num_rows;
        }
        else
        {
            desc.null_offset = 0;
        }

        write_cursor = (write_cursor + 7ull) & ~7ull;
        desc.offsets_offset = write_cursor;
        write_cursor += (num_rows + 1u) * sizeof(uint64_t);

        desc.data_offset = write_cursor;
        uint64_t total_chars = str_col->getChars().size();
        desc.data_size = total_chars;
        write_cursor += total_chars;
        return write_cursor;
    }

    // Unwrap nullable to get the actual element size; ColumnNullable::sizeOfValueIfFixed()
    // returns nested_size+1, which would produce the wrong ColType.
    const IColumn * inner_col = null_col ? &null_col->getNestedColumn() : col;
    uint32_t elem_size = static_cast<uint32_t>(inner_col->sizeOfValueIfFixed());
    uint32_t wire_elem_size = elem_size;
    uint32_t base_type = 0;
    if      (wire_elem_size == 1) base_type = COL_FIXED8  | (is_nullable ? COL_IS_NULLABLE : 0u);
    else if (wire_elem_size == 2) base_type = COL_FIXED16 | (is_nullable ? COL_IS_NULLABLE : 0u);
    else if (wire_elem_size == 4) base_type = COL_FIXED32 | (is_nullable ? COL_IS_NULLABLE : 0u);
    else if (wire_elem_size == 8) base_type = COL_FIXED64 | (is_nullable ? COL_IS_NULLABLE : 0u);
    // Any other width (UUID, IPv6, Int128/UInt128, Decimal128/256, FixedString(N) of
    // any length): no dedicated tag per width, element size is recovered on read as
    // data_size/num_rows (set below), so no offsets array is needed either.
    else base_type = COL_FIXEDN | (is_nullable ? COL_IS_NULLABLE : 0u);

    desc.type = base_type | (is_const ? COL_IS_CONST : 0u);
    if (is_nullable)
    {
        desc.null_offset = write_cursor;
        write_cursor += num_rows;
        write_cursor = (write_cursor + 3ull) & ~3ull; // 4-byte align data after null map
    }
    else
    {
        desc.null_offset = 0;
    }
    desc.offsets_offset = 0;
    desc.data_offset    = write_cursor;
    desc.data_size      = static_cast<uint64_t>(num_rows) * wire_elem_size;
    write_cursor       += desc.data_size;
    return write_cursor;
}

// Serialize column data into the pre-allocated buffer at the positions given by desc.
inline void writeColData(
    const IColumn * col,
    bool is_nullable,
    uint32_t num_rows,
    const ColDescriptor & desc,
    std::span<uint8_t> buf)
{
    // Unwrap Nullable up front (mirrors buildColDescriptor) and write its null map
    // before dispatching on the concrete column, so COL_COMPLEX (Array/Tuple) gets
    // the same "null map first, then the actual payload" treatment as scalars/strings.
    if (const auto * top_null_col = typeid_cast<const ColumnNullable *>(col))
    {
        col = &top_null_col->getNestedColumn();
        if (is_nullable && desc.null_offset)
        {
            const auto & nm = top_null_col->getNullMapData();
            std::memcpy(buf.data() + desc.null_offset, nm.data(), num_rows);
        }
    }

    // Map(K, V) is Array(Tuple(K, V)) under the hood; unwrap so the Array branch
    // below writes it with no dedicated wire format.
    if (const auto * map_col = typeid_cast<const ColumnMap *>(col))
        col = &map_col->getNestedColumn();

    // ── Variant column → COL_VARIANT ─────────────────────────────────────────
    if (const auto * var_col = typeid_cast<const ColumnVariant *>(col))
    {
        // Write global discriminators (NULL_DISCRIMINATOR=0xFF for null rows).
        uint8_t * disc_dst = buf.data() + desc.null_offset;
        for (uint32_t i = 0; i < num_rows; ++i)
            disc_dst[i] = var_col->globalDiscriminatorAt(i);

        // Write per-row offsets within each variant's sub-column.
        uint32_t * offs_dst = reinterpret_cast<uint32_t *>(buf.data() + desc.offsets_offset);
        const auto & row_offs = var_col->getOffsets();
        for (uint32_t i = 0; i < num_rows; ++i)
            offs_dst[i] = static_cast<uint32_t>(row_offs[i]);

        // Build variant header: K + K×{global_discriminator(4B) + ColDescriptor(20B)}.
        uint8_t * block = buf.data() + desc.data_offset;

        uint32_t num_variants = static_cast<uint32_t>(var_col->getNumVariants());

        // Count non-empty sub-variants (must match buildColDescriptor).
        uint32_t k = 0;
        for (uint32_t local = 0; local < num_variants; ++local)
            if (!var_col->getVariantByLocalDiscriminator(local).empty())
                ++k;

        std::memcpy(block, &k, 4u);
        uint8_t * record_ptr = block + 4u;

        // Track where sub-column data starts (after header).
        uint64_t sub_cursor = desc.data_offset + 4u + k * (4u + COLUMNAR_DESC_BYTES);

        for (uint32_t local = 0; local < num_variants; ++local)
        {
            const IColumn & sub = var_col->getVariantByLocalDiscriminator(local);
            if (sub.empty())
                continue;

            uint8_t global_d = var_col->globalDiscriminatorByLocal(static_cast<ColumnVariant::Discriminator>(local));
            uint32_t sub_rows = static_cast<uint32_t>(sub.size());

            ColDescriptor inner_desc{};
            sub_cursor = buildColDescriptor(&sub, false, false, sub_rows, sub_cursor, inner_desc);
            inner_desc.null_offset = sub_rows;  // repurpose null_offset to carry sub_rows for the decoder

            std::memcpy(record_ptr,     &global_d,   1u);
            std::memset(record_ptr + 1, 0,           3u);
            std::memcpy(record_ptr + 4, &inner_desc, COLUMNAR_DESC_BYTES);
            record_ptr += 4u + COLUMNAR_DESC_BYTES;

            writeColData(&sub, false, sub_rows, inner_desc, buf);
        }
        return;
    }

    // ── Array column → COL_COMPLEX ────────────────────────────────────────────
    if (const auto * arr_col = typeid_cast<const ColumnArray *>(col))
    {
        const auto & ch_offsets = arr_col->getOffsets();
        const IColumn & nested  = arr_col->getData();
        uint32_t total_elems = static_cast<uint32_t>(nested.size());

        // Sequential layout: outer offsets at data_offset, nested data immediately after.
        uint8_t * wire_outer = buf.data() + desc.data_offset;
        unalignedStore<uint64_t>(wire_outer, 0ull);
        for (uint32_t i = 0; i < num_rows; ++i)
            unalignedStore<uint64_t>(wire_outer + (i + 1u) * 8u, static_cast<uint64_t>(ch_offsets[i]));

        writeComplexData(nested, total_elems, buf.data() + desc.data_offset + (num_rows + 1u) * sizeof(uint64_t));
        return;
    }

    // ── Tuple column → COL_COMPLEX ────────────────────────────────────────────
    if (const auto * tup_col = typeid_cast<const ColumnTuple *>(col))
    {
        writeComplexData(*tup_col, num_rows, buf.data() + desc.data_offset);
        return;
    }

    const ColumnString * str_col = typeid_cast<const ColumnString *>(col);
    if (str_col)
    {
        const auto & ch_offsets = str_col->getOffsets();
        const auto & chars = str_col->getChars();

        uint8_t * wire_offsets = buf.data() + desc.offsets_offset;
        uint8_t * data_dst = buf.data() + desc.data_offset;

        unalignedStore<uint64_t>(wire_offsets, 0ull);
        uint64_t wire_pos = 0ull;
        uint64_t ch_pos = 0ull;
        for (uint32_t i = 0; i < num_rows; ++i)
        {
            uint64_t str_end = ch_offsets[i];
            uint64_t str_len = str_end - ch_pos;
            std::memcpy(data_dst + wire_pos, chars.data() + ch_pos, str_len);
            wire_pos += str_len;
            unalignedStore<uint64_t>(wire_offsets + (i + 1u) * 8u, wire_pos);
            ch_pos = str_end;
        }
        return;
    }

    const auto * raw      = col->getRawData().data();
    uint32_t     elem_sz  = static_cast<uint32_t>(col->sizeOfValueIfFixed());

    std::memcpy(buf.data() + desc.data_offset, raw, num_rows * elem_sz);
}

// Decode one column from a COLUMNAR_V1 frame given its pre-parsed descriptor.
// buf:         the complete frame buffer (all byte offsets in desc are absolute
//              from buf.data()).
// desc:        ColDescriptor for this column (read from the descriptor table).
// num_rows:    total row count from the frame header (used to size ColumnConst).
// result_type: drives type-specific decoding for COL_COMPLEX and COL_FIXED*.
inline MutableColumnPtr readColumnFromDesc(
    std::span<const uint8_t> buf,
    const ColDescriptor & desc,
    uint32_t num_rows,
    const DataTypePtr & result_type)
{
    bool is_nullable_wire = (desc.type & COL_IS_NULLABLE) != 0;
    uint32_t raw_type     = desc.type & ~(COL_IS_CONST | COL_IS_NULLABLE);
    bool is_const         = (desc.type & COL_IS_CONST) != 0;
    uint32_t rows_to_dec  = is_const ? 1u : num_rows;

    const DataTypePtr & base_type = is_nullable_wire
        ? dynamic_cast<const DataTypeNullable &>(*result_type).getNestedType()
        : result_type;

    // desc comes straight from guest/network memory and is otherwise untrusted:
    // validate data_offset/data_size against buf.size() before forming any
    // pointer from them (data_end below, and every buf.data() + desc.data_offset
    // in the branches further down), or a malformed descriptor can build an
    // out-of-bounds pointer that later bounds checks then compare against.
    if (desc.data_offset > buf.size() || desc.data_size > buf.size() - desc.data_offset)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1: column data range out of bounds: offset={}, size={}, buf={}",
            desc.data_offset, desc.data_size, buf.size());

    const uint8_t * const data_end = buf.data() + desc.data_offset + desc.data_size;

    // Recursive decoder for COL_COMPLEX (Array / Tuple / nested scalars).
    std::function<MutableColumnPtr(const uint8_t *&, const DataTypePtr &, uint32_t)> decode;
    decode = [&](const uint8_t *& p, const DataTypePtr & type, uint32_t n) -> MutableColumnPtr
    {
        if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
            return ColumnMap::create(decode(p, map_type->getNestedType(), n));
        if (const auto * arr_type = typeid_cast<const DataTypeArray *>(type.get()))
        {
            // n comes from a guest-controlled row/element count; widen before the +1 or
            // n == UINT32_MAX wraps the uint32_t addition to 0, making outer_bytes 0 and
            // trivially passing the bounds check below.
            uint64_t outer_bytes = (static_cast<uint64_t>(n) + 1u) * sizeof(uint64_t);
            if (p + outer_bytes > data_end)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX nested Array outer offsets out of bounds");
            const uint8_t * outer_offs = p;
            p += outer_bytes;
            uint32_t total_elems = static_cast<uint32_t>(unalignedLoad<uint64_t>(outer_offs + n * 8u));
            // outer_offs holds n+1 cumulative counts (offs[0]==0, offs[n]==total_elems); the
            // module fully controls these, so reject anything non-monotonic or not starting
            // at 0 before using them as ColumnArray offsets, or a crafted [0, 3, 1]-style frame
            // can make later offset differences underflow into a huge size downstream.
            if (unalignedLoad<uint64_t>(outer_offs) != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX Array offsets must start at 0");
            auto nested_col = decode(p, arr_type->getNestedType(), total_elems);
            auto offsets_col = ColumnUInt64::create(n);
            uint64_t prev_off = 0;
            for (uint32_t i = 0; i < n; ++i)
            {
                uint64_t off = unalignedLoad<uint64_t>(outer_offs + (i + 1u) * 8u);
                if (off < prev_off)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "COLUMNAR_V1: COL_COMPLEX Array offsets must be non-decreasing");
                offsets_col->getData()[i] = off;
                prev_off = off;
            }
            return ColumnArray::create(std::move(nested_col), std::move(offsets_col));
        }
        if (const auto * tup_type = typeid_cast<const DataTypeTuple *>(type.get()))
        {
            const auto & field_types = tup_type->getElements();
            Columns fields;
            fields.reserve(field_types.size());
            for (const auto & ft : field_types)
                fields.push_back(decode(p, ft, n));
            return ColumnTuple::create(std::move(fields))->assumeMutable();
        }
        if (typeid_cast<const DataTypeString *>(type.get()))
        {
            // Same overflow hazard as the Array branch above: widen before the +1.
            uint64_t off_bytes = (static_cast<uint64_t>(n) + 1u) * sizeof(uint64_t);
            if (p + off_bytes > data_end)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX String offsets out of bounds");
            const uint8_t * wire_offs = p;
            p += off_bytes;
            uint64_t total_chars = unalignedLoad<uint64_t>(wire_offs + n * 8u);
            if (p + total_chars > data_end)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX String chars out of bounds");
            const uint8_t * chars_src = p;
            p += total_chars;
            // wire_offs holds n+1 cumulative byte offsets (offs[0]==0, offs[n]==total_chars);
            // the module fully controls these, so reject anything non-monotonic before using
            // them to slice chars_src, or a crafted offset pair can underflow str_len into a
            // huge size and drive an out-of-bounds memcpy.
            if (unalignedLoad<uint64_t>(wire_offs) != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX String offsets must start at 0");
            auto col_str = ColumnString::create();
            auto & chars   = col_str->getChars();
            auto & offsets = col_str->getOffsets();
            offsets.resize(n);
            uint64_t ch_pos = 0ull;
            uint64_t prev_wire_end = 0;
            for (uint32_t i = 0; i < n; ++i)
            {
                uint64_t wire_end   = unalignedLoad<uint64_t>(wire_offs + (i + 1u) * 8u);
                uint64_t wire_start = unalignedLoad<uint64_t>(wire_offs + i * 8u);
                if (wire_start != prev_wire_end || wire_end < wire_start)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "COLUMNAR_V1: COL_COMPLEX String offsets must be non-decreasing and contiguous");
                prev_wire_end = wire_end;
                uint64_t str_len    = wire_end - wire_start;
                chars.resize(ch_pos + str_len);
                std::memcpy(chars.data() + ch_pos, chars_src + wire_start, str_len);
                ch_pos += str_len;
                offsets[i] = ch_pos;
            }
            return col_str;
        }
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            // Mirrors writeComplexData: u8 null_map[n] prepended, then the nested
            // type's own complexData layout.
            if (p + static_cast<uint64_t>(n) > data_end)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX nested Nullable null map out of bounds");
            auto null_map_col = ColumnUInt8::create(n);
            std::memcpy(null_map_col->getData().data(), p, n);
            p += n;
            auto inner = decode(p, nullable_type->getNestedType(), n);
            return ColumnNullable::create(std::move(inner), std::move(null_map_col));
        }
        if (typeid_cast<const DataTypeVariant *>(type.get()))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: nested Variant inside Array/Tuple is not supported in COL_COMPLEX; "
                "use a flat variant column or a different format");
        uint32_t elem_bytes = static_cast<uint32_t>(type->getSizeOfValueInMemory());
        if (p + static_cast<uint64_t>(n) * elem_bytes > data_end)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_COMPLEX fixed data out of bounds");
        auto col = type->createColumn();
        col->insertManyDefaults(n);
        std::memcpy(const_cast<char *>(col->getRawData().data()), p, n * elem_bytes);
        p += n * elem_bytes;
        return col;
    };

    auto maybe_nullable = [&](MutableColumnPtr inner) -> MutableColumnPtr
    {
        if (is_nullable_wire && desc.null_offset)
        {
            if (desc.null_offset + static_cast<uint64_t>(rows_to_dec) > buf.size())
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: null map out of bounds: offset={}, rows={}, buf={}",
                    desc.null_offset, rows_to_dec, buf.size());
            // Null map: 1=null, 0=non-null — identical to ColumnNullable layout; direct copy.
            auto null_col = ColumnUInt8::create(rows_to_dec);
            std::memcpy(null_col->getData().data(), buf.data() + desc.null_offset, rows_to_dec);
            return ColumnNullable::create(std::move(inner), std::move(null_col));
        }
        return inner;
    };

    MutableColumnPtr col;

    if (raw_type == COL_BYTES)
    {
        if (desc.offsets_offset + static_cast<uint64_t>(rows_to_dec + 1) * 8u > buf.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_BYTES offsets array out of bounds: offset={}, rows={}, buf={}",
                desc.offsets_offset, rows_to_dec, buf.size());
        if (desc.data_offset + desc.data_size > buf.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_BYTES data out of bounds: offset={}, size={}, buf={}",
                desc.data_offset, desc.data_size, buf.size());

        const uint8_t  * wire_offsets = buf.data() + desc.offsets_offset;
        const uint8_t  * data         = buf.data() + desc.data_offset;
        auto col_str = ColumnString::create();
        auto & chars   = col_str->getChars();
        auto & offsets = col_str->getOffsets();
        offsets.resize(rows_to_dec);
        uint64_t ch_pos = 0ull;
        for (uint32_t i = 0; i < rows_to_dec; ++i)
        {
            uint64_t wire_end   = unalignedLoad<uint64_t>(wire_offsets + (i + 1u) * 8u);
            uint64_t wire_start = unalignedLoad<uint64_t>(wire_offsets + i * 8u);
            if (wire_start > wire_end || wire_end > desc.data_size)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_BYTES invalid string offsets at row {}: [{}, {}), data_size={}",
                    i, wire_start, wire_end, desc.data_size);
            uint64_t str_len    = wire_end - wire_start;
            chars.resize(ch_pos + str_len);
            std::memcpy(chars.data() + ch_pos, data + wire_start, str_len);
            ch_pos += str_len;
            offsets[i] = ch_pos;
        }
        col = maybe_nullable(std::move(col_str));
    }
    else if (raw_type == COL_FIXED8)
    {
        if (base_type->getSizeOfValueInMemory() != 1)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED8 type width mismatch: declared type has {} bytes",
                base_type->getSizeOfValueInMemory());
        if (desc.data_offset + static_cast<uint64_t>(rows_to_dec) > buf.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED8 data out of bounds: offset={}, rows={}, buf={}",
                desc.data_offset, rows_to_dec, buf.size());
        // Use base_type so Int8, Bool, etc. round-trip correctly (not just UInt8).
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(inner->getRawData().data()),
                    buf.data() + desc.data_offset, rows_to_dec);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_FIXED16)
    {
        if (base_type->getSizeOfValueInMemory() != 2)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED16 type width mismatch: declared type has {} bytes",
                base_type->getSizeOfValueInMemory());
        if (desc.data_offset + static_cast<uint64_t>(rows_to_dec) * 2u > buf.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED16 data out of bounds: offset={}, rows={}, buf={}",
                desc.data_offset, rows_to_dec, buf.size());
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(inner->getRawData().data()),
                    buf.data() + desc.data_offset, rows_to_dec * 2);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_FIXED32)
    {
        if (base_type->getSizeOfValueInMemory() != 4)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED32 type width mismatch: declared type has {} bytes",
                base_type->getSizeOfValueInMemory());
        if (desc.data_offset + static_cast<uint64_t>(rows_to_dec) * 4u > buf.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED32 data out of bounds: offset={}, rows={}, buf={}",
                desc.data_offset, rows_to_dec, buf.size());
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(inner->getRawData().data()),
                    buf.data() + desc.data_offset, rows_to_dec * 4);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_FIXED64)
    {
        if (base_type->getSizeOfValueInMemory() != 8)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED64 type width mismatch: declared type has {} bytes",
                base_type->getSizeOfValueInMemory());
        if (desc.data_offset + static_cast<uint64_t>(rows_to_dec) * 8u > buf.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED64 data out of bounds: offset={}, rows={}, buf={}",
                desc.data_offset, rows_to_dec, buf.size());
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(inner->getRawData().data()),
                    buf.data() + desc.data_offset, rows_to_dec * 8);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_FIXEDN)
    {
        // Element width isn't tag-selected (unlike COL_FIXED8/16/32/64): recover it as
        // data_size/rows_to_dec, the only place it's recorded on the wire. data_offset/
        // data_size were already bounds-checked against buf.size() above.
        uint64_t elem_size = 0;
        if (rows_to_dec != 0)
        {
            if (desc.data_size % rows_to_dec != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_FIXEDN data_size {} not a multiple of row count {}",
                    desc.data_size, rows_to_dec);
            elem_size = desc.data_size / rows_to_dec;
            if (base_type->getSizeOfValueInMemory() != elem_size)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_FIXEDN type width mismatch: declared type has {} bytes, wire has {}",
                    base_type->getSizeOfValueInMemory(), elem_size);
        }
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        if (desc.data_size != 0)
            std::memcpy(const_cast<char *>(inner->getRawData().data()),
                        buf.data() + desc.data_offset, desc.data_size);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_COMPLEX)
    {
        // Map(K, V) is Array(Tuple(K, V)) on the wire; decode through the Array type
        // and re-wrap the result in ColumnMap at the end.
        const auto * map_type_ptr = typeid_cast<const DataTypeMap *>(base_type.get());
        const DataTypePtr & complex_type = map_type_ptr ? map_type_ptr->getNestedType() : base_type;
        if (const auto * arr_type = typeid_cast<const DataTypeArray *>(complex_type.get()))
        {
            // WASM→CH sequential layout: outer uint64 offsets[rows+1] at data_offset,
            // followed immediately by nested writeComplexData-format data.
            // rows_to_dec comes from the frame header's num_rows; widen before the +1 or
            // rows_to_dec == UINT32_MAX wraps to 0, trivially passing the bounds check below.
            const uint64_t outer_offset_bytes = (static_cast<uint64_t>(rows_to_dec) + 1u) * sizeof(uint64_t);
            if (outer_offset_bytes > desc.data_size)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX outer offsets exceed data_size: need={}, data_size={}",
                    outer_offset_bytes, desc.data_size);
            const uint8_t * p = buf.data() + desc.data_offset;
            const uint8_t * outer_offs = p;
            p += outer_offset_bytes;
            uint32_t total_elems = static_cast<uint32_t>(unalignedLoad<uint64_t>(outer_offs + rows_to_dec * 8u));
            // Same guest-controlled offsets as the nested decode() branch above: reject
            // anything not starting at 0 or non-monotonic before trusting total_elems for
            // the nested allocation/decode, or a crafted [0, 3, 1]-style frame can build a
            // ColumnArray whose per-row size underflows into a huge value downstream.
            if (unalignedLoad<uint64_t>(outer_offs) != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX Array offsets must start at 0");
            uint64_t prev_off = 0;
            for (uint32_t i = 0; i < rows_to_dec; ++i)
            {
                uint64_t off = unalignedLoad<uint64_t>(outer_offs + (i + 1u) * 8u);
                if (off < prev_off)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "COLUMNAR_V1: COL_COMPLEX Array offsets must be non-decreasing");
                prev_off = off;
            }
            auto nested_col = decode(p, arr_type->getNestedType(), total_elems);
            auto offsets_col = ColumnUInt64::create(rows_to_dec);
            for (uint32_t i = 0; i < rows_to_dec; ++i)
                offsets_col->getData()[i] = unalignedLoad<uint64_t>(outer_offs + (i + 1u) * 8u);
            col = maybe_nullable(ColumnArray::create(std::move(nested_col), std::move(offsets_col)));
        }
        else
        {
            // Tuple (and other complex types): data is packed at data_offset by writeComplexData.
            const uint8_t * data_ptr = buf.data() + desc.data_offset;
            col = maybe_nullable(decode(data_ptr, complex_type, rows_to_dec));
        }
        if (map_type_ptr)
            col = ColumnMap::create(std::move(col));
    }
    else if (raw_type == COL_VARIANT)
    {
        const auto * variant_type = typeid_cast<const DataTypeVariant *>(base_type.get());
        if (!variant_type)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT descriptor does not match declared type {}", base_type->getName());

        const auto & alt_types = variant_type->getVariants();
        if (alt_types.size() > ColumnVariant::MAX_NESTED_COLUMNS)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT declared type has too many alternatives: {}", alt_types.size());

        // Discriminators: uint8[rows_to_dec] at null_offset (NULL_DISCRIMINATOR=0xFF for NULL rows).
        if (desc.null_offset + static_cast<uint64_t>(rows_to_dec) > buf.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT discriminators out of bounds: offset={}, rows={}, buf={}",
                desc.null_offset, rows_to_dec, buf.size());
        const uint8_t * disc_src = buf.data() + desc.null_offset;

        // Row offsets: uint32[rows_to_dec] at offsets_offset (position within the row's sub-column).
        if (desc.offsets_offset + static_cast<uint64_t>(rows_to_dec) * 4u > buf.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT row offsets out of bounds: offset={}, rows={}, buf={}",
                desc.offsets_offset, rows_to_dec, buf.size());
        const uint8_t * offs_src = buf.data() + desc.offsets_offset;

        // Header: uint32 K + K x { uint8 global_discriminator, uint8[3] pad, ColDescriptor }.
        if (desc.data_size < 4u)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT header truncated: data_size={}", desc.data_size);
        const uint8_t * header = buf.data() + desc.data_offset;
        uint32_t k = unalignedLoad<uint32_t>(header);
        constexpr uint64_t record_bytes = 4u + COLUMNAR_DESC_BYTES;
        if (static_cast<uint64_t>(k) * record_bytes > desc.data_size - 4u)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT header declares {} sub-variants, exceeding data_size={}",
                k, desc.data_size);

        // Map global discriminator -> (inner descriptor, row count in that sub-column).
        std::vector<std::pair<ColDescriptor, uint32_t>> sub_by_global(alt_types.size(), {ColDescriptor{}, 0u});
        std::vector<bool> sub_present(alt_types.size(), false);
        const uint8_t * record_ptr = header + 4u;
        for (uint32_t r = 0; r < k; ++r)
        {
            uint8_t global_d = record_ptr[0];
            if (global_d >= alt_types.size())
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_VARIANT sub-variant global discriminator {} out of range [0, {})",
                    static_cast<uint32_t>(global_d), alt_types.size());
            if (sub_present[global_d])
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_VARIANT duplicate global discriminator {}", static_cast<uint32_t>(global_d));

            ColDescriptor inner_desc{};
            std::memcpy(&inner_desc, record_ptr + 4u, COLUMNAR_DESC_BYTES);
            // null_offset was repurposed by the writer to carry this sub-column's row count.
            uint32_t sub_rows = static_cast<uint32_t>(inner_desc.null_offset);
            sub_by_global[global_d] = {inner_desc, sub_rows};
            sub_present[global_d] = true;
            record_ptr += record_bytes;
        }

        // Decode each alternative's sub-column (empty if absent from the header).
        MutableColumns variant_cols;
        variant_cols.reserve(alt_types.size());
        for (size_t g = 0; g < alt_types.size(); ++g)
        {
            if (sub_present[g])
            {
                const auto & [inner_desc, sub_rows] = sub_by_global[g];
                variant_cols.push_back(readColumnFromDesc(buf, inner_desc, sub_rows, alt_types[g]));
            }
            else
            {
                variant_cols.push_back(alt_types[g]->createColumn());
            }
        }

        // Discriminators/offsets are local == global here since variant_cols is built in
        // the declared type's global order (see ColumnVariant.h for the local/global distinction).
        auto discr_col = ColumnVariant::ColumnDiscriminators::create(rows_to_dec);
        auto offs_col  = ColumnVariant::ColumnOffsets::create(rows_to_dec);
        std::vector<uint32_t> next_offset(alt_types.size(), 0);
        for (uint32_t i = 0; i < rows_to_dec; ++i)
        {
            uint8_t d = disc_src[i];
            uint32_t row_off = unalignedLoad<uint32_t>(offs_src + i * 4u);
            if (d != ColumnVariant::NULL_DISCRIMINATOR)
            {
                if (d >= alt_types.size() || row_off != next_offset[d] || row_off >= sub_by_global[d].second)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "COLUMNAR_V1: COL_VARIANT row {} has invalid discriminator/offset: discr={}, offset={}",
                        i, static_cast<uint32_t>(d), row_off);
                ++next_offset[d];
            }
            discr_col->getData()[i] = d;
            offs_col->getData()[i] = row_off;
        }

        col = ColumnVariant::create(std::move(discr_col), std::move(offs_col), std::move(variant_cols));
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "COLUMNAR_V1: unsupported output ColType {}", raw_type);
    }

    if (is_const && num_rows > 0)
        return ColumnConst::create(std::move(col), num_rows);
    return col;
}

// Decode a single-column COLUMNAR_V1 output buffer into a MutableColumnPtr.
// This is the entry point used by WASM UDF executors; it enforces num_cols == 1.
// result_type drives recursive decoding for COL_COMPLEX.
inline MutableColumnPtr readColumnarOutput(
    std::span<const uint8_t> buf,
    const DataTypePtr & result_type,
    size_t expected_rows)
{
    if (buf.size() < COLUMNAR_HEADER_BYTES + COLUMNAR_DESC_BYTES)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1 output buffer too small: {} bytes", buf.size());

    uint32_t num_rows = 0;
    uint32_t num_cols = 0;
    std::memcpy(&num_rows, buf.data(),     4);
    std::memcpy(&num_cols, buf.data() + 4, 4);

    if (num_rows != expected_rows)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1 output row count mismatch: expected {}, got {}", expected_rows, num_rows);
    if (num_cols != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1 output must have exactly 1 column, got {}", num_cols);

    ColDescriptor desc{};
    std::memcpy(&desc, buf.data() + COLUMNAR_HEADER_BYTES, sizeof(desc));
    return readColumnFromDesc(buf, desc, num_rows, result_type);
}

}
}
