#pragma once

/// Wire format helpers for the `ColumnBinary` wire encoding.
///
/// ── Purpose ─────────────────────────────────────────────────────────────────
/// It is a flat columnar encoding shared by the `ColumnBinary` I/O format
/// and the WASM UDF ABI. It is designed for low-overhead host↔guest transfer:
/// fixed-width columns serialize as a single memcpy; variable-width columns
/// (strings) pay the unavoidable uint64_t offset conversion.
/// All functions are inline — safe to include
/// from multiple TUs.
///
/// ── Wire layout ─────────────────────────────────────────────────────────────
///
///   [ 4 B magic | 2 B version | 2 B reserved
///     | 4 B num_rows | 4 B num_cols ]       ← FRAME_HEADER_BYTES = 16
///   [ ColDescriptor × num_cols    ]       ← COL_DESC_BYTES = 40 each
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
///   COL_LOWCARD (8) — LowCardinality(T), top-level only: a dictionary
///                     sub-column (embedded ColDescriptor, same recursive
///                     shape COL_VARIANT already uses for its sub-columns)
///                     plus a shared index array, one entry per row
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
/// ColumnConst (a single stored value + a logical row count). ColumnBinary
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
/// validateColumnBinaryWireSupportedType is the single source of truth for which
/// ClickHouse types this wire format can represent; call it eagerly (at format
/// construction / CREATE FUNCTION time) rather than discovering a rejection
/// only once the first block is serialized. Not supported: Nullable(Array/Variant)
/// (no wire slot for a top-level null map on COL_COMPLEX/COL_VARIANT; Nullable
/// (Tuple(...)) is supported instead, via a top-level null map on COL_COMPLEX),
/// and Variant nested inside Array/Tuple. Nullable(T) nested inside Array/Tuple
/// is supported (u8 null_map[n] prepended to T's own layout, see complexDataSize/
/// writeComplexData/the decode lambda). Map(K, V) is supported with no dedicated
/// wire encoding: it's Array(Tuple(K, V)) under the hood (DataTypeMap::
/// getNestedType() / ColumnMap::getNestedColumn()), so it's unwrapped to that at
/// every touch point and goes through the existing Array/Tuple path.
/// Top-level LowCardinality(T) has a direct dictionary + index encoding
/// (COL_LOWCARD). Nested LowCardinality (inside Array/Tuple) still fully
/// materializes to T's full column instead (ColumnLowCardinality::
/// convertToFullColumn() / insertRangeFromFullColumn) — TODO, see the comment on
/// validateColumnBinaryWireSupportedType's LowCardinality branch. Any fixed-width type
/// is supported at any width (COL_FIXED8/16/32/64
/// for 1/2/4/8 bytes, COL_FIXEDN for everything else — UUID, IPv6, Int128/
/// UInt128, Decimal128/256), and so is FixedString(N) of any length (also
/// COL_FIXEDN). Any type kind this format has no encoding for at all (Dynamic,
/// JSON/Object, AggregateFunction(...), ...) is also rejected.

#include <cstring>
#include <span>
#include <functional>
#include <utility>
#include <vector>

#include <base/unaligned.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
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
#include <limits>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

namespace ColumnBinaryWire
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
constexpr uint32_t COL_LOWCARD      = 8;  // LowCardinality(T), top-level only: dictionary
                                           // sub-column (embedded ColDescriptor) + index array
// Modifier flags (OR'd onto base type; base types 0–6, so bits 5-7 are free for flags).
constexpr uint32_t COL_IS_NULLABLE  = 0x20u; // Nullable(T); null_offset carries u8[row_count] null map
constexpr uint32_t COL_IS_CONST     = 0x80u;

/// The user-facing `ColumnBinary` format is gated behind
/// `allow_experimental_column_binary_format`: the frame header carries no wire version,
/// so an incompatible layout change would misparse previously written data rather than
/// reject it. The gate keeps `ColumnBinary` out of persisted data until the layout is
/// frozen and the header is versioned. The `ColumnBinary` WASM UDF ABI shares this wire
/// format but is not gated by this setting — WASM UDFs are experimental in their own
/// right, and their frames never outlive a single call.
inline void checkColumnBinaryFormatIsAllowed(bool allow_experimental)
{
    if (!allow_experimental)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "The 'ColumnBinary' format is experimental: its wire layout is still evolving, and "
            "while the frame header carries a format version, no compatibility with earlier or "
            "later versions is promised yet, so data written today may not be readable by a "
            "future version. Set allow_experimental_column_binary_format = 1 to use it.");
}

/// Frame magic, the ASCII bytes 'C', 'B', 'I', 'N' in wire order. A frame is a bare byte
/// range with no container around it - a WASM guest buffer, a file, a socket - so it carries
/// its own identity, and a reader that is handed the wrong bytes says so instead of decoding
/// an arbitrary row and column count out of them.
constexpr uint32_t FRAME_MAGIC = 0x4E494243; /// 'C' | 'B' << 8 | 'I' << 16 | 'N' << 24

/// Version of the frame layout. Bump it whenever the meaning of any existing byte changes;
/// a reader rejects any version it does not implement rather than guessing.
constexpr uint16_t FRAME_VERSION = 1;

/// [ 4 B magic | 2 B version | 2 B reserved | 4 B num_rows | 4 B num_cols ]
///
/// 16 bytes rather than the 10 the fields strictly need, so the descriptor table that follows
/// starts 8-byte aligned and a guest can read `ColDescriptor` in place. `reserved` is written
/// as 0 and required to be 0 on read, which keeps it available for frame-wide flags: an old
/// reader then refuses a frame that uses one instead of silently ignoring it.
constexpr uint32_t FRAME_HEADER_BYTES = 16;
constexpr uint32_t COL_DESC_BYTES   = 40;

inline void writeFrameHeader(uint8_t * dst, uint32_t num_rows, uint32_t num_cols)
{
    constexpr uint16_t reserved = 0;
    std::memcpy(dst,      &FRAME_MAGIC,    4);
    std::memcpy(dst + 4,  &FRAME_VERSION,  2);
    std::memcpy(dst + 6,  &reserved,       2);
    std::memcpy(dst + 8,  &num_rows,       4);
    std::memcpy(dst + 12, &num_cols,       4);
}

/// Read and validate a frame header from `src`, which must hold at least FRAME_HEADER_BYTES.
inline void readFrameHeader(const uint8_t * src, uint32_t & num_rows, uint32_t & num_cols)
{
    uint32_t magic = 0;
    uint16_t version = 0;
    uint16_t reserved = 0;
    std::memcpy(&magic,    src,      4);
    std::memcpy(&version,  src + 4,  2);
    std::memcpy(&reserved, src + 6,  2);

    if (magic != FRAME_MAGIC)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: bad frame magic {:#010x}, expected {:#010x}", magic, FRAME_MAGIC);
    if (version != FRAME_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: unsupported frame version {}, this server writes and reads version {}",
            version, FRAME_VERSION);
    if (reserved != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: reserved frame header field is {}, expected 0; the frame uses a "
            "feature this version does not implement", reserved);

    std::memcpy(&num_rows, src + 8,  4);
    std::memcpy(&num_cols, src + 12, 4);
}

struct ColDescriptor
{
    uint64_t type;
    uint64_t null_offset;
    uint64_t offsets_offset;
    uint64_t data_offset;
    uint64_t data_size;
};
static_assert(sizeof(ColDescriptor) == COL_DESC_BYTES);

// The wire descriptor's own offsets/sizes are uint64_t, but element/row counts are threaded
// through the recursive size/write/read helpers below as uint32_t (row counts realistically
// don't exceed 4 billion). A flattened Array(T)'s total nested element count is a different
// story: a single top-level row can legitimately carry more than 2^32-1 elements. Narrowing
// that through a uint32_t intermediate would silently wrap, causing writeColData to underfill
// or overrun a buffer sized off the wrapped (small) count instead of the real (large) one.
// Call this at every point that narrows a real element/row count into the uint32_t path, so
// the failure mode is a clear exception instead of silent frame corruption.
inline uint32_t checkFitsUint32(uint64_t value, const char * what)
{
    if (value > std::numeric_limits<uint32_t>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: {} ({}) exceeds the maximum representable element count ({})",
            what, value, std::numeric_limits<uint32_t>::max());
    return static_cast<uint32_t>(value);
}

// Recursively check that `type` can round-trip through the ColumnBinary wire format,
// throwing INCORRECT_DATA immediately with the exact reason otherwise. Without this,
// callers only discover an unsupported signature (nested Nullable/Variant inside
// Array/Tuple, Map, LowCardinality, or a fixed-width type whose size isn't exactly
// 1/2/4/8 bytes such as UUID/IPv6/Int128/UInt128/Decimal128/256/FixedString) when the
// first block is actually serialized.
// is_nested is false only for the outermost call; Nullable/Variant are only
// disallowed once already inside an Array/Tuple (COL_COMPLEX), where there is no
// wire slot for a nested null map or discriminator.
inline void validateColumnBinaryWireSupportedType(const DataTypePtr & type, bool is_nested = false)
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
                "ColumnBinary/ColumnBinary: Nullable(Array/Variant) is not supported: {}", type->getName());
        validateColumnBinaryWireSupportedType(nested_type, is_nested);
        return;
    }
    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        if (is_nested)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary/ColumnBinary: nested Variant inside Array/Tuple is not supported: {}", type->getName());
        for (const auto & alt : variant_type->getVariants())
            validateColumnBinaryWireSupportedType(alt, is_nested);
        return;
    }
    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        // Map(K, V) is Array(Tuple(K, V)) under the hood (DataTypeMap::getNestedType());
        // no dedicated wire encoding needed, just validate through the existing Array/
        // Tuple path.
        validateColumnBinaryWireSupportedType(map_type->getNestedType(), is_nested);
        return;
    }
    if (const auto * lowcard_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        // Top-level LowCardinality(T) has a direct dictionary + index encoding
        // (COL_LOWCARD; see buildColDescriptor/writeColData/readColumnFromDesc). TODO:
        // nested LowCardinality (inside Array/Tuple) still fully materializes to T's full
        // column instead (ColumnLowCardinality::convertToFullColumn(), see
        // complexDataSize/writeComplexData/the decode lambda) — a nested dictionary
        // encoding raises its own design question (would it share one dictionary across
        // the whole flattened Array, or get one per element-run?) not answered here.
        validateColumnBinaryWireSupportedType(lowcard_type->getDictionaryType(), is_nested);
        return;
    }
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        validateColumnBinaryWireSupportedType(array_type->getNestedType(), /* is_nested */ true);
        return;
    }
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        for (const auto & elem : tuple_type->getElements())
            validateColumnBinaryWireSupportedType(elem, /* is_nested */ true);
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
        "ColumnBinary/ColumnBinary: type is not supported: {}", type->getName());
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
    if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(&col))
        return complexDataSize(*lc_col->convertToFullColumn(), n);
    if (const auto * arr = typeid_cast<const ColumnArray *>(&col))
    {
        uint32_t total = checkFitsUint32(arr->getData().size(), "flattened Array element count");
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
            "ColumnBinary: nested Variant inside Array/Tuple is not supported in COL_COMPLEX; "
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
    if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(&col))
    {
        writeComplexData(*lc_col->convertToFullColumn(), n, dst);
        return;
    }
    if (const auto * arr = typeid_cast<const ColumnArray *>(&col))
    {
        const auto & ch_offs = arr->getOffsets();
        unalignedStore<uint64_t>(dst, 0ull);
        for (uint32_t i = 0; i < n; ++i)
            unalignedStore<uint64_t>(dst + (static_cast<uint64_t>(i) + 1u) * 8u, static_cast<uint64_t>(ch_offs[i]));
        uint32_t total = checkFitsUint32(arr->getData().size(), "flattened Array element count");
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
            "ColumnBinary: nested Variant inside Array/Tuple is not supported in COL_COMPLEX; "
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
    // (validateColumnBinaryWireSupportedType rejects Nullable(Variant(...))).
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
        write_cursor += 4u + k * (4u + COL_DESC_BYTES);

        // Now allocate space for each non-empty sub-column.
        for (uint32_t local = 0; local < num_variants; ++local)
        {
            const IColumn & sub = var_col->getVariantByLocalDiscriminator(local);
            if (sub.empty())
                continue;
            ColDescriptor inner_desc{};
            uint32_t sub_rows = checkFitsUint32(sub.size(), "Variant sub-column row count");
            write_cursor = buildColDescriptor(&sub, false, false, sub_rows, write_cursor, inner_desc);
        }

        desc.data_size = write_cursor - desc.data_offset;
        return write_cursor;
    }

    // ── LowCardinality column → COL_LOWCARD (top-level only; nested inside
    // Array/Tuple still materializes, see complexDataSize) ──────────────────
    // Wire layout:
    //   offsets_offset → index[num_rows]   index_elem_width bytes each
    //   data_offset    → header:
    //     uint32 dict_row_count
    //     uint8  index_elem_width
    //     uint8[3] pad
    //     ColDescriptor dict_desc          (40 bytes, abs buffer offsets)
    //     (dictionary sub-column data at positions given by dict_desc)
    // null_offset is unused, and so is the dictionary sub-column's: for
    // LowCardinality(Nullable(T)) the recursive buildColDescriptor call below
    // passes is_nullable=false and unwraps ColumnUnique::getNestedColumn's
    // Nullable wrapper, so no dictionary null map is written. Nullability is
    // conveyed by ColumnUnique's reserved slot layout instead — slot 0 is the
    // NULL sentinel — which the reader reconstructs from the declared type.
    if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(col))
    {
        desc.type        = COL_LOWCARD | (is_const ? COL_IS_CONST : 0u);
        desc.null_offset = 0;

        const IColumn & dict_col = *lc_col->getDictionary().getNestedColumn();
        const IColumn & idx_col  = lc_col->getIndexes();
        uint32_t dict_rows    = checkFitsUint32(dict_col.size(), "LowCardinality dictionary row count");
        uint32_t idx_elem_sz  = static_cast<uint32_t>(idx_col.sizeOfValueIfFixed());

        desc.offsets_offset = write_cursor;
        write_cursor += static_cast<uint64_t>(num_rows) * idx_elem_sz;

        write_cursor = (write_cursor + 3ull) & ~3ull;
        desc.data_offset = write_cursor;
        write_cursor += 4u + 4u + COL_DESC_BYTES; // dict_row_count + (index_elem_width+pad) + dict_desc

        ColDescriptor dict_desc{};
        write_cursor = buildColDescriptor(&dict_col, false, false, dict_rows, write_cursor, dict_desc);

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
        uint32_t total_elems = checkFitsUint32(nested.size(), "flattened Array element count");

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
        uint64_t sub_cursor = desc.data_offset + 4u + k * (4u + COL_DESC_BYTES);

        for (uint32_t local = 0; local < num_variants; ++local)
        {
            const IColumn & sub = var_col->getVariantByLocalDiscriminator(local);
            if (sub.empty())
                continue;

            uint8_t global_d = var_col->globalDiscriminatorByLocal(static_cast<ColumnVariant::Discriminator>(local));
            uint32_t sub_rows = checkFitsUint32(sub.size(), "Variant sub-column row count");

            ColDescriptor inner_desc{};
            sub_cursor = buildColDescriptor(&sub, false, false, sub_rows, sub_cursor, inner_desc);
            inner_desc.null_offset = sub_rows;  // repurpose null_offset to carry sub_rows for the decoder

            std::memcpy(record_ptr,     &global_d,   1u);
            std::memset(record_ptr + 1, 0,           3u);
            std::memcpy(record_ptr + 4, &inner_desc, COL_DESC_BYTES);
            record_ptr += 4u + COL_DESC_BYTES;

            writeColData(&sub, false, sub_rows, inner_desc, buf);
        }
        return;
    }

    // ── LowCardinality column → COL_LOWCARD ──────────────────────────────────
    if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(col))
    {
        const IColumn & dict_col = *lc_col->getDictionary().getNestedColumn();
        const IColumn & idx_col  = lc_col->getIndexes();
        uint32_t dict_rows   = checkFitsUint32(dict_col.size(), "LowCardinality dictionary row count");
        uint32_t idx_elem_sz = static_cast<uint32_t>(idx_col.sizeOfValueIfFixed());

        // Recompute the dictionary sub-column's descriptor (buildColDescriptor is
        // deterministic given the same inputs) rather than threading it through some
        // other channel — same pattern the Variant branch above uses for inner_desc.
        ColDescriptor dict_desc{};
        buildColDescriptor(&dict_col, false, false, dict_rows, desc.data_offset + 4u + 4u + COL_DESC_BYTES, dict_desc);

        uint8_t * header = buf.data() + desc.data_offset;
        std::memcpy(header, &dict_rows, 4u);
        uint8_t idx_elem_sz_byte = static_cast<uint8_t>(idx_elem_sz);
        std::memcpy(header + 4u, &idx_elem_sz_byte, 1u);
        std::memset(header + 5u, 0, 3u);
        std::memcpy(header + 8u, &dict_desc, COL_DESC_BYTES);

        writeColData(&dict_col, false, dict_rows, dict_desc, buf);

        std::memcpy(buf.data() + desc.offsets_offset, idx_col.getRawData().data(),
                    static_cast<uint64_t>(num_rows) * idx_elem_sz);
        return;
    }

    // ── Array column → COL_COMPLEX ────────────────────────────────────────────
    if (const auto * arr_col = typeid_cast<const ColumnArray *>(col))
    {
        const auto & ch_offsets = arr_col->getOffsets();
        const IColumn & nested  = arr_col->getData();
        uint32_t total_elems = checkFitsUint32(nested.size(), "flattened Array element count");

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

// Decode one column from a ColumnBinary frame given its pre-parsed descriptor.
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

    // desc.type is otherwise-untrusted (guest/network-controlled): a malformed frame could set
    // COL_IS_NULLABLE against a declared type that isn't actually Nullable(T). Check explicitly
    // and throw a normal INCORRECT_DATA exception instead of letting a reference dynamic_cast
    // throw std::bad_cast, which isn't a DB::Exception and wouldn't be handled the same way by
    // callers expecting a clean parse-error contract from this function.
    const auto * nullable_result_type = typeid_cast<const DataTypeNullable *>(result_type.get());
    if (is_nullable_wire && !nullable_result_type)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: descriptor sets COL_IS_NULLABLE but declared type {} is not Nullable",
            result_type->getName());
    const DataTypePtr & base_type = is_nullable_wire
        ? nullable_result_type->getNestedType()
        : result_type;

    // desc comes straight from guest/network memory and is otherwise untrusted:
    // validate data_offset/data_size against buf.size() before forming any
    // pointer from them (data_end below, and every buf.data() + desc.data_offset
    // in the branches further down), or a malformed descriptor can build an
    // out-of-bounds pointer that later bounds checks then compare against.
    if (desc.data_offset > buf.size() || desc.data_size > buf.size() - desc.data_offset)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: column data range out of bounds: offset={}, size={}, buf={}",
            desc.data_offset, desc.data_size, buf.size());

    const uint8_t * const data_end = buf.data() + desc.data_offset + desc.data_size;

    // Recursive decoder for COL_COMPLEX (Array / Tuple / nested scalars).
    std::function<MutableColumnPtr(const uint8_t *&, const DataTypePtr &, uint32_t)> decode;
    decode = [&](const uint8_t *& p, const DataTypePtr & type, uint32_t n) -> MutableColumnPtr
    {
        if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
            return ColumnMap::create(decode(p, map_type->getNestedType(), n));
        if (const auto * lowcard_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            auto full_col = decode(p, lowcard_type->getDictionaryType(), n);
            auto lc_col = lowcard_type->createColumn();
            typeid_cast<ColumnLowCardinality &>(*lc_col).insertRangeFromFullColumn(*full_col, 0, full_col->size());
            return lc_col;
        }
        if (const auto * arr_type = typeid_cast<const DataTypeArray *>(type.get()))
        {
            // n comes from a guest-controlled row/element count; widen before the +1 or
            // n == UINT32_MAX wraps the uint32_t addition to 0, making outer_bytes 0 and
            // trivially passing the bounds check below.
            uint64_t outer_bytes = (static_cast<uint64_t>(n) + 1u) * sizeof(uint64_t);
            // Compare against the remaining space (data_end - p, safe since p <= data_end is
            // a loop invariant) rather than forming p + outer_bytes: outer_bytes is guest-
            // controlled and can be large enough that the raw pointer addition overflows the
            // address space, producing UB before the comparison even runs.
            if (outer_bytes > static_cast<uint64_t>(data_end - p))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_COMPLEX nested Array outer offsets out of bounds");
            const uint8_t * outer_offs = p;
            p += outer_bytes;
            // Widen before the multiply: n is guest-controlled, and n * 8u computed in
            // uint32_t arithmetic wraps for any n >= 0x20000000, making this read the wrong
            // (or an in-bounds but unintended) slot instead of the true last cumulative count.
            uint32_t total_elems = checkFitsUint32(
                unalignedLoad<uint64_t>(outer_offs + static_cast<uint64_t>(n) * 8u),
                "flattened Array element count read from frame");
            // outer_offs holds n+1 cumulative counts (offs[0]==0, offs[n]==total_elems); the
            // module fully controls these, so reject anything non-monotonic or not starting
            // at 0 before using them as ColumnArray offsets, or a crafted [0, 3, 1]-style frame
            // can make later offset differences underflow into a huge size downstream.
            if (unalignedLoad<uint64_t>(outer_offs) != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_COMPLEX Array offsets must start at 0");
            auto nested_col = decode(p, arr_type->getNestedType(), total_elems);
            auto offsets_col = ColumnUInt64::create(n);
            uint64_t prev_off = 0;
            for (uint32_t i = 0; i < n; ++i)
            {
                // Widen before the multiply: same n * 8u wraparound hazard as above.
                uint64_t off = unalignedLoad<uint64_t>(outer_offs + (static_cast<uint64_t>(i) + 1u) * 8u);
                if (off < prev_off)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "ColumnBinary: COL_COMPLEX Array offsets must be non-decreasing");
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
            // See the Array branch above: compare against remaining space, not p + off_bytes,
            // to avoid overflowing pointer arithmetic on a guest-controlled length.
            if (off_bytes > static_cast<uint64_t>(data_end - p))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_COMPLEX String offsets out of bounds");
            const uint8_t * wire_offs = p;
            p += off_bytes;
            // Same n * 8u wraparound hazard as the Array branch above: widen before the multiply.
            uint64_t total_chars = unalignedLoad<uint64_t>(wire_offs + static_cast<uint64_t>(n) * 8u);
            if (total_chars > static_cast<uint64_t>(data_end - p))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_COMPLEX String chars out of bounds");
            const uint8_t * chars_src = p;
            p += total_chars;
            // wire_offs holds n+1 cumulative byte offsets (offs[0]==0, offs[n]==total_chars);
            // the module fully controls these, so reject anything non-monotonic before using
            // them to slice chars_src, or a crafted offset pair can underflow str_len into a
            // huge size and drive an out-of-bounds memcpy.
            if (unalignedLoad<uint64_t>(wire_offs) != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_COMPLEX String offsets must start at 0");
            auto col_str = ColumnString::create();
            auto & chars   = col_str->getChars();
            auto & offsets = col_str->getOffsets();
            offsets.resize(n);
            uint64_t ch_pos = 0ull;
            uint64_t prev_wire_end = 0;
            for (uint32_t i = 0; i < n; ++i)
            {
                // Widen before the multiply: same n * 8u wraparound hazard as above.
                uint64_t wire_end   = unalignedLoad<uint64_t>(wire_offs + (static_cast<uint64_t>(i) + 1u) * 8u);
                uint64_t wire_start = unalignedLoad<uint64_t>(wire_offs + static_cast<uint64_t>(i) * 8u);
                if (wire_start != prev_wire_end || wire_end < wire_start)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "ColumnBinary: COL_COMPLEX String offsets must be non-decreasing and contiguous");
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
            if (static_cast<uint64_t>(n) > static_cast<uint64_t>(data_end - p))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_COMPLEX nested Nullable null map out of bounds");
            auto null_map_col = ColumnUInt8::create(n);
            std::memcpy(null_map_col->getData().data(), p, n);
            p += n;
            auto inner = decode(p, nullable_type->getNestedType(), n);
            return ColumnNullable::create(std::move(inner), std::move(null_map_col));
        }
        if (typeid_cast<const DataTypeVariant *>(type.get()))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: nested Variant inside Array/Tuple is not supported in COL_COMPLEX; "
                "use a flat variant column or a different format");
        uint32_t elem_bytes = static_cast<uint32_t>(type->getSizeOfValueInMemory());
        if (static_cast<uint64_t>(n) * elem_bytes > static_cast<uint64_t>(data_end - p))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_COMPLEX fixed data out of bounds");
        auto col = type->createColumn();
        col->insertManyDefaults(n);
        std::memcpy(const_cast<char *>(col->getRawData().data()), p, n * elem_bytes);
        p += n * elem_bytes;
        return col;
    };

    auto maybe_nullable = [&](MutableColumnPtr inner) -> MutableColumnPtr
    {
        if (!is_nullable_wire)
            return inner;
        // null_offset == 0 must be rejected, not treated as "no null map": 0 is a valid sentinel
        // for offset fields that are genuinely optional (e.g. offsets_offset on a fixed-width
        // column), but every real frame has header + descriptor table before any data blob, so
        // byte 0 can never be a legitimate null-map location. Falling through to return the
        // plain (non-nullable) inner column here would let a malformed frame's non-nullable
        // source reach ColumnNullable::insertRangeFrom downstream (e.g. via
        // StreamingFormatExecutor::insertChunk on the INSERT path), whose release-build
        // assert_cast is a raw static_cast — UB instead of a clean parse error.
        if (desc.null_offset == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_IS_NULLABLE is set but null_offset is 0 (missing null map)");
        // null_offset is an untrusted uint64_t straight from the wire: check the offset
        // against buf.size() first, then the length against the *remaining* space, instead
        // of offset + length > buf.size(), which a large offset can wrap past overflow.
        if (desc.null_offset > buf.size() || static_cast<uint64_t>(rows_to_dec) > buf.size() - desc.null_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: null map out of bounds: offset={}, rows={}, buf={}",
                desc.null_offset, rows_to_dec, buf.size());
        // Null map: 1=null, 0=non-null — identical to ColumnNullable layout; direct copy.
        auto null_col = ColumnUInt8::create(rows_to_dec);
        std::memcpy(null_col->getData().data(), buf.data() + desc.null_offset, rows_to_dec);
        return ColumnNullable::create(std::move(inner), std::move(null_col));
    };

    MutableColumnPtr col;

    if (raw_type == COL_BYTES)
    {
        // Unlike the COL_FIXED* branches, which build the declared column via
        // base_type->createColumn(), this one always builds a ColumnString. The tag is
        // otherwise-untrusted, so without this check a frame can declare COL_BYTES for a
        // column the schema says is, say, UInt64 and hand a ColumnString back to the caller;
        // the resulting insertRangeFrom into the destination column goes through assert_cast,
        // which is a plain static_cast in release builds — type confusion instead of a clean
        // rejection. The writer only ever emits COL_BYTES for a ColumnString, so mirror that.
        if (!typeid_cast<const ColumnString *>(base_type->createColumn().get()))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_BYTES descriptor does not match declared type {}", base_type->getName());

        // Widen before the +1: rows_to_dec is otherwise-untrusted (guest/network-controlled),
        // and rows_to_dec == UINT32_MAX would wrap (rows_to_dec + 1) to 0 in uint32_t
        // arithmetic before the cast, making the bounds check below trivially pass and letting
        // offsets.resize/the read loop run against out-of-frame data.
        // offsets_offset is an untrusted uint64_t straight from the wire: check the offset
        // against buf.size() first, then the length against the *remaining* space, instead
        // of offset + length > buf.size(), which a large offset can wrap past overflow.
        const uint64_t offsets_bytes = (static_cast<uint64_t>(rows_to_dec) + 1u) * 8u;
        if (desc.offsets_offset > buf.size() || offsets_bytes > buf.size() - desc.offsets_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_BYTES offsets array out of bounds: offset={}, rows={}, buf={}",
                desc.offsets_offset, rows_to_dec, buf.size());
        if (desc.data_offset > buf.size() || desc.data_size > buf.size() - desc.data_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_BYTES data out of bounds: offset={}, size={}, buf={}",
                desc.data_offset, desc.data_size, buf.size());

        const uint8_t  * wire_offsets = buf.data() + desc.offsets_offset;
        const uint8_t  * data         = buf.data() + desc.data_offset;
        auto col_str = ColumnString::create();
        auto & chars   = col_str->getChars();
        auto & offsets = col_str->getOffsets();
        offsets.resize(rows_to_dec);
        uint64_t ch_pos = 0ull;
        // The writer emits offsets that start at 0 and cover the chars blob exactly and
        // contiguously (buildColDescriptor sets data_size = total_chars precisely, no padding
        // or gaps), so validate that shape here rather than only bounding each [start, end)
        // pair independently — otherwise a malformed frame like offsets [1, 3] over payload
        // "abc" decodes as "bc" (a shifted string) instead of being rejected, and [0, 1] would
        // silently leave trailing bytes of the declared block unused.
        uint64_t expected_start = 0ull;
        for (uint32_t i = 0; i < rows_to_dec; ++i)
        {
            // Widen before the multiply: i * 8u computed in uint32_t arithmetic wraps for
            // i >= 0x20000000, which a large enough frame (raised column_binary_max_frame_size,
            // or an oversized WASM guest output) can reach.
            uint64_t wire_end   = unalignedLoad<uint64_t>(wire_offsets + (static_cast<uint64_t>(i) + 1u) * 8u);
            uint64_t wire_start = unalignedLoad<uint64_t>(wire_offsets + static_cast<uint64_t>(i) * 8u);
            if (wire_start != expected_start)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_BYTES offsets must be contiguous starting at 0: row {} expected "
                    "start {}, got {}",
                    i, expected_start, wire_start);
            if (wire_end < wire_start || wire_end > desc.data_size)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_BYTES invalid string offsets at row {}: [{}, {}), data_size={}",
                    i, wire_start, wire_end, desc.data_size);
            uint64_t str_len    = wire_end - wire_start;
            chars.resize(ch_pos + str_len);
            std::memcpy(chars.data() + ch_pos, data + wire_start, str_len);
            ch_pos += str_len;
            offsets[i] = ch_pos;
            expected_start = wire_end;
        }
        if (expected_start != desc.data_size)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_BYTES offsets do not cover the full data block: consumed {}, data_size={}",
                expected_start, desc.data_size);
        col = maybe_nullable(std::move(col_str));
    }
    else if (raw_type == COL_FIXED8)
    {
        if (base_type->getSizeOfValueInMemory() != 1)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_FIXED8 type width mismatch: declared type has {} bytes",
                base_type->getSizeOfValueInMemory());
        // desc.data_size is otherwise-untrusted and must match the declared row count exactly:
        // without this check, only the (buf.size()-relative) bounds check below applies, which
        // lets a frame with a too-small declared data_size still consume bytes belonging to the
        // next column's payload as long as enough bytes remain in the whole buffer.
        if (desc.data_size != static_cast<uint64_t>(rows_to_dec))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_FIXED8 data_size {} does not match row count {}",
                desc.data_size, rows_to_dec);
        if (desc.data_offset > buf.size() || static_cast<uint64_t>(rows_to_dec) > buf.size() - desc.data_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_FIXED8 data out of bounds: offset={}, rows={}, buf={}",
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
                "ColumnBinary: COL_FIXED16 type width mismatch: declared type has {} bytes",
                base_type->getSizeOfValueInMemory());
        // See the matching check in COL_FIXED8 above.
        if (desc.data_size != static_cast<uint64_t>(rows_to_dec) * 2u)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_FIXED16 data_size {} does not match row count {}",
                desc.data_size, rows_to_dec);
        if (desc.data_offset > buf.size() || static_cast<uint64_t>(rows_to_dec) * 2u > buf.size() - desc.data_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_FIXED16 data out of bounds: offset={}, rows={}, buf={}",
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
                "ColumnBinary: COL_FIXED32 type width mismatch: declared type has {} bytes",
                base_type->getSizeOfValueInMemory());
        // See the matching check in COL_FIXED8 above.
        if (desc.data_size != static_cast<uint64_t>(rows_to_dec) * 4u)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_FIXED32 data_size {} does not match row count {}",
                desc.data_size, rows_to_dec);
        if (desc.data_offset > buf.size() || static_cast<uint64_t>(rows_to_dec) * 4u > buf.size() - desc.data_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_FIXED32 data out of bounds: offset={}, rows={}, buf={}",
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
                "ColumnBinary: COL_FIXED64 type width mismatch: declared type has {} bytes",
                base_type->getSizeOfValueInMemory());
        // See the matching check in COL_FIXED8 above.
        if (desc.data_size != static_cast<uint64_t>(rows_to_dec) * 8u)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_FIXED64 data_size {} does not match row count {}",
                desc.data_size, rows_to_dec);
        if (desc.data_offset > buf.size() || static_cast<uint64_t>(rows_to_dec) * 8u > buf.size() - desc.data_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_FIXED64 data out of bounds: offset={}, rows={}, buf={}",
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
                    "ColumnBinary: COL_FIXEDN data_size {} not a multiple of row count {}",
                    desc.data_size, rows_to_dec);
            elem_size = desc.data_size / rows_to_dec;
            if (base_type->getSizeOfValueInMemory() != elem_size)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_FIXEDN type width mismatch: declared type has {} bytes, wire has {}",
                    base_type->getSizeOfValueInMemory(), elem_size);
        }
        // rows_to_dec == 0: no width to divide by, but a malformed frame could still
        // declare a non-zero data_size here; without rejecting that, the memcpy below
        // would write desc.data_size bytes into a zero-sized allocation.
        else if (desc.data_size != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_FIXEDN data_size {} must be 0 for an empty column",
                desc.data_size);
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
                    "ColumnBinary: COL_COMPLEX outer offsets exceed data_size: need={}, data_size={}",
                    outer_offset_bytes, desc.data_size);
            const uint8_t * p = buf.data() + desc.data_offset;
            const uint8_t * outer_offs = p;
            p += outer_offset_bytes;
            // Widen before the multiply: rows_to_dec * 8u computed in uint32_t arithmetic
            // wraps for rows_to_dec >= 0x20000000.
            uint32_t total_elems = checkFitsUint32(
                unalignedLoad<uint64_t>(outer_offs + static_cast<uint64_t>(rows_to_dec) * 8u),
                "flattened Array element count read from frame");
            // Same guest-controlled offsets as the nested decode() branch above: reject
            // anything not starting at 0 or non-monotonic before trusting total_elems for
            // the nested allocation/decode, or a crafted [0, 3, 1]-style frame can build a
            // ColumnArray whose per-row size underflows into a huge value downstream.
            if (unalignedLoad<uint64_t>(outer_offs) != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_COMPLEX Array offsets must start at 0");
            uint64_t prev_off = 0;
            for (uint32_t i = 0; i < rows_to_dec; ++i)
            {
                // Widen before the multiply: same wraparound hazard as above.
                uint64_t off = unalignedLoad<uint64_t>(outer_offs + (static_cast<uint64_t>(i) + 1u) * 8u);
                if (off < prev_off)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "ColumnBinary: COL_COMPLEX Array offsets must be non-decreasing");
                prev_off = off;
            }
            auto nested_col = decode(p, arr_type->getNestedType(), total_elems);
            auto offsets_col = ColumnUInt64::create(rows_to_dec);
            for (uint32_t i = 0; i < rows_to_dec; ++i)
                offsets_col->getData()[i] = unalignedLoad<uint64_t>(outer_offs + (static_cast<uint64_t>(i) + 1u) * 8u);
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
                "ColumnBinary: COL_VARIANT descriptor does not match declared type {}", base_type->getName());

        const auto & alt_types = variant_type->getVariants();
        if (alt_types.size() > ColumnVariant::MAX_NESTED_COLUMNS)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_VARIANT declared type has too many alternatives: {}", alt_types.size());

        // Neither array is optional for COL_VARIANT, so 0 is not an "absent" sentinel here the
        // way it is for a plain column's null map: the writer's cursor starts past the header
        // and descriptor table, so it is never 0 in a genuine frame. Without these checks a
        // frame omitting the discriminators makes disc_src point at the frame header, so a
        // 1-row column takes its discriminator from the low byte of num_rows and is accepted
        // with no discriminator bytes on the wire at all; omitting the row offsets is the same
        // hole for an all-null frame, where row_off is never examined.
        if (desc.null_offset == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_VARIANT descriptor has no discriminators (null_offset is 0)");
        if (desc.offsets_offset == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_VARIANT descriptor has no row offsets (offsets_offset is 0)");

        // Discriminators: uint8[rows_to_dec] at null_offset (NULL_DISCRIMINATOR=0xFF for NULL rows).
        if (desc.null_offset > buf.size() || static_cast<uint64_t>(rows_to_dec) > buf.size() - desc.null_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_VARIANT discriminators out of bounds: offset={}, rows={}, buf={}",
                desc.null_offset, rows_to_dec, buf.size());
        const uint8_t * disc_src = buf.data() + desc.null_offset;

        // Row offsets: uint32[rows_to_dec] at offsets_offset (position within the row's sub-column).
        if (desc.offsets_offset > buf.size() || static_cast<uint64_t>(rows_to_dec) * 4u > buf.size() - desc.offsets_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_VARIANT row offsets out of bounds: offset={}, rows={}, buf={}",
                desc.offsets_offset, rows_to_dec, buf.size());
        const uint8_t * offs_src = buf.data() + desc.offsets_offset;

        // Header: uint32 K + K x { uint8 global_discriminator, uint8[3] pad, ColDescriptor }.
        if (desc.data_size < 4u)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_VARIANT header truncated: data_size={}", desc.data_size);
        const uint8_t * header = buf.data() + desc.data_offset;
        uint32_t k = unalignedLoad<uint32_t>(header);
        constexpr uint64_t record_bytes = 4u + COL_DESC_BYTES;
        if (static_cast<uint64_t>(k) * record_bytes > desc.data_size - 4u)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_VARIANT header declares {} sub-variants, exceeding data_size={}",
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
                    "ColumnBinary: COL_VARIANT sub-variant global discriminator {} out of range [0, {})",
                    static_cast<uint32_t>(global_d), alt_types.size());
            if (sub_present[global_d])
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_VARIANT duplicate global discriminator {}", static_cast<uint32_t>(global_d));

            ColDescriptor inner_desc{};
            std::memcpy(&inner_desc, record_ptr + 4u, COL_DESC_BYTES);
            // null_offset was repurposed by the writer to carry this sub-column's row count
            // (not a real offset — Variant sub-columns are always written with is_nullable=
            // false, so nothing ever reads null_offset as an offset for them). offsets_offset
            // and data_offset+data_size are real byte ranges though, and otherwise-untrusted
            // (guest/network-controlled): without confining them to this COL_VARIANT's own
            // [data_offset, data_offset+data_size) region, a malformed frame could point one
            // alternative's sub-column at bytes belonging to a sibling top-level column or
            // the frame header and still decode "successfully".
            uint32_t sub_rows = checkFitsUint32(inner_desc.null_offset, "Variant sub-column row count read from frame");
            uint64_t sub_region_end = desc.data_offset + desc.data_size;
            // The sub-columns' payload starts right after this variant header (the writer places
            // it there), so the region a sub-descriptor may address begins at payload_start, not
            // at desc.data_offset. Without that lower bound a malformed frame could point an
            // alternative back at the header bytes that describe it and have them decoded as
            // payload instead of being rejected.
            uint64_t payload_start = desc.data_offset + 4u + static_cast<uint64_t>(k) * record_bytes;
            // offsets_offset's *start* being inside the region isn't enough: its full extent
            // (e.g. COL_BYTES's uint64[rows+1] array) depends on the alternative's raw type, so
            // checking only the start here would let a malformed frame point an alternative's
            // offsets array partly at sibling bytes outside this COL_VARIANT's own blob while
            // still passing this check. Confining the recursive decode below to a subspan that
            // ends at sub_region_end makes every one of its own internal bounds checks (which
            // compare against buf.size()) automatically bound against this region instead of
            // the whole frame, without duplicating per-type offset-array size formulas here.
            if (inner_desc.offsets_offset != 0
                && (inner_desc.offsets_offset < payload_start || inner_desc.offsets_offset > sub_region_end))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_VARIANT sub-variant offsets_offset {} outside variant data region [{}, {})",
                    inner_desc.offsets_offset, payload_start, sub_region_end);
            if (inner_desc.data_offset < payload_start
                || inner_desc.data_offset > sub_region_end
                || inner_desc.data_size > sub_region_end - inner_desc.data_offset)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_VARIANT sub-variant data range [{}, {}) outside variant data region [{}, {})",
                    inner_desc.data_offset, inner_desc.data_offset + inner_desc.data_size,
                    payload_start, sub_region_end);
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
                // sub_region_end <= buf.size() always (this COL_VARIANT descriptor is itself
                // validated against buf.size() by its caller), so this subspan is safe; see the
                // comment above on why confining it here closes the offsets_offset extent gap.
                variant_cols.push_back(readColumnFromDesc(
                    buf.subspan(0, desc.data_offset + desc.data_size), inner_desc, sub_rows, alt_types[g]));
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
                        "ColumnBinary: COL_VARIANT row {} has invalid discriminator/offset: discr={}, offset={}",
                        i, static_cast<uint32_t>(d), row_off);
                ++next_offset[d];
            }
            discr_col->getData()[i] = d;
            offs_col->getData()[i] = row_off;
        }

        col = ColumnVariant::create(std::move(discr_col), std::move(offs_col), std::move(variant_cols));
    }
    else if (raw_type == COL_LOWCARD)
    {
        const auto * lowcard_type = typeid_cast<const DataTypeLowCardinality *>(base_type.get());
        if (!lowcard_type)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_LOWCARD descriptor does not match declared type {}", base_type->getName());

        // Header: uint32 dict_row_count, uint8 index_elem_width, uint8[3] pad, ColDescriptor dict_desc.
        constexpr uint64_t header_bytes = 4u + 4u + COL_DESC_BYTES;
        if (desc.data_size < header_bytes)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_LOWCARD header truncated: data_size={}", desc.data_size);
        const uint8_t * header = buf.data() + desc.data_offset;
        uint32_t dict_row_count = unalignedLoad<uint32_t>(header);
        uint8_t index_elem_width = header[4];
        if (index_elem_width != 1 && index_elem_width != 2 && index_elem_width != 4 && index_elem_width != 8)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_LOWCARD index element width {} is not one of 1/2/4/8",
                static_cast<uint32_t>(index_elem_width));

        ColDescriptor dict_desc{};
        std::memcpy(&dict_desc, header + 8u, COL_DESC_BYTES);

        // dict_desc is read from otherwise-untrusted guest/network bytes; confine it to
        // this COL_LOWCARD's own [data_offset, data_offset+data_size) region before
        // recursing, same reasoning as the COL_VARIANT sub-descriptor check above.
        // Unlike COL_VARIANT's inner_desc (whose null_offset is repurposed to carry a row
        // count, not a real offset), dict_desc.null_offset IS a real null-map byte offset
        // whenever the dictionary itself is Nullable (LowCardinality(Nullable(T))), since
        // it's written via the ordinary buildColDescriptor path — so it needs the same
        // containment check as offsets_offset/data_offset, not an exemption.
        uint64_t region_end = desc.data_offset + desc.data_size;
        // COL_IS_CONST on the dictionary descriptor is never emitted by the writer, and the
        // ColumnConst that readColumnFromDesc would return for it is not a valid ColumnUnique
        // holder: ColumnUnique rejects a nullable holder but not a const one, and its
        // constructor immediately calls reverse_index.setColumn(getRawColumnPtr()), whose
        // assert_cast is a plain static_cast in release builds. Reject the bit here rather
        // than letting a malformed frame reach that cast.
        if (dict_desc.type & COL_IS_CONST)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_LOWCARD dictionary descriptor must not set COL_IS_CONST");
        // The dictionary payload starts right after this header (the writer places it there),
        // so the addressable region begins at payload_start, not at desc.data_offset — otherwise
        // a malformed frame could point the dictionary back at the header bytes that describe it
        // and have them decoded as dictionary values instead of being rejected.
        uint64_t payload_start = desc.data_offset + header_bytes;
        if (dict_desc.null_offset != 0
            && (dict_desc.null_offset < payload_start || dict_desc.null_offset > region_end))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_LOWCARD dictionary null_offset {} outside data region [{}, {})",
                dict_desc.null_offset, payload_start, region_end);
        if (dict_desc.offsets_offset != 0
            && (dict_desc.offsets_offset < payload_start || dict_desc.offsets_offset > region_end))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_LOWCARD dictionary offsets_offset {} outside data region [{}, {})",
                dict_desc.offsets_offset, payload_start, region_end);
        if (dict_desc.data_offset < payload_start
            || dict_desc.data_offset > region_end
            || dict_desc.data_size > region_end - dict_desc.data_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_LOWCARD dictionary data range [{}, {}) outside data region [{}, {})",
                dict_desc.data_offset, dict_desc.data_offset + dict_desc.data_size, payload_start, region_end);

        // Confining to a subspan ending at region_end (rather than checking
        // null_offset/offsets_offset as start-only pointers) closes the extent gap for both:
        // a malformed frame could otherwise point either one's start inside this COL_LOWCARD's
        // own blob while its full byte range (dict_row_count null-map bytes, or the
        // type-dependent offsets array) still reaches into sibling bytes. region_end <=
        // buf.size() always, since this descriptor is itself validated against buf.size() by
        // its caller, so the subspan is safe.
        // removeNullable: for a LowCardinality(Nullable(T)) the writer serializes the
        // dictionary as a NON-nullable sub-column (buildColDescriptor unwraps
        // ColumnUnique::getNestedColumn's Nullable wrapper and passes is_nullable=false), so
        // the dictionary descriptor never carries COL_IS_NULLABLE. Handing the declared
        // Nullable(T) down would make the tag branches below disagree with the bytes on the
        // wire: they would build a ColumnNullable, which ColumnUnique rejects as a holder
        // ("Holder column for ColumnUnique can't be nullable"), and the COL_BYTES branch's
        // declared-type check would reject a well-formed frame outright. Nullability is
        // conveyed by ColumnUnique's reserved slot layout instead, and createColumnUnique
        // below is still given the full Nullable(T) type so it allocates those slots.
        auto dict_col = readColumnFromDesc(
            buf.subspan(0, region_end), dict_desc, dict_row_count, removeNullable(lowcard_type->getDictionaryType()));

        // Unlike null_offset, offsets_offset has no "absent" meaning for COL_LOWCARD: the
        // index array is mandatory, and the writer's cursor starts past the header and
        // descriptor table, so it is never 0 in a genuine frame. Without this check a frame
        // setting it to 0 makes idx_src point at the frame header, so a 1-row/1-byte-index
        // column silently takes its dictionary index from the low byte of num_rows instead of
        // from any real index array - metadata reparsed as payload rather than a rejection.
        if (desc.offsets_offset == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_LOWCARD descriptor has no index array (offsets_offset is 0)");
        if (desc.offsets_offset > buf.size()
            || static_cast<uint64_t>(rows_to_dec) * index_elem_width > buf.size() - desc.offsets_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_LOWCARD index array out of bounds: offset={}, rows={}, width={}, buf={}",
                desc.offsets_offset, rows_to_dec, static_cast<uint32_t>(index_elem_width), buf.size());
        const uint8_t * idx_src = buf.data() + desc.offsets_offset;

        MutableColumnPtr idx_col;
        switch (index_elem_width)
        {
            case 1: idx_col = ColumnUInt8::create(); break;
            case 2: idx_col = ColumnUInt16::create(); break;
            case 4: idx_col = ColumnUInt32::create(); break;
            default: idx_col = ColumnUInt64::create(); break;
        }
        idx_col->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(idx_col->getRawData().data()), idx_src,
                    static_cast<uint64_t>(rows_to_dec) * index_elem_width);

        // Every index value must reference a real dictionary row, or a malformed frame
        // could make ColumnLowCardinality's later use of this index read out of bounds
        // into the dictionary's underlying storage.
        for (uint32_t i = 0; i < rows_to_dec; ++i)
            if (idx_col->getUInt(i) >= dict_row_count)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: COL_LOWCARD index {} at row {} exceeds dictionary size {}",
                    idx_col->getUInt(i), i, dict_row_count);

        // `ColumnUnique` reserves the leading dictionary slots for its special values:
        // `numSpecialValues` is 1 for a plain dictionary (slot 0 = the nested default) and 2
        // for a nullable one (slot 0 = the NULL sentinel, slot 1 = the nested default). The
        // wire carries the dictionary of a LowCardinality(Nullable(T)) column as a
        // NON-nullable sub-column: the writer above unwraps ColumnUnique::getNestedColumn's
        // Nullable wrapper and passes is_nullable=false, so nullability is conveyed purely by
        // slot 0's position, not by an encoded null map. A frame that is short of those
        // reserved slots is malformed; the ColumnUnique constructor would otherwise report it
        // as ILLEGAL_COLUMN ("Too small holder column"), which reads as an internal error
        // rather than as bad input.
        const auto & dictionary_type = *lowcard_type->getDictionaryType();
        const uint32_t num_special_values = dictionary_type.isNullable() ? 2 : 1;
        if (dict_row_count < num_special_values)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: COL_LOWCARD dictionary of type {} has {} rows, but its reserved leading "
                "slots require at least {}", dictionary_type.getName(), dict_row_count, num_special_values);
        auto unique_col = DataTypeLowCardinality::createColumnUnique(dictionary_type, std::move(dict_col));
        col = ColumnLowCardinality::create(std::move(unique_col), std::move(idx_col), /* is_shared */ false);
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "ColumnBinary: unsupported output ColType {}", raw_type);
    }

    if (is_const)
    {
        // rows_to_dec is unconditionally 1 for a const column (the wire always stores exactly
        // one value for COL_IS_CONST, regardless of the frame's actual row count), so `col`
        // above was decoded as a 1-element column even when num_rows == 0. Returning it as-is
        // here would hand back a 1-row column for what must be a 0-row result, violating the
        // caller's row-count invariant (e.g. Chunk requires every column to have the same
        // number of rows as the chunk itself).
        if (num_rows == 0)
            return col->cloneEmpty();
        return ColumnConst::create(std::move(col), num_rows);
    }
    return col;
}

// Decode a single-column ColumnBinary output buffer into a MutableColumnPtr.
// This is the entry point used by WASM UDF executors; it enforces num_cols == 1.
// result_type drives recursive decoding for COL_COMPLEX.
inline MutableColumnPtr readColumnarOutput(
    std::span<const uint8_t> buf,
    const DataTypePtr & result_type,
    size_t expected_rows)
{
    if (buf.size() < FRAME_HEADER_BYTES + COL_DESC_BYTES)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary output buffer too small: {} bytes", buf.size());

    uint32_t num_rows = 0;
    uint32_t num_cols = 0;
    readFrameHeader(buf.data(), num_rows, num_cols);

    if (num_rows != expected_rows)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary output row count mismatch: expected {}, got {}", expected_rows, num_rows);
    if (num_cols != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary output must have exactly 1 column, got {}", num_cols);

    ColDescriptor desc{};
    std::memcpy(&desc, buf.data() + FRAME_HEADER_BYTES, sizeof(desc));

    // Mirrors ColumnBinaryInputFormat::read()'s frame validator: null_offset/offsets_offset use
    // 0 as the "absent" sentinel, but any nonzero value, and data_offset unconditionally (it has
    // no absent sentinel — the writer's cursor always starts at hdr_desc_size and only grows),
    // must point at or past the end of the header + descriptor table. Without this check, a
    // hostile or buggy WASM module could set e.g. data_offset = 0 and have readColumnFromDesc
    // silently decode header/descriptor bytes as the column's payload instead of throwing.
    constexpr uint64_t hdr_desc_size = FRAME_HEADER_BYTES + COL_DESC_BYTES; // num_cols == 1
    for (uint64_t off : {desc.null_offset, desc.offsets_offset})
        if (off != 0 && off < hdr_desc_size)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary output descriptor offset {} points inside the header/descriptor table (< {})",
                off, hdr_desc_size);
    if (desc.data_offset < hdr_desc_size)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary output descriptor data_offset {} points inside the header/descriptor table (< {})",
            desc.data_offset, hdr_desc_size);

    // Confine the decode to this column's own declared region, mirroring
    // ColumnBinaryInputFormat::read()'s per-column subspan. The checks above only reject
    // offsets pointing back into the header/descriptor area; without an upper bound a guest
    // could declare a 1-byte payload and still source its offsets array or null map from
    // trailing bytes past data_offset + data_size, reading outside the blob it declared.
    if (desc.data_size > std::numeric_limits<uint64_t>::max() - desc.data_offset)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary output descriptor data_offset + data_size overflows: offset={}, size={}",
            desc.data_offset, desc.data_size);
    const uint64_t region_end = desc.data_offset + desc.data_size;
    if (region_end > buf.size())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary output descriptor data range [{}, {}) exceeds buffer size {}",
            desc.data_offset, region_end, buf.size());
    return readColumnFromDesc(buf.subspan(0, region_end), desc, num_rows, result_type);
}

}
}
