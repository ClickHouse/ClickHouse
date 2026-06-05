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
///   COL_COMPLEX (5) — Array(T) or Tuple(T…), recursive layout
///   COL_VARIANT (6) — Variant(…), discriminated union with per-row offsets
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

#include <cstring>
#include <span>
#include <functional>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
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

// ── COL_COMPLEX recursive helpers ────────────────────────────────────────────
//
// COL_COMPLEX data block layout (recursive, mirrors the output decoder):
//   Array(T):   uint64 offsets[n+1]  +  complexDataBlock(inner, total_elems)
//   Tuple(T..): complexDataBlock(field_0, n) + complexDataBlock(field_1, n) + ...
//   String:     uint64 offsets[n+1]  +  chars (no null terminators)
//   Fixed:      raw bytes[n * elem_bytes]

// Forward declaration — complexDataSize and writeComplexData are mutually recursive
// only via lambdas; we declare the inline wrappers here.
inline uint32_t complexDataSize(const IColumn & col, uint32_t n);
inline void     writeComplexData(const IColumn & col, uint32_t n, uint8_t * dst);

inline uint32_t complexDataSize(const IColumn & col, uint32_t n)
{
    if (const auto * arr = typeid_cast<const ColumnArray *>(&col))
    {
        uint32_t total = static_cast<uint32_t>(arr->getData().size());
        return (n + 1u) * 8u + complexDataSize(arr->getData(), total);
    }
    if (const auto * tup = typeid_cast<const ColumnTuple *>(&col))
    {
        uint32_t sz = 0;
        for (const auto & field : tup->getColumns())
            sz += complexDataSize(*field, n);
        return sz;
    }
    if (typeid_cast<const ColumnString *>(&col))
    {
        const auto & str = assert_cast<const ColumnString &>(col);
        uint32_t chars = static_cast<uint32_t>(str.getChars().size());
        return (n + 1u) * 8u + chars;
    }
    // Fixed-width fallback (ColumnVector<T>, ColumnUInt8, etc.)
    return n * static_cast<uint32_t>(col.sizeOfValueIfFixed());
}

inline void writeComplexData(const IColumn & col, uint32_t n, uint8_t * dst)
{
    if (const auto * arr = typeid_cast<const ColumnArray *>(&col))
    {
        const auto & ch_offs = arr->getOffsets();
        uint64_t * wire_offs = reinterpret_cast<uint64_t *>(dst);
        wire_offs[0] = 0ull;
        for (uint32_t i = 0; i < n; ++i)
            wire_offs[i + 1u] = static_cast<uint64_t>(ch_offs[i]);
        uint32_t total = static_cast<uint32_t>(arr->getData().size());
        writeComplexData(arr->getData(), total, dst + (n + 1u) * 8u);
        return;
    }
    if (const auto * tup = typeid_cast<const ColumnTuple *>(&col))
    {
        uint32_t pos = 0;
        for (const auto & field : tup->getColumns())
        {
            uint32_t field_sz = complexDataSize(*field, n);
            writeComplexData(*field, n, dst + pos);
            pos += field_sz;
        }
        return;
    }
    if (const auto * str = typeid_cast<const ColumnString *>(&col))
    {
        const auto & ch_offs = str->getOffsets();
        const auto & chars   = str->getChars();
        uint64_t * wire_offs = reinterpret_cast<uint64_t *>(dst);
        uint8_t  * chars_dst = dst + (n + 1u) * 8u;
        wire_offs[0] = 0ull;
        uint64_t wire_pos = 0ull;
        uint64_t ch_pos   = 0ull;
        for (uint32_t i = 0; i < n; ++i)
        {
            uint64_t end = ch_offs[i];
            uint64_t len = end - ch_pos;
            std::memcpy(chars_dst + wire_pos, chars.data() + ch_pos, len);
            wire_pos += len;
            wire_offs[i + 1u] = wire_pos;
            ch_pos = end;
        }
        return;
    }
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
        desc.type           = COL_COMPLEX | (is_const ? COL_IS_CONST : 0u);
        desc.null_offset    = 0;
        desc.offsets_offset = 0;  // unused for Array; outer offsets are at data_offset

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
        desc.type           = COL_COMPLEX | (is_const ? COL_IS_CONST : 0u);
        desc.null_offset    = 0;
        desc.offsets_offset = 0;
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
        uint32_t total_chars = static_cast<uint32_t>(str_col->getChars().size());
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
    else                          base_type = COL_FIXED64 | (is_nullable ? COL_IS_NULLABLE : 0u);

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
    desc.data_size      = num_rows * wire_elem_size;
    write_cursor       += num_rows * wire_elem_size;
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
        uint64_t * wire_outer = reinterpret_cast<uint64_t *>(buf.data() + desc.data_offset);
        wire_outer[0] = 0ull;
        for (uint32_t i = 0; i < num_rows; ++i)
            wire_outer[i + 1u] = static_cast<uint64_t>(ch_offsets[i]);

        writeComplexData(nested, total_elems, buf.data() + desc.data_offset + (num_rows + 1u) * sizeof(uint64_t));
        return;
    }

    // ── Tuple column → COL_COMPLEX ────────────────────────────────────────────
    if (const auto * tup_col = typeid_cast<const ColumnTuple *>(col))
    {
        writeComplexData(*tup_col, num_rows, buf.data() + desc.data_offset);
        return;
    }

    const ColumnNullable * null_col = typeid_cast<const ColumnNullable *>(col);
    if (null_col)
        col = &null_col->getNestedColumn();

    if (is_nullable && null_col && desc.null_offset)
    {
        // Null map: 1=null, 0=non-null — identical to ColumnNullable::getNullMapData().
        const auto & nm = null_col->getNullMapData();
        std::memcpy(buf.data() + desc.null_offset, nm.data(), num_rows);
    }

    const ColumnString * str_col = typeid_cast<const ColumnString *>(col);
    if (str_col)
    {
        const auto & ch_offsets = str_col->getOffsets();
        const auto & chars = str_col->getChars();

        uint64_t * wire_offsets = reinterpret_cast<uint64_t *>(buf.data() + desc.offsets_offset);
        uint8_t * data_dst = buf.data() + desc.data_offset;

        wire_offsets[0] = 0ull;
        uint64_t wire_pos = 0ull;
        uint64_t ch_pos = 0ull;
        for (uint32_t i = 0; i < num_rows; ++i)
        {
            uint64_t str_end = ch_offsets[i];
            uint64_t str_len = str_end - ch_pos;
            std::memcpy(data_dst + wire_pos, chars.data() + ch_pos, str_len);
            wire_pos += str_len;
            wire_offsets[i + 1] = wire_pos;
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

    const uint8_t * const data_end = buf.data() + desc.data_offset + desc.data_size;

    // Recursive decoder for COL_COMPLEX (Array / Tuple / nested scalars).
    std::function<MutableColumnPtr(const uint8_t *&, const DataTypePtr &, uint32_t)> decode;
    decode = [&](const uint8_t *& p, const DataTypePtr & type, uint32_t n) -> MutableColumnPtr
    {
        if (const auto * arr_type = typeid_cast<const DataTypeArray *>(type.get()))
        {
            uint64_t outer_bytes = (n + 1u) * sizeof(uint64_t);
            if (p + outer_bytes > data_end)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX nested Array outer offsets out of bounds");
            const uint64_t * outer_offs = reinterpret_cast<const uint64_t *>(p);
            p += outer_bytes;
            uint32_t total_elems = static_cast<uint32_t>(outer_offs[n]);
            auto nested_col = decode(p, arr_type->getNestedType(), total_elems);
            auto offsets_col = ColumnUInt64::create(n);
            for (uint32_t i = 0; i < n; ++i)
                offsets_col->getData()[i] = outer_offs[i + 1u];
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
            uint64_t off_bytes = (n + 1u) * sizeof(uint64_t);
            if (p + off_bytes > data_end)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX String offsets out of bounds");
            const uint64_t * wire_offs = reinterpret_cast<const uint64_t *>(p);
            p += off_bytes;
            uint64_t total_chars = wire_offs[n];
            if (p + total_chars > data_end)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX String chars out of bounds");
            const uint8_t * chars_src = p;
            p += total_chars;
            auto col_str = ColumnString::create();
            auto & chars   = col_str->getChars();
            auto & offsets = col_str->getOffsets();
            offsets.resize(n);
            uint64_t ch_pos = 0ull;
            for (uint32_t i = 0; i < n; ++i)
            {
                uint64_t wire_end   = wire_offs[i + 1u];
                uint64_t wire_start = wire_offs[i];
                uint64_t str_len    = wire_end - wire_start;
                chars.resize(ch_pos + str_len);
                std::memcpy(chars.data() + ch_pos, chars_src + wire_start, str_len);
                ch_pos += str_len;
                offsets[i] = ch_pos;
            }
            return col_str;
        }
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
        const uint64_t * wire_offsets = reinterpret_cast<const uint64_t *>(buf.data() + desc.offsets_offset);
        const uint8_t  * data         = buf.data() + desc.data_offset;
        auto col_str = ColumnString::create();
        auto & chars   = col_str->getChars();
        auto & offsets = col_str->getOffsets();
        offsets.resize(rows_to_dec);
        uint64_t ch_pos = 0ull;
        for (uint32_t i = 0; i < rows_to_dec; ++i)
        {
            uint64_t wire_end   = wire_offsets[i + 1];
            uint64_t wire_start = wire_offsets[i];
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
        // Use base_type so Int8, Bool, etc. round-trip correctly (not just UInt8).
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(inner->getRawData().data()),
                    buf.data() + desc.data_offset, rows_to_dec);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_FIXED16)
    {
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(inner->getRawData().data()),
                    buf.data() + desc.data_offset, rows_to_dec * 2);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_FIXED32)
    {
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(inner->getRawData().data()),
                    buf.data() + desc.data_offset, rows_to_dec * 4);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_FIXED64)
    {
        if (desc.data_offset + rows_to_dec * 8u > buf.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED64 data out of bounds: offset={}, rows={}, buf={}",
                desc.data_offset, rows_to_dec, buf.size());
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(inner->getRawData().data()),
                    buf.data() + desc.data_offset, rows_to_dec * 8);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_COMPLEX)
    {
        if (const auto * arr_type = typeid_cast<const DataTypeArray *>(base_type.get()))
        {
            // WASM→CH sequential layout: outer uint64 offsets[rows+1] at data_offset,
            // followed immediately by nested writeComplexData-format data.
            const uint64_t outer_offset_bytes = (rows_to_dec + 1u) * sizeof(uint64_t);
            if (outer_offset_bytes > desc.data_size)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX outer offsets exceed data_size: need={}, data_size={}",
                    outer_offset_bytes, desc.data_size);
            const uint8_t * p = buf.data() + desc.data_offset;
            const uint64_t * outer_offs = reinterpret_cast<const uint64_t *>(p);
            p += outer_offset_bytes;
            uint32_t total_elems = static_cast<uint32_t>(outer_offs[rows_to_dec]);
            auto nested_col = decode(p, arr_type->getNestedType(), total_elems);
            auto offsets_col = ColumnUInt64::create(rows_to_dec);
            for (uint32_t i = 0; i < rows_to_dec; ++i)
                offsets_col->getData()[i] = outer_offs[i + 1u];
            col = maybe_nullable(ColumnArray::create(std::move(nested_col), std::move(offsets_col)));
        }
        else
        {
            // Tuple (and other complex types): data is packed at data_offset by writeComplexData.
            const uint8_t * data_ptr = buf.data() + desc.data_offset;
            col = maybe_nullable(decode(data_ptr, base_type, rows_to_dec));
        }
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
