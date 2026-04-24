#pragma once

/// Wire format helpers for COLUMNAR_V1 WASM UDF ABI.
///
/// Extracted into a header so they can be unit-tested without linking the full
/// UserDefinedWebAssembly translation unit.
///
/// All functions are inline — safe to include from multiple TUs.

#include <cstring>
#include <span>
#include <functional>

#include <Columns/ColumnArray.h>
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
    extern const int WASM_ERROR;
}

namespace ColumnarV1
{

constexpr uint32_t COL_BYTES        = 0;
constexpr uint32_t COL_NULL_BYTES   = 1;
constexpr uint32_t COL_FIXED8       = 2;
constexpr uint32_t COL_NULL_FIXED8  = 3;
constexpr uint32_t COL_FIXED32      = 4;
constexpr uint32_t COL_NULL_FIXED32 = 5;
constexpr uint32_t COL_FIXED64      = 6;
constexpr uint32_t COL_NULL_FIXED64 = 7;
constexpr uint32_t COL_COMPLEX      = 8;  // Array(T) / Tuple(T...) — recursive format
constexpr uint32_t COL_VARIANT      = 9;  // Variant(...) — discriminated union
constexpr uint32_t COL_IS_CONST     = 0x80u;
// COL_IS_REPEAT: column is cyclic with period R.  Row i maps to stored_row[i % R].
// offsets_offset field carries R (not a byte offset).
// For string columns the R+1 wire offsets are embedded at the start of the data block.
// Cuts wire size from N*elem_size to R*elem_size when a JOIN repeats R unique values.
constexpr uint32_t COL_IS_REPEAT    = 0x40u;

constexpr uint32_t COLUMNAR_HEADER_BYTES = 8;
constexpr uint32_t COLUMNAR_DESC_BYTES   = 20;

struct ColDescriptor
{
    uint32_t type;
    uint32_t null_offset;
    uint32_t offsets_offset;
    uint32_t data_offset;
    uint32_t data_size;
};
static_assert(sizeof(ColDescriptor) == COLUMNAR_DESC_BYTES);

// Find the smallest period R such that col[i] == col[i % R] for all scanned rows.
// Returns 0 if no period is found within budget.
// Budget: scan at most PERIOD_SCAN_LIMIT elements; period must not exceed MAX_PERIOD.
inline uint32_t detectPeriod(const IColumn * col, uint32_t num_rows)
{
    constexpr uint32_t PERIOD_SCAN_LIMIT = 8192;
    constexpr uint32_t MAX_PERIOD        = 4096;
    uint32_t scan = std::min(num_rows, PERIOD_SCAN_LIMIT);
    for (uint32_t r = 1; r <= std::min(scan / 2, MAX_PERIOD); ++r)
    {
        bool ok = true;
        for (uint32_t i = r; i < scan; ++i)
        {
            if (col->compareAt(i, i % r, *col, 1) != 0) { ok = false; break; }
        }
        if (ok) return r;
    }
    return 0;
}

// ── COL_COMPLEX recursive helpers ────────────────────────────────────────────
//
// COL_COMPLEX data block layout (recursive, mirrors the output decoder):
//   Array(T):   uint32 offsets[n+1]  +  complexDataBlock(inner, total_elems)
//   Tuple(T..): complexDataBlock(field_0, n) + complexDataBlock(field_1, n) + ...
//   String:     uint32 offsets[n+1]  +  null-terminated chars
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
        return (n + 1u) * 4u + complexDataSize(arr->getData(), total);
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
        uint32_t chars = static_cast<uint32_t>(str.getChars().size()) + n;
        return (n + 1u) * 4u + chars;
    }
    // Fixed-width fallback (ColumnVector<T>, ColumnUInt8, etc.)
    return n * static_cast<uint32_t>(col.sizeOfValueIfFixed());
}

inline void writeComplexData(const IColumn & col, uint32_t n, uint8_t * dst)
{
    if (const auto * arr = typeid_cast<const ColumnArray *>(&col))
    {
        const auto & ch_offs = arr->getOffsets();
        uint32_t * wire_offs = reinterpret_cast<uint32_t *>(dst);
        wire_offs[0] = 0u;
        for (uint32_t i = 0; i < n; ++i)
            wire_offs[i + 1u] = static_cast<uint32_t>(ch_offs[i]);
        uint32_t total = static_cast<uint32_t>(arr->getData().size());
        writeComplexData(arr->getData(), total, dst + (n + 1u) * 4u);
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
        uint32_t * wire_offs = reinterpret_cast<uint32_t *>(dst);
        uint8_t  * chars_dst = dst + (n + 1u) * 4u;
        wire_offs[0] = 0u;
        uint32_t wire_pos = 0u;
        uint32_t ch_pos   = 0u;
        for (uint32_t i = 0; i < n; ++i)
        {
            uint32_t end = static_cast<uint32_t>(ch_offs[i]);
            uint32_t len = end - ch_pos;
            std::memcpy(chars_dst + wire_pos, chars.data() + ch_pos, len);
            wire_pos += len;
            chars_dst[wire_pos++] = '\0';
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
inline uint32_t buildColDescriptor(
    const IColumn * col,
    bool is_const,
    bool is_nullable,
    uint32_t num_rows,
    uint32_t write_cursor,
    ColDescriptor & desc)
{
    // Budget for COL_IS_REPEAT: only encode if the R unique rows fit under this limit.
    constexpr uint64_t REPEAT_DATA_LIMIT = 64u * 1024u * 1024u;  // 64 MB

    // ── Variant column → COL_VARIANT ─────────────────────────────────────────
    // Wire layout:
    //   null_offset    → discriminators[num_rows]    uint8 (global discr; 0xFF=NULL)
    //   offsets_offset → row_offsets[num_rows]       uint32 (pos within sub-column)
    //   data_offset    → variant header:
    //     uint32 K                                   (number of present sub-variants)
    //     K × { uint8 global_discriminator           (4-byte aligned record)
    //           uint8[3] pad
    //           ColDescriptor inner_desc }           (20 bytes, abs buffer offsets)
    //     (sub-column data at positions given by inner_desc)
    if (const auto * var_col = typeid_cast<const ColumnVariant *>(col))
    {
        desc.type = COL_VARIANT | (is_const ? COL_IS_CONST : 0u);

        desc.null_offset = write_cursor;
        write_cursor += num_rows;

        write_cursor = (write_cursor + 3u) & ~3u;
        desc.offsets_offset = write_cursor;
        write_cursor += num_rows * sizeof(uint32_t);

        write_cursor = (write_cursor + 3u) & ~3u;
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
        desc.type        = COL_COMPLEX | (is_const ? COL_IS_CONST : 0u);
        desc.null_offset = 0;

        write_cursor = (write_cursor + 3u) & ~3u;
        desc.offsets_offset = write_cursor;
        write_cursor += (num_rows + 1u) * sizeof(uint32_t);

        const IColumn & nested = arr_col->getData();
        uint32_t total_elems = static_cast<uint32_t>(nested.size());

        desc.data_offset = write_cursor;
        desc.data_size   = complexDataSize(nested, total_elems);
        write_cursor    += desc.data_size;
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
        uint32_t base_type = is_nullable ? COL_NULL_BYTES : COL_BYTES;

        // COL_IS_REPEAT detection for non-const string columns.
        // offsets_offset stores R (the period); string offsets + chars live in the data block.
        if (!is_const && num_rows > 1)
        {
            uint32_t r = detectPeriod(str_col, num_rows);
            if (r > 0 && r < num_rows)
            {
                // Compute bytes needed for the R unique strings.
                // CH ColumnString stores all chars concatenated; the R-th string ends at
                // str_col->getOffsets()[R-1].  Add R null terminators for wire format.
                uint64_t unique_chars = static_cast<uint64_t>(str_col->getOffsets()[r - 1]) + r;
                uint64_t unique_data  = (r + 1u) * sizeof(uint32_t) + unique_chars;
                if (unique_data < REPEAT_DATA_LIMIT)
                {
                    desc.type           = base_type | COL_IS_REPEAT;
                    desc.null_offset    = 0;  // nullable repeat not yet supported
                    desc.offsets_offset = r;  // period, NOT a byte offset
                    write_cursor        = (write_cursor + 3u) & ~3u;
                    desc.data_offset    = write_cursor;
                    desc.data_size      = static_cast<uint32_t>(unique_data);
                    write_cursor       += desc.data_size;
                    return write_cursor;
                }
            }
        }

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

        write_cursor = (write_cursor + 3u) & ~3u;
        desc.offsets_offset = write_cursor;
        write_cursor += (num_rows + 1u) * sizeof(uint32_t);

        desc.data_offset = write_cursor;
        uint32_t total_chars = static_cast<uint32_t>(str_col->getChars().size()) + num_rows;
        desc.data_size = total_chars;
        write_cursor += total_chars;
        return write_cursor;
    }

    uint32_t elem_size = static_cast<uint32_t>(col->sizeOfValueIfFixed());
    // 2-byte types (UInt16/Int16) are promoted to FIXED32 (4 bytes, zero-padded)
    // so WASM reads a clean 4-byte value without touching adjacent memory.
    uint32_t wire_elem_size = (elem_size == 2) ? 4u : elem_size;
    uint32_t base_type;
    if      (wire_elem_size == 1) base_type = is_nullable ? COL_NULL_FIXED8  : COL_FIXED8;
    else if (wire_elem_size == 4) base_type = is_nullable ? COL_NULL_FIXED32 : COL_FIXED32;
    else                          base_type = is_nullable ? COL_NULL_FIXED64 : COL_FIXED64;

    // COL_IS_REPEAT detection for non-const fixed-width columns.
    if (!is_const && num_rows > 1)
    {
        uint32_t r = detectPeriod(col, num_rows);
        if (r > 0 && r < num_rows)
        {
            uint64_t unique_data = static_cast<uint64_t>(r) * wire_elem_size;
            if (unique_data < REPEAT_DATA_LIMIT)
            {
                desc.type           = base_type | COL_IS_REPEAT;
                desc.null_offset    = 0;
                desc.offsets_offset = r;  // period
                desc.data_offset    = write_cursor;
                desc.data_size      = r * wire_elem_size;
                write_cursor       += r * wire_elem_size;
                return write_cursor;
            }
        }
    }

    desc.type         = base_type | (is_const ? COL_IS_CONST : 0u);
    desc.null_offset  = 0;
    desc.offsets_offset = 0;
    desc.data_offset  = write_cursor;
    desc.data_size    = num_rows * wire_elem_size;
    write_cursor     += num_rows * wire_elem_size;
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
        uint32_t sub_cursor = desc.data_offset + 4u + k * (4u + COLUMNAR_DESC_BYTES);

        for (uint32_t local = 0; local < num_variants; ++local)
        {
            const IColumn & sub = var_col->getVariantByLocalDiscriminator(local);
            if (sub.empty())
                continue;

            uint8_t global_d = var_col->globalDiscriminatorByLocal(static_cast<ColumnVariant::Discriminator>(local));
            uint32_t sub_rows = static_cast<uint32_t>(sub.size());

            ColDescriptor inner_desc{};
            sub_cursor = buildColDescriptor(&sub, false, false, sub_rows, sub_cursor, inner_desc);
            inner_desc.null_offset = sub_rows;  // sub_rows stored for WASM navigation

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

        uint32_t * wire_outer = reinterpret_cast<uint32_t *>(buf.data() + desc.offsets_offset);
        wire_outer[0] = 0u;
        for (uint32_t i = 0; i < num_rows; ++i)
            wire_outer[i + 1u] = static_cast<uint32_t>(ch_offsets[i]);

        writeComplexData(nested, total_elems, buf.data() + desc.data_offset);
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
        const auto & nm = null_col->getNullMapData();
        std::memcpy(buf.data() + desc.null_offset, nm.data(), num_rows);
    }

    const ColumnString * str_col = typeid_cast<const ColumnString *>(col);
    if (str_col)
    {
        const auto & ch_offsets = str_col->getOffsets();
        const auto & chars = str_col->getChars();

        // COL_IS_REPEAT: write only the R unique rows; offsets embedded in data block.
        bool is_repeat = (desc.type & COL_IS_REPEAT) != 0;
        uint32_t write_rows = is_repeat ? desc.offsets_offset : num_rows;

        uint32_t * wire_offsets = is_repeat
            ? reinterpret_cast<uint32_t *>(buf.data() + desc.data_offset)
            : reinterpret_cast<uint32_t *>(buf.data() + desc.offsets_offset);
        uint8_t * data_dst = is_repeat
            ? buf.data() + desc.data_offset + (write_rows + 1u) * sizeof(uint32_t)
            : buf.data() + desc.data_offset;

        wire_offsets[0] = 0;
        uint32_t wire_pos = 0;
        uint32_t ch_pos = 0;
        for (uint32_t i = 0; i < write_rows; ++i)
        {
            uint32_t str_end = static_cast<uint32_t>(ch_offsets[i]);
            uint32_t str_len = str_end - ch_pos;
            std::memcpy(data_dst + wire_pos, chars.data() + ch_pos, str_len);
            wire_pos += str_len;
            data_dst[wire_pos++] = '\0';
            wire_offsets[i + 1] = wire_pos;
            ch_pos = str_end;
        }
        return;
    }

    const auto * raw      = col->getRawData().data();
    uint32_t     src_elem = static_cast<uint32_t>(col->sizeOfValueIfFixed());
    uint32_t     dst_elem = (src_elem == 2) ? 4u : src_elem;

    // COL_IS_REPEAT: write only the R unique rows.
    bool is_repeat = (desc.type & COL_IS_REPEAT) != 0;
    uint32_t write_rows = is_repeat ? desc.offsets_offset : num_rows;

    if (src_elem == dst_elem)
    {
        std::memcpy(buf.data() + desc.data_offset, raw, write_rows * dst_elem);
    }
    else
    {
        // 2-byte → 4-byte promotion: zero-pad each element to 4 bytes.
        uint8_t * dst = buf.data() + desc.data_offset;
        for (uint32_t i = 0; i < write_rows; ++i)
        {
            uint32_t v = 0;
            std::memcpy(&v, raw + i * src_elem, src_elem);
            std::memcpy(dst + i * dst_elem, &v, dst_elem);
        }
    }
}

// Decode a single-column COLUMNAR_V1 output buffer into a MutableColumnPtr.
// result_type drives recursive decoding for COL_COMPLEX.
inline MutableColumnPtr readColumnarOutput(
    std::span<const uint8_t> buf,
    const DataTypePtr & result_type,
    size_t expected_rows)
{
    if (buf.size() < COLUMNAR_HEADER_BYTES + COLUMNAR_DESC_BYTES)
        throw Exception(ErrorCodes::WASM_ERROR,
            "COLUMNAR_V1 output buffer too small: {} bytes", buf.size());

    uint32_t num_rows, num_cols;
    std::memcpy(&num_rows, buf.data(),     4);
    std::memcpy(&num_cols, buf.data() + 4, 4);

    if (num_rows != expected_rows)
        throw Exception(ErrorCodes::WASM_ERROR,
            "COLUMNAR_V1 output row count mismatch: expected {}, got {}", expected_rows, num_rows);
    if (num_cols != 1)
        throw Exception(ErrorCodes::WASM_ERROR,
            "COLUMNAR_V1 output must have exactly 1 column, got {}", num_cols);

    ColDescriptor desc;
    std::memcpy(&desc, buf.data() + COLUMNAR_HEADER_BYTES, sizeof(desc));

    uint32_t raw_type = desc.type & ~(COL_IS_CONST | COL_IS_REPEAT);

    if (raw_type == COL_BYTES || raw_type == COL_NULL_BYTES)
    {
        const uint32_t * wire_offsets = reinterpret_cast<const uint32_t *>(buf.data() + desc.offsets_offset);
        const uint8_t * data = buf.data() + desc.data_offset;

        auto col_str = ColumnString::create();
        auto & chars   = col_str->getChars();
        auto & offsets = col_str->getOffsets();

        offsets.resize(num_rows);
        uint32_t ch_pos = 0;
        for (uint32_t i = 0; i < num_rows; ++i)
        {
            uint32_t wire_end   = wire_offsets[i + 1];
            uint32_t wire_start = wire_offsets[i];
            uint32_t str_len    = wire_end - wire_start;
            if (str_len > 0) str_len--;
            chars.resize(ch_pos + str_len);
            std::memcpy(chars.data() + ch_pos, data + wire_start, str_len);
            ch_pos += str_len;
            offsets[i] = ch_pos;
        }

        if (raw_type == COL_NULL_BYTES && desc.null_offset)
        {
            auto null_col = ColumnUInt8::create(num_rows);
            std::memcpy(null_col->getData().data(), buf.data() + desc.null_offset, num_rows);
            return ColumnNullable::create(std::move(col_str), std::move(null_col));
        }
        return col_str;
    }

    if (raw_type == COL_FIXED8 || raw_type == COL_NULL_FIXED8)
    {
        auto col_u8 = ColumnUInt8::create(num_rows);
        std::memcpy(col_u8->getData().data(), buf.data() + desc.data_offset, num_rows);
        if (raw_type == COL_NULL_FIXED8 && desc.null_offset)
        {
            auto null_col = ColumnUInt8::create(num_rows);
            std::memcpy(null_col->getData().data(), buf.data() + desc.null_offset, num_rows);
            return ColumnNullable::create(std::move(col_u8), std::move(null_col));
        }
        return col_u8;
    }

    // Fixed32 — create column matching the declared return type (Int32, UInt32, Float32, etc.)
    if (raw_type == COL_FIXED32 || raw_type == COL_NULL_FIXED32)
    {
        const DataTypePtr & base_type = (raw_type == COL_NULL_FIXED32)
            ? dynamic_cast<const DataTypeNullable &>(*result_type).getNestedType()
            : result_type;
        auto col32 = base_type->createColumn();
        col32->insertManyDefaults(num_rows);
        std::memcpy(const_cast<char *>(col32->getRawData().data()),
                    buf.data() + desc.data_offset, num_rows * 4);
        if (raw_type == COL_NULL_FIXED32 && desc.null_offset)
        {
            auto null_col = ColumnUInt8::create(num_rows);
            std::memcpy(null_col->getData().data(), buf.data() + desc.null_offset, num_rows);
            return ColumnNullable::create(std::move(col32), std::move(null_col));
        }
        return col32;
    }

    if (raw_type == COL_FIXED64 || raw_type == COL_NULL_FIXED64)
    {
        const DataTypePtr & base_type = (raw_type == COL_NULL_FIXED64)
            ? dynamic_cast<const DataTypeNullable &>(*result_type).getNestedType()
            : result_type;
        auto col64 = base_type->createColumn();
        col64->insertManyDefaults(num_rows);
        std::memcpy(const_cast<char *>(col64->getRawData().data()),
                    buf.data() + desc.data_offset, num_rows * 8);
        if (raw_type == COL_NULL_FIXED64 && desc.null_offset)
        {
            auto null_col = ColumnUInt8::create(num_rows);
            std::memcpy(null_col->getData().data(), buf.data() + desc.null_offset, num_rows);
            return ColumnNullable::create(std::move(col64), std::move(null_col));
        }
        return col64;
    }

    if (raw_type == COL_COMPLEX)
    {
        const uint8_t * data_ptr = buf.data() + desc.data_offset;

        std::function<MutableColumnPtr(const uint8_t *&, const DataTypePtr &, uint32_t)> decode;
        decode = [&](const uint8_t *& p, const DataTypePtr & type, uint32_t n) -> MutableColumnPtr
        {
            if (const auto * arr_type = typeid_cast<const DataTypeArray *>(type.get()))
            {
                const uint32_t * outer_offs = reinterpret_cast<const uint32_t *>(p);
                p += (n + 1u) * sizeof(uint32_t);
                uint32_t total_elems = outer_offs[n];

                auto nested_col = decode(p, arr_type->getNestedType(), total_elems);

                auto offsets_col = ColumnUInt64::create(n);
                for (uint32_t i = 0; i < n; ++i)
                    offsets_col->getData()[i] = static_cast<UInt64>(outer_offs[i + 1u]);

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
                const uint32_t * wire_offs = reinterpret_cast<const uint32_t *>(p);
                p += (n + 1u) * sizeof(uint32_t);
                const uint8_t * chars_src = p;
                uint32_t total_chars = wire_offs[n];
                p += total_chars;

                auto col_str = ColumnString::create();
                auto & chars   = col_str->getChars();
                auto & offsets = col_str->getOffsets();
                offsets.resize(n);
                uint32_t ch_pos = 0u;
                for (uint32_t i = 0; i < n; ++i)
                {
                    uint32_t wire_end   = wire_offs[i + 1u];
                    uint32_t wire_start = wire_offs[i];
                    uint32_t str_len    = wire_end - wire_start;
                    if (str_len > 0u) str_len--;
                    chars.resize(ch_pos + str_len);
                    std::memcpy(chars.data() + ch_pos, chars_src + wire_start, str_len);
                    ch_pos += str_len;
                    offsets[i] = ch_pos;
                }
                return col_str;
            }

            auto col = type->createColumn();
            col->insertManyDefaults(n);
            uint32_t elem_bytes = static_cast<uint32_t>(type->getSizeOfValueInMemory());
            std::memcpy(const_cast<char *>(col->getRawData().data()), p, n * elem_bytes);
            p += n * elem_bytes;
            return col;
        };

        return decode(data_ptr, result_type, num_rows);
    }

    throw Exception(ErrorCodes::WASM_ERROR, "COLUMNAR_V1: unsupported output ColType {}", raw_type);
}

} // namespace ColumnarV1
} // namespace DB
