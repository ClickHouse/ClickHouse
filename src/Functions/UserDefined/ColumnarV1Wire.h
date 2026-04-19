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
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/Exception.h>

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
constexpr uint32_t COL_IS_CONST     = 0x80u;

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
    // ── Array column → COL_COMPLEX ────────────────────────────────────────────
    if (const auto * arr_col = typeid_cast<const ColumnArray *>(col))
    {
        desc.type        = COL_COMPLEX | (is_const ? COL_IS_CONST : 0u);
        desc.null_offset = 0;

        write_cursor = (write_cursor + 3u) & ~3u;
        desc.offsets_offset = write_cursor;
        write_cursor += (num_rows + 1u) * sizeof(uint32_t);

        const IColumn & nested = arr_col->getData();
        uint32_t M = static_cast<uint32_t>(nested.size());

        desc.data_offset = write_cursor;

        if (const auto * nested_str = typeid_cast<const ColumnString *>(&nested))
        {
            uint32_t inner_chars = static_cast<uint32_t>(nested_str->getChars().size()) + M;
            desc.data_size = (M + 1u) * sizeof(uint32_t) + inner_chars;
        }
        else
        {
            uint32_t elem_size = static_cast<uint32_t>(nested.sizeOfValueIfFixed());
            desc.data_size = M * elem_size;
        }

        write_cursor += desc.data_size;
        return write_cursor;
    }

    const ColumnString * str_col = typeid_cast<const ColumnString *>(col);
    const ColumnNullable * null_col = typeid_cast<const ColumnNullable *>(col);

    if (null_col)
        str_col = typeid_cast<const ColumnString *>(&null_col->getNestedColumn());

    if (str_col)
    {
        uint32_t base_type = is_nullable ? COL_NULL_BYTES : COL_BYTES;
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
    uint32_t base_type;
    if      (elem_size == 1) base_type = is_nullable ? COL_NULL_FIXED8  : COL_FIXED8;
    else if (elem_size == 4) base_type = is_nullable ? COL_NULL_FIXED32 : COL_FIXED32;
    else                     base_type = is_nullable ? COL_NULL_FIXED64 : COL_FIXED64;
    desc.type         = base_type | (is_const ? COL_IS_CONST : 0u);
    desc.null_offset  = 0;
    desc.offsets_offset = 0;
    desc.data_offset  = write_cursor;
    desc.data_size    = num_rows * elem_size;
    write_cursor     += num_rows * elem_size;
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
    // ── Array column → COL_COMPLEX ────────────────────────────────────────────
    if (const auto * arr_col = typeid_cast<const ColumnArray *>(col))
    {
        const auto & ch_offsets = arr_col->getOffsets();
        const IColumn & nested  = arr_col->getData();
        uint32_t M = static_cast<uint32_t>(nested.size());

        uint32_t * wire_outer = reinterpret_cast<uint32_t *>(buf.data() + desc.offsets_offset);
        wire_outer[0] = 0u;
        for (uint32_t i = 0; i < num_rows; ++i)
            wire_outer[i + 1u] = static_cast<uint32_t>(ch_offsets[i]);

        if (const auto * nested_str = typeid_cast<const ColumnString *>(&nested))
        {
            const auto & ch_str_offs = nested_str->getOffsets();
            const auto & chars       = nested_str->getChars();

            uint32_t * inner_wire = reinterpret_cast<uint32_t *>(buf.data() + desc.data_offset);
            uint8_t  * chars_dst  = buf.data() + desc.data_offset + (M + 1u) * sizeof(uint32_t);

            inner_wire[0] = 0u;
            uint32_t wire_pos = 0u;
            uint32_t ch_pos   = 0u;
            for (uint32_t j = 0; j < M; ++j)
            {
                uint32_t str_end = static_cast<uint32_t>(ch_str_offs[j]);
                uint32_t str_len = str_end - ch_pos;
                std::memcpy(chars_dst + wire_pos, chars.data() + ch_pos, str_len);
                wire_pos += str_len;
                chars_dst[wire_pos++] = '\0';
                inner_wire[j + 1u] = wire_pos;
                ch_pos = str_end;
            }
        }
        else
        {
            std::memcpy(buf.data() + desc.data_offset, nested.getRawData().data(), desc.data_size);
        }
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
        uint32_t * wire_offsets = reinterpret_cast<uint32_t *>(buf.data() + desc.offsets_offset);
        uint8_t * data_dst = buf.data() + desc.data_offset;
        wire_offsets[0] = 0;
        uint32_t wire_pos = 0;
        uint32_t ch_pos = 0;
        for (uint32_t i = 0; i < num_rows; ++i)
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

    const auto * raw = col->getRawData().data();
    std::memcpy(buf.data() + desc.data_offset, raw, desc.data_size);
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

    uint32_t raw_type = desc.type & ~COL_IS_CONST;

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
