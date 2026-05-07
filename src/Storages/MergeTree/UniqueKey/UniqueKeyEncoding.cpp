#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Core/TypeId.h>
#include <DataTypes/IDataType.h>

#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

void appendUIntBE8(UInt8 v, String & out)
{
    out.push_back(static_cast<char>(v));
}

void appendUIntBE16(UInt16 v, String & out)
{
    UInt16 be = __builtin_bswap16(v);
    out.append(reinterpret_cast<const char *>(&be), sizeof(be));
}

void appendUIntBE32(UInt32 v, String & out)
{
    UInt32 be = __builtin_bswap32(v);
    out.append(reinterpret_cast<const char *>(&be), sizeof(be));
}

void appendUIntBE64(UInt64 v, String & out)
{
    UInt64 be = __builtin_bswap64(v);
    out.append(reinterpret_cast<const char *>(&be), sizeof(be));
}

/// Signed: flip the sign bit, then big-endian.
void appendIntBE8(Int8 v, String & out)
{
    appendUIntBE8(static_cast<UInt8>(static_cast<UInt8>(v) ^ static_cast<UInt8>(0x80)), out);
}

void appendIntBE16(Int16 v, String & out)
{
    appendUIntBE16(static_cast<UInt16>(static_cast<UInt16>(v) ^ static_cast<UInt16>(0x8000)), out);
}

void appendIntBE32(Int32 v, String & out)
{
    appendUIntBE32(static_cast<UInt32>(v) ^ static_cast<UInt32>(0x80000000U), out);
}

void appendIntBE64(Int64 v, String & out)
{
    appendUIntBE64(static_cast<UInt64>(v) ^ static_cast<UInt64>(0x8000000000000000ULL), out);
}

/// IEEE-754 total-order: invert all bits when sign bit is set, else flip
/// the sign bit. Canonicalize -0.0 → +0.0; canonicalize every NaN to the
/// all-`0xFF` sentinel (sorts strictly after ±Inf, matches
/// `IColumn::compareAt(.., nan_direction_hint=1)`).
void appendFloat32(Float32 v, String & out)
{
    UInt32 bits = std::bit_cast<UInt32>(v);
    if (std::isnan(v))
    {
        appendUIntBE32(0xFFFFFFFFU, out);
        return;
    }
    if (bits == 0x80000000U)
        bits = 0;
    if (bits & 0x80000000U)
        bits = ~bits;
    else
        bits ^= 0x80000000U;
    appendUIntBE32(bits, out);
}

void appendFloat64(Float64 v, String & out)
{
    UInt64 bits = std::bit_cast<UInt64>(v);
    if (std::isnan(v))
    {
        appendUIntBE64(0xFFFFFFFFFFFFFFFFULL, out);
        return;
    }
    if (bits == 0x8000000000000000ULL)
        bits = 0;
    if (bits & 0x8000000000000000ULL)
        bits = ~bits;
    else
        bits ^= 0x8000000000000000ULL;
    appendUIntBE64(bits, out);
}

/// Wide integer: emit limbs most-significant-first via `_impl::big(i)`,
/// each big-endian. Signed variants flip the top bit of the MSB limb.
template <typename T>
void appendWideUIntBE(const T & v, String & out)
{
    using Impl = typename T::_impl;
    for (unsigned i = 0; i < T::_impl::item_count; ++i)
        appendUIntBE64(v.items[Impl::big(i)], out);
}

void appendUInt128BE(const UInt128 & v, String & out)
{
    appendWideUIntBE(v, out);
}

void appendInt128BE(const Int128 & v, String & out)
{
    UInt128 u = static_cast<UInt128>(v);
    u.items[UInt128::_impl::big(0)] ^= 0x8000000000000000ULL;
    appendWideUIntBE(u, out);
}

void appendUInt256BE(const UInt256 & v, String & out)
{
    appendWideUIntBE(v, out);
}

void appendInt256BE(const Int256 & v, String & out)
{
    UInt256 u = static_cast<UInt256>(v);
    u.items[UInt256::_impl::big(0)] ^= 0x8000000000000000ULL;
    appendWideUIntBE(u, out);
}

/// String escape: `'\0'` → `'\0\x01'`; terminate with `'\0\x00'`.
/// Prefix-free: every encoded value ends in `'\0\x00'`, embedded `'\0'`
/// becomes `'\0\x01'`. Fast path uses `memchr` to find the first
/// embedded NUL; slow path walks segment-by-segment.
void appendEscapedString(const char * data, size_t size, String & out)
{
    out.reserve(out.size() + size + 2);

    const char * const end = data + size;
    const char * p = static_cast<const char *>(std::memchr(data, '\0', size));

    if (p == nullptr)
    {
        out.append(data, size);
    }
    else
    {
        const char * cursor = data;
        do
        {
            out.append(cursor, p - cursor);
            out.append("\0\x01", 2);
            cursor = p + 1;
            p = static_cast<const char *>(std::memchr(cursor, '\0', end - cursor));
        }
        while (p != nullptr);
        out.append(cursor, end - cursor);
    }

    out.append("\0\x00", 2);
}

/// Per-row dispatch on a non-Nullable column. Single jump-table on
/// `IColumn::getDataType()` — alias rows (Date↔UInt16, Date32↔Int32,
/// DateTime↔UInt32, UUID↔UInt128) share branches via `static_cast` to
/// the underlying column type. Enum8/Enum16 columns report as Int8/Int16
/// (no dedicated ColumnEnum), so they hit the signed-int branches.
/// Throws NOT_IMPLEMENTED for any TypeIndex outside this set.
void encodeOneNonNullable(const IColumn & column, size_t row, String & out)
{
    switch (column.getDataType())
    {
        case TypeIndex::UInt8:
            appendUIntBE8(static_cast<const ColumnUInt8 &>(column).getData()[row], out);
            return;
        case TypeIndex::UInt16:
        case TypeIndex::Date:
            appendUIntBE16(static_cast<const ColumnUInt16 &>(column).getData()[row], out);
            return;
        case TypeIndex::UInt32:
        case TypeIndex::DateTime:
            appendUIntBE32(static_cast<const ColumnUInt32 &>(column).getData()[row], out);
            return;
        case TypeIndex::UInt64:
            appendUIntBE64(static_cast<const ColumnUInt64 &>(column).getData()[row], out);
            return;
        case TypeIndex::Int8:
            appendIntBE8(static_cast<const ColumnInt8 &>(column).getData()[row], out);
            return;
        case TypeIndex::Int16:
            appendIntBE16(static_cast<const ColumnInt16 &>(column).getData()[row], out);
            return;
        case TypeIndex::Int32:
        case TypeIndex::Date32:
            appendIntBE32(static_cast<const ColumnInt32 &>(column).getData()[row], out);
            return;
        case TypeIndex::Int64:
            appendIntBE64(static_cast<const ColumnInt64 &>(column).getData()[row], out);
            return;
        case TypeIndex::Float32:
            appendFloat32(static_cast<const ColumnFloat32 &>(column).getData()[row], out);
            return;
        case TypeIndex::Float64:
            appendFloat64(static_cast<const ColumnFloat64 &>(column).getData()[row], out);
            return;
        case TypeIndex::String:
        {
            const std::string_view view = static_cast<const ColumnString &>(column).getDataAt(row);
            appendEscapedString(view.data(), view.size(), out);
            return;
        }
        case TypeIndex::FixedString:
        {
            /// FixedString is naturally prefix-free; raw bytes preserve order.
            const std::string_view view = static_cast<const ColumnFixedString &>(column).getDataAt(row);
            out.append(view.data(), view.size());
            return;
        }
        case TypeIndex::UUID:
            appendUInt128BE(static_cast<const ColumnUUID &>(column).getData()[row].toUnderType(), out);
            return;
        case TypeIndex::UInt128:
            appendUInt128BE(static_cast<const ColumnUInt128 &>(column).getData()[row], out);
            return;
        case TypeIndex::Int128:
            appendInt128BE(static_cast<const ColumnInt128 &>(column).getData()[row], out);
            return;
        case TypeIndex::UInt256:
            appendUInt256BE(static_cast<const ColumnUInt256 &>(column).getData()[row], out);
            return;
        case TypeIndex::Int256:
            appendInt256BE(static_cast<const ColumnInt256 &>(column).getData()[row], out);
            return;
        case TypeIndex::Decimal32:
            appendIntBE32(static_cast<const ColumnDecimal<Decimal32> &>(column).getData()[row].value, out);
            return;
        case TypeIndex::Decimal64:
            appendIntBE64(static_cast<const ColumnDecimal<Decimal64> &>(column).getData()[row].value, out);
            return;
        case TypeIndex::Decimal128:
            appendInt128BE(static_cast<const ColumnDecimal<Decimal128> &>(column).getData()[row].value, out);
            return;
        case TypeIndex::Decimal256:
            appendInt256BE(static_cast<const ColumnDecimal<Decimal256> &>(column).getData()[row].value, out);
            return;
        case TypeIndex::DateTime64:
            appendIntBE64(static_cast<const ColumnDecimal<DateTime64> &>(column).getData()[row].value, out);
            return;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "UNIQUE KEY encoding: column type {} is not supported",
                            column.getName());
    }
}

}

namespace
{

/// Column-wise free functions: one `typeid_cast` per column, tight row
/// loop per type. `null_map` is non-null when called from a Nullable
/// wrapper and skips null rows. `permutation`, if non-null, indexes the
/// row reads via `(*permutation)[r]`.

template <typename Col, typename Append>
void appendVectorColumn(
    const Col & c,
    const UInt8 * null_map,
    const IColumn::Permutation * permutation,
    size_t num_rows,
    std::vector<String> & out,
    Append && row_appender)
{
    const auto & data = c.getData();
    if (permutation)
    {
        if (null_map)
        {
            for (size_t r = 0; r < num_rows; ++r)
            {
                const size_t src = (*permutation)[r];
                if (!null_map[src])
                    row_appender(data[src], out[r]);
            }
        }
        else
        {
            for (size_t r = 0; r < num_rows; ++r)
                row_appender(data[(*permutation)[r]], out[r]);
        }
    }
    else if (null_map)
    {
        for (size_t r = 0; r < num_rows; ++r)
            if (!null_map[r])
                row_appender(data[r], out[r]);
    }
    else
    {
        for (size_t r = 0; r < num_rows; ++r)
            row_appender(data[r], out[r]);
    }
}

void appendStringColumn(
    const ColumnString & c,
    const UInt8 * null_map,
    const IColumn::Permutation * permutation,
    size_t num_rows,
    std::vector<String> & out)
{
    auto run = [&](size_t r, size_t src)
    {
        const std::string_view view = c.getDataAt(src);
        appendEscapedString(view.data(), view.size(), out[r]);
    };
    for (size_t r = 0; r < num_rows; ++r)
    {
        const size_t src = permutation ? (*permutation)[r] : r;
        if (null_map && null_map[src])
            continue;
        run(r, src);
    }
}

void appendFixedStringColumn(
    const ColumnFixedString & c,
    const UInt8 * null_map,
    const IColumn::Permutation * permutation,
    size_t num_rows,
    std::vector<String> & out)
{
    const size_t n = c.getN();
    for (size_t r = 0; r < num_rows; ++r)
    {
        const size_t src = permutation ? (*permutation)[r] : r;
        if (null_map && null_map[src])
            continue;
        const std::string_view view = c.getDataAt(src);
        out[r].append(view.substr(0, n));
    }
}

/// Returns false if the TypeIndex is not handled; caller falls back to
/// row-wise dispatch for that column. Single jump-table on
/// `IColumn::getDataType()` mirroring the `encodeOneNonNullable` shape.
bool dispatchNonNullableColumnWise(
    const IColumn & column,
    const UInt8 * null_map,
    const IColumn::Permutation * permutation,
    size_t num_rows,
    std::vector<String> & out)
{
    switch (column.getDataType())
    {
        case TypeIndex::UInt8:
            appendVectorColumn(static_cast<const ColumnUInt8 &>(column), null_map, permutation, num_rows, out,
                [](UInt8 v, String & dst) { appendUIntBE8(v, dst); });
            return true;
        case TypeIndex::UInt16:
        case TypeIndex::Date:
            appendVectorColumn(static_cast<const ColumnUInt16 &>(column), null_map, permutation, num_rows, out,
                [](UInt16 v, String & dst) { appendUIntBE16(v, dst); });
            return true;
        case TypeIndex::UInt32:
        case TypeIndex::DateTime:
            appendVectorColumn(static_cast<const ColumnUInt32 &>(column), null_map, permutation, num_rows, out,
                [](UInt32 v, String & dst) { appendUIntBE32(v, dst); });
            return true;
        case TypeIndex::UInt64:
            appendVectorColumn(static_cast<const ColumnUInt64 &>(column), null_map, permutation, num_rows, out,
                [](UInt64 v, String & dst) { appendUIntBE64(v, dst); });
            return true;
        case TypeIndex::Int8:
            appendVectorColumn(static_cast<const ColumnInt8 &>(column), null_map, permutation, num_rows, out,
                [](Int8 v, String & dst) { appendIntBE8(v, dst); });
            return true;
        case TypeIndex::Int16:
            appendVectorColumn(static_cast<const ColumnInt16 &>(column), null_map, permutation, num_rows, out,
                [](Int16 v, String & dst) { appendIntBE16(v, dst); });
            return true;
        case TypeIndex::Int32:
        case TypeIndex::Date32:
            appendVectorColumn(static_cast<const ColumnInt32 &>(column), null_map, permutation, num_rows, out,
                [](Int32 v, String & dst) { appendIntBE32(v, dst); });
            return true;
        case TypeIndex::Int64:
            appendVectorColumn(static_cast<const ColumnInt64 &>(column), null_map, permutation, num_rows, out,
                [](Int64 v, String & dst) { appendIntBE64(v, dst); });
            return true;
        case TypeIndex::Float32:
            appendVectorColumn(static_cast<const ColumnFloat32 &>(column), null_map, permutation, num_rows, out,
                [](Float32 v, String & dst) { appendFloat32(v, dst); });
            return true;
        case TypeIndex::Float64:
            appendVectorColumn(static_cast<const ColumnFloat64 &>(column), null_map, permutation, num_rows, out,
                [](Float64 v, String & dst) { appendFloat64(v, dst); });
            return true;
        case TypeIndex::String:
            appendStringColumn(static_cast<const ColumnString &>(column), null_map, permutation, num_rows, out);
            return true;
        case TypeIndex::FixedString:
            appendFixedStringColumn(static_cast<const ColumnFixedString &>(column), null_map, permutation, num_rows, out);
            return true;
        case TypeIndex::UUID:
            appendVectorColumn(static_cast<const ColumnUUID &>(column), null_map, permutation, num_rows, out,
                [](const UUID & v, String & dst) { appendUInt128BE(v.toUnderType(), dst); });
            return true;
        case TypeIndex::UInt128:
            appendVectorColumn(static_cast<const ColumnUInt128 &>(column), null_map, permutation, num_rows, out,
                [](const UInt128 & v, String & dst) { appendUInt128BE(v, dst); });
            return true;
        case TypeIndex::Int128:
            appendVectorColumn(static_cast<const ColumnInt128 &>(column), null_map, permutation, num_rows, out,
                [](const Int128 & v, String & dst) { appendInt128BE(v, dst); });
            return true;
        case TypeIndex::UInt256:
            appendVectorColumn(static_cast<const ColumnUInt256 &>(column), null_map, permutation, num_rows, out,
                [](const UInt256 & v, String & dst) { appendUInt256BE(v, dst); });
            return true;
        case TypeIndex::Int256:
            appendVectorColumn(static_cast<const ColumnInt256 &>(column), null_map, permutation, num_rows, out,
                [](const Int256 & v, String & dst) { appendInt256BE(v, dst); });
            return true;
        case TypeIndex::Decimal32:
            appendVectorColumn(static_cast<const ColumnDecimal<Decimal32> &>(column), null_map, permutation, num_rows, out,
                [](const Decimal32 & v, String & dst) { appendIntBE32(v.value, dst); });
            return true;
        case TypeIndex::Decimal64:
            appendVectorColumn(static_cast<const ColumnDecimal<Decimal64> &>(column), null_map, permutation, num_rows, out,
                [](const Decimal64 & v, String & dst) { appendIntBE64(v.value, dst); });
            return true;
        case TypeIndex::Decimal128:
            appendVectorColumn(static_cast<const ColumnDecimal<Decimal128> &>(column), null_map, permutation, num_rows, out,
                [](const Decimal128 & v, String & dst) { appendInt128BE(v.value, dst); });
            return true;
        case TypeIndex::Decimal256:
            appendVectorColumn(static_cast<const ColumnDecimal<Decimal256> &>(column), null_map, permutation, num_rows, out,
                [](const Decimal256 & v, String & dst) { appendInt256BE(v.value, dst); });
            return true;
        case TypeIndex::DateTime64:
            appendVectorColumn(static_cast<const ColumnDecimal<DateTime64> &>(column), null_map, permutation, num_rows, out,
                [](const DateTime64 & v, String & dst) { appendIntBE64(v.value, dst); });
            return true;
        default:
            return false;
    }
}

}

namespace UniqueKeyEncoding
{

void encodeBlock(
    const Columns & columns,
    const IColumn::Permutation * permutation,
    size_t max_size,
    std::vector<String> & out)
{
    out.clear();
    if (columns.empty())
        return;

    const size_t num_rows = columns.front()->size();
    if (permutation && permutation->size() != num_rows)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "UNIQUE KEY encoding: permutation size {} != block rows {}",
                        permutation->size(), num_rows);

    /// Permutation entries are used as direct row indices below
    /// (`null_map[src]`, `data[src]`, `getDataAt(src)`); a malformed entry would
    /// be an out-of-bounds access. Validate as a true permutation of [0, n):
    /// every value in range and no duplicates.
    if (permutation && num_rows > 0)
    {
        std::vector<bool> seen(num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            const size_t v = (*permutation)[i];
            if (v >= num_rows)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "UNIQUE KEY encoding: permutation[{}]={} out of range (num_rows={})",
                                i, v, num_rows);
            if (seen[v])
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "UNIQUE KEY encoding: permutation has duplicate value {}", v);
            seen[v] = true;
        }
    }

    out.resize(num_rows);
    if (num_rows == 0)
        return;

    for (const auto & col_ptr : columns)
    {
        const IColumn * col = col_ptr.get();

        const UInt8 * null_map = nullptr;
        const IColumn * inner = col;
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(col))
        {
            null_map = nullable->getNullMapData().data();
            inner = &nullable->getNestedColumn();
            for (size_t r = 0; r < num_rows; ++r)
            {
                const size_t src = permutation ? (*permutation)[r] : r;
                /// Null flag: NULL=0x01 (after), non-NULL=0x00 (before) —
                /// matches `compareAt` with `nulls_direction=1` (the direction
                /// the writer's `stableGetPermutation` and Float NaN sentinel
                /// both use).
                out[r].push_back(null_map[src] ? '\x01' : '\x00');
            }
        }

        if (!dispatchNonNullableColumnWise(*inner, null_map, permutation, num_rows, out))
        {
            for (size_t r = 0; r < num_rows; ++r)
            {
                const size_t src = permutation ? (*permutation)[r] : r;
                if (null_map && null_map[src])
                    continue;
                encodeOneNonNullable(*inner, src, out[r]);
            }
        }

        for (size_t r = 0; r < num_rows; ++r)
        {
            if (out[r].size() > max_size)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "UNIQUE KEY encoded size exceeds unique_key_max_encoded_size={} bytes",
                                max_size);
        }
    }
}

}

}
