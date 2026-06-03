#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/transformEndianness.h>
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
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Order-preserving big-endian byte append. Reuses ClickHouse's
/// `transformEndianness` so integer / wide-integer / float byte-swap shares
/// the same primitive as the rest of the IO stack.
template <typename T>
void appendBigEndian(T v, String & out)
{
    transformEndianness<std::endian::big>(v);
    out.append(reinterpret_cast<const char *>(&v), sizeof(v));
}

/// Signed: flip the sign bit on the MSB byte, then big-endian.
template <typename Signed>
void appendSignedBigEndian(Signed v, String & out)
{
    using Unsigned = std::make_unsigned_t<Signed>;
    static constexpr Unsigned sign_bit = Unsigned{1} << (sizeof(Unsigned) * 8 - 1);
    appendBigEndian(static_cast<Unsigned>(static_cast<Unsigned>(v) ^ sign_bit), out);
}

/// Wide signed (Int128 / Int256): flip the top bit of the most-significant
/// limb in the unsigned representation, then big-endian. `make_unsigned_t`
/// returns the wrong type for `wide::integer<N, int>`, so the unsigned
/// counterpart is passed in explicitly.
void appendWideSignedBigEndian(const Int128 & v, String & out)
{
    UInt128 u = static_cast<UInt128>(v);
    u.items[UInt128::_impl::big(0)] ^= 0x8000000000000000ULL;
    appendBigEndian(u, out);
}

void appendWideSignedBigEndian(const Int256 & v, String & out)
{
    UInt256 u = static_cast<UInt256>(v);
    u.items[UInt256::_impl::big(0)] ^= 0x8000000000000000ULL;
    appendBigEndian(u, out);
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
        appendBigEndian<UInt32>(0xFFFFFFFFU, out);
        return;
    }
    if (bits == 0x80000000U)
        bits = 0;
    if (bits & 0x80000000U)
        bits = ~bits;
    else
        bits ^= 0x80000000U;
    appendBigEndian(bits, out);
}

void appendFloat64(Float64 v, String & out)
{
    UInt64 bits = std::bit_cast<UInt64>(v);
    if (std::isnan(v))
    {
        appendBigEndian<UInt64>(0xFFFFFFFFFFFFFFFFULL, out);
        return;
    }
    if (bits == 0x8000000000000000ULL)
        bits = 0;
    if (bits & 0x8000000000000000ULL)
        bits = ~bits;
    else
        bits ^= 0x8000000000000000ULL;
    appendBigEndian(bits, out);
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
            appendBigEndian(static_cast<const ColumnUInt8 &>(column).getData()[row], out);
            return;
        case TypeIndex::UInt16:
        case TypeIndex::Date:
            appendBigEndian(static_cast<const ColumnUInt16 &>(column).getData()[row], out);
            return;
        case TypeIndex::UInt32:
        case TypeIndex::DateTime:
            appendBigEndian(static_cast<const ColumnUInt32 &>(column).getData()[row], out);
            return;
        case TypeIndex::UInt64:
            appendBigEndian(static_cast<const ColumnUInt64 &>(column).getData()[row], out);
            return;
        case TypeIndex::Int8:
            appendSignedBigEndian(static_cast<const ColumnInt8 &>(column).getData()[row], out);
            return;
        case TypeIndex::Int16:
            appendSignedBigEndian(static_cast<const ColumnInt16 &>(column).getData()[row], out);
            return;
        case TypeIndex::Int32:
        case TypeIndex::Date32:
            appendSignedBigEndian(static_cast<const ColumnInt32 &>(column).getData()[row], out);
            return;
        case TypeIndex::Int64:
            appendSignedBigEndian(static_cast<const ColumnInt64 &>(column).getData()[row], out);
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
            appendBigEndian(static_cast<const ColumnUUID &>(column).getData()[row].toUnderType(), out);
            return;
        case TypeIndex::UInt128:
            appendBigEndian(static_cast<const ColumnUInt128 &>(column).getData()[row], out);
            return;
        case TypeIndex::Int128:
            appendWideSignedBigEndian(static_cast<const ColumnInt128 &>(column).getData()[row], out);
            return;
        case TypeIndex::UInt256:
            appendBigEndian(static_cast<const ColumnUInt256 &>(column).getData()[row], out);
            return;
        case TypeIndex::Int256:
            appendWideSignedBigEndian(static_cast<const ColumnInt256 &>(column).getData()[row], out);
            return;
        case TypeIndex::Decimal32:
            appendSignedBigEndian(static_cast<const ColumnDecimal<Decimal32> &>(column).getData()[row].value, out);
            return;
        case TypeIndex::Decimal64:
            appendSignedBigEndian(static_cast<const ColumnDecimal<Decimal64> &>(column).getData()[row].value, out);
            return;
        case TypeIndex::Decimal128:
            appendWideSignedBigEndian(static_cast<const ColumnDecimal<Decimal128> &>(column).getData()[row].value, out);
            return;
        case TypeIndex::Decimal256:
            appendWideSignedBigEndian(static_cast<const ColumnDecimal<Decimal256> &>(column).getData()[row].value, out);
            return;
        case TypeIndex::DateTime64:
            appendSignedBigEndian(static_cast<const ColumnDecimal<DateTime64> &>(column).getData()[row].value, out);
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
    /// `getDataType()` reports the nested type for these wrappers, but
    /// the dynamic type is still the wrapper — `static_cast` below would
    /// be UB. They are not part of the UK part-build contract.
    if (column.isSparse() || column.isReplicated())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "UNIQUE KEY encoding: ColumnSparse/ColumnReplicated unsupported");

    switch (column.getDataType())
    {
        case TypeIndex::UInt8:
            appendVectorColumn(static_cast<const ColumnUInt8 &>(column), null_map, permutation, num_rows, out,
                [](UInt8 v, String & dst) { appendBigEndian(v, dst); });
            return true;
        case TypeIndex::UInt16:
        case TypeIndex::Date:
            appendVectorColumn(static_cast<const ColumnUInt16 &>(column), null_map, permutation, num_rows, out,
                [](UInt16 v, String & dst) { appendBigEndian(v, dst); });
            return true;
        case TypeIndex::UInt32:
        case TypeIndex::DateTime:
            appendVectorColumn(static_cast<const ColumnUInt32 &>(column), null_map, permutation, num_rows, out,
                [](UInt32 v, String & dst) { appendBigEndian(v, dst); });
            return true;
        case TypeIndex::UInt64:
            appendVectorColumn(static_cast<const ColumnUInt64 &>(column), null_map, permutation, num_rows, out,
                [](UInt64 v, String & dst) { appendBigEndian(v, dst); });
            return true;
        case TypeIndex::Int8:
            appendVectorColumn(static_cast<const ColumnInt8 &>(column), null_map, permutation, num_rows, out,
                [](Int8 v, String & dst) { appendSignedBigEndian(v, dst); });
            return true;
        case TypeIndex::Int16:
            appendVectorColumn(static_cast<const ColumnInt16 &>(column), null_map, permutation, num_rows, out,
                [](Int16 v, String & dst) { appendSignedBigEndian(v, dst); });
            return true;
        case TypeIndex::Int32:
        case TypeIndex::Date32:
            appendVectorColumn(static_cast<const ColumnInt32 &>(column), null_map, permutation, num_rows, out,
                [](Int32 v, String & dst) { appendSignedBigEndian(v, dst); });
            return true;
        case TypeIndex::Int64:
            appendVectorColumn(static_cast<const ColumnInt64 &>(column), null_map, permutation, num_rows, out,
                [](Int64 v, String & dst) { appendSignedBigEndian(v, dst); });
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
                [](const UUID & v, String & dst) { appendBigEndian(v.toUnderType(), dst); });
            return true;
        case TypeIndex::UInt128:
            appendVectorColumn(static_cast<const ColumnUInt128 &>(column), null_map, permutation, num_rows, out,
                [](const UInt128 & v, String & dst) { appendBigEndian(v, dst); });
            return true;
        case TypeIndex::Int128:
            appendVectorColumn(static_cast<const ColumnInt128 &>(column), null_map, permutation, num_rows, out,
                [](const Int128 & v, String & dst) { appendWideSignedBigEndian(v, dst); });
            return true;
        case TypeIndex::UInt256:
            appendVectorColumn(static_cast<const ColumnUInt256 &>(column), null_map, permutation, num_rows, out,
                [](const UInt256 & v, String & dst) { appendBigEndian(v, dst); });
            return true;
        case TypeIndex::Int256:
            appendVectorColumn(static_cast<const ColumnInt256 &>(column), null_map, permutation, num_rows, out,
                [](const Int256 & v, String & dst) { appendWideSignedBigEndian(v, dst); });
            return true;
        case TypeIndex::Decimal32:
            appendVectorColumn(static_cast<const ColumnDecimal<Decimal32> &>(column), null_map, permutation, num_rows, out,
                [](const Decimal32 & v, String & dst) { appendSignedBigEndian(v.value, dst); });
            return true;
        case TypeIndex::Decimal64:
            appendVectorColumn(static_cast<const ColumnDecimal<Decimal64> &>(column), null_map, permutation, num_rows, out,
                [](const Decimal64 & v, String & dst) { appendSignedBigEndian(v.value, dst); });
            return true;
        case TypeIndex::Decimal128:
            appendVectorColumn(static_cast<const ColumnDecimal<Decimal128> &>(column), null_map, permutation, num_rows, out,
                [](const Decimal128 & v, String & dst) { appendWideSignedBigEndian(v.value, dst); });
            return true;
        case TypeIndex::Decimal256:
            appendVectorColumn(static_cast<const ColumnDecimal<Decimal256> &>(column), null_map, permutation, num_rows, out,
                [](const Decimal256 & v, String & dst) { appendWideSignedBigEndian(v.value, dst); });
            return true;
        case TypeIndex::DateTime64:
            appendVectorColumn(static_cast<const ColumnDecimal<DateTime64> &>(column), null_map, permutation, num_rows, out,
                [](const DateTime64 & v, String & dst) { appendSignedBigEndian(v.value, dst); });
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
    /// Block invariant: equal-length columns. Permutation is trusted.
    for (size_t c = 1; c < columns.size(); ++c)
    {
        const size_t sz = columns[c]->size();
        if (sz != num_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "UNIQUE KEY encoding: column[{}] size {} != column[0] size {}",
                            c, sz, num_rows);
    }

    out.resize(num_rows);
    if (num_rows == 0)
        return;

    /// `ColumnConst` can legitimately reach the encoder (expression-
    /// evaluated UK columns); materialize it so the dispatch's
    /// `static_cast` lands on the concrete column type.
    Columns materialized;
    materialized.reserve(columns.size());
    for (const auto & col : columns)
        materialized.push_back(col->convertToFullColumnIfConst());

    for (const auto & col_ptr : materialized)
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
