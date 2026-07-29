#include <bit>
#include <type_traits>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Processors/Transforms/JoinKeyEncoding.h>
#include <Common/NaNUtils.h>

namespace DB
{

namespace
{

/// Everything whose order is its underlying signed integer's order (signed integers, Date32,
/// DateTime64, Decimal32/64, Enum8/16): offset the sign bit so the unsigned order matches.
UInt64 encodeSignedKey(Int64 value)
{
    return static_cast<UInt64>(value) ^ (UInt64(1) << 63);
}

/// `compareAt` with nan_direction_hint = 1 treats every NaN, of either sign, as equal to other
/// NaNs and greater than all numbers, and -0.0 as equal to +0.0. The plain total-order bit trick
/// preserves neither (a negative NaN would sort below -inf), so every NaN maps to the greatest
/// encoding and -0.0 is canonicalized to +0.0 before the trick. (Joins exclude NaN-keyed rows
/// via their validity masks - such rows can never match - so the NaN mapping is defensive.)
UInt64 encodeFloatKey(Float64 value)
{
    if (isNaN(value))
        return ~UInt64(0);
    UInt64 bits = std::bit_cast<UInt64>(value == 0.0 ? 0.0 : value);
    return bits & (UInt64(1) << 63) ? ~bits : bits | (UInt64(1) << 63);
}

UInt64 encodeFloatKey(Float32 value)
{
    if (isNaN(value))
        return ~UInt64(0);
    UInt32 bits = std::bit_cast<UInt32>(value == 0.0f ? 0.0f : value);
    return bits & (UInt32(1) << 31) ? ~bits : bits | (UInt32(1) << 31);
}

template <typename T>
UInt64 encodeKeyValue(T value)
{
    if constexpr (std::is_floating_point_v<T>)
        return encodeFloatKey(value);
    else if constexpr (is_decimal<T>)
        return encodeSignedKey(value.value);
    else if constexpr (std::is_signed_v<T>)
        return encodeSignedKey(value);
    else
        return static_cast<UInt64>(value);
}

template <typename ColumnType, typename F>
bool tryColumnType(const IColumn * data_column, F && f)
{
    const auto * concrete = checkAndGetColumn<ColumnType>(data_column);
    if (!concrete)
        return false;
    f(*concrete);
    return true;
}

/// The single dispatch over the encodable column types, shared by the probe and the encoder so
/// they can never disagree. Calls `f` on the concrete column and returns true, or returns false
/// when the column has no fixed-width encoding.
template <typename F>
bool dispatchEncodableColumn(const IColumn & column, F && f)
{
    const IColumn * data_column = &column;
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(data_column))
        data_column = &nullable->getNestedColumn();

    return tryColumnType<ColumnUInt8>(data_column, f)
        || tryColumnType<ColumnUInt16>(data_column, f)
        || tryColumnType<ColumnUInt32>(data_column, f)
        || tryColumnType<ColumnUInt64>(data_column, f)
        || tryColumnType<ColumnInt8>(data_column, f)
        || tryColumnType<ColumnInt16>(data_column, f)
        || tryColumnType<ColumnInt32>(data_column, f)
        || tryColumnType<ColumnInt64>(data_column, f)
        || tryColumnType<ColumnFloat32>(data_column, f)
        || tryColumnType<ColumnFloat64>(data_column, f)
        || tryColumnType<ColumnDecimal<Decimal32>>(data_column, f)
        || tryColumnType<ColumnDecimal<Decimal64>>(data_column, f)
        || tryColumnType<ColumnDecimal<DateTime64>>(data_column, f);
}

}

bool isJoinKeyColumnEncodable(const IColumn & column)
{
    return dispatchEncodableColumn(column, [](const auto &) {});
}

bool tryAppendEncodedKeys(const IColumn & column, UInt64 flip_mask, PaddedPODArray<UInt64> & out)
{
    return dispatchEncodableColumn(column, [&](const auto & concrete)
    {
        const auto & data = concrete.getData();
        const size_t old_size = out.size();
        out.resize(old_size + data.size());
        for (size_t i = 0; i < data.size(); ++i)
            out[old_size + i] = encodeKeyValue(data[i]) ^ flip_mask;
    });
}

}
