#pragma once
// Moved Decimal-related functions out from Core/Types.h to reduce compilation time.

#include <Core/Types.h>
#include <Common/intExp.h>

#include <limits>

namespace DB
{

template <typename T> T decimalScaleMultiplier(UInt32 scale);
template <> inline Int32 decimalScaleMultiplier<Int32>(UInt32 scale) { return common::exp10_i32(scale); }
template <> inline Int64 decimalScaleMultiplier<Int64>(UInt32 scale) { return common::exp10_i64(scale); }
template <> inline Int128 decimalScaleMultiplier<Int128>(UInt32 scale) { return common::exp10_i128(scale); }

template <typename T>
struct DecimalComponents
{
    T whole;
    T fractional;
};

template <typename DecimalType>
DecimalType decimalFromComponents(const typename DecimalType::NativeType & whole, const typename DecimalType::NativeType & fractional, UInt32 scale)
{
    using T = typename DecimalType::NativeType;

    const auto mul = decimalScaleMultiplier<T>(scale);
    const T value = whole * mul + fractional / decimalScaleMultiplier<T>(std::numeric_limits<T>::digits10 - scale);
    return DecimalType(value);
}

template <typename DecimalType>
DecimalType decimalFromComponents(const DecimalComponents<typename DecimalType::NativeType> & components, UInt32 scale)
{
    return decimalFromComponents<DecimalType>(components.whole, components.fractional, scale);
}

template <typename DecimalType>
DecimalComponents<typename DecimalType::NativeType> decimalSplit(const DecimalType & decimal, UInt32 scale)
{
    if (scale == 0)
    {
        return {decimal.value, 0};
    }
    const auto scaleMultiplier = decimalScaleMultiplier<typename DecimalType::NativeType>(scale);
    return {decimal.value / scaleMultiplier, decimal.value % scaleMultiplier};
}

template <typename DecimalType>
typename DecimalType::NativeType decimalWholePart(const DecimalType & decimal, size_t scale)
{
    if (scale == 0)
        return decimal.value;

    return decimal.value / decimalScaleMultiplier<typename DecimalType::NativeType>(scale);
}

template <typename DecimalType>
typename DecimalType::NativeType decimalFractionalPart(const DecimalType & decimal, size_t scale)
{
    using T = typename DecimalType::NativeType;

    if (scale == 0)
        return 0;

    T result = decimal.value;
    if (result < T(0))
        result *= T(-1);

    return result % decimalScaleMultiplier<T>(scale);
}

}
