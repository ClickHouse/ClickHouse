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

template <typename T>
Decimal<T> decimalFromComponents(const T & whole, const T & fractional, UInt32 scale)
{
    const auto mul = decimalScaleMultiplier<T>(scale);
    const T value = whole * mul + fractional / decimalScaleMultiplier<T>(std::numeric_limits<T>::digits10 - scale);
    return Decimal<T>(value);
}

template <typename T>
Decimal<T> decimalFromComponents(const DecimalComponents<T> & components, UInt32 scale)
{
    return decimalFromComponents(components.whole, components.fractional, scale);
}

template <typename T>
DecimalComponents<T> decimalSplit(const Decimal<T> & decimal, UInt32 scale)
{
    if (scale == 0)
    {
        return {decimal.value, 0};
    }
    const auto scaleMultiplier = decimalScaleMultiplier<T>(scale);
    return {decimal.value / scaleMultiplier, decimal.value % scaleMultiplier};
}

template <typename T>
T decimalWholePart(const Decimal<T> & decimal, size_t scale)
{
    if (scale == 0)
        return decimal.value;

    return decimal.value / decimalScaleMultiplier<T>(scale);
}

template <typename T>
T decimalFractionalPart(const Decimal<T> & decimal, size_t scale)
{
    if (scale == 0)
        return 0;

    T result = decimal.value;
    if (result < T(0))
        result *= T(-1);

    return result % decimalScaleMultiplier<T>(scale);
}

}
