#pragma once
// Moved Decimal-related functions out from Core/Types.h to reduce compilation time.

#include <Core/Types.h>
#include <Common/intExp.h>

#include <limits>

class DateLUTImpl;

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
DecimalType decimalFromComponentsWithMultipliers(const typename DecimalType::NativeType & whole,
                                                 const typename DecimalType::NativeType & fractional,
                                                 typename DecimalType::NativeType scale_multiplier,
                                                 typename DecimalType::NativeType fractional_divider)
{
    using T = typename DecimalType::NativeType;
    const T value = whole * scale_multiplier + fractional / fractional_divider;
    return DecimalType(value);
}

template <typename DecimalType>
typename DecimalType::NativeType  decimalFractionalDivider(UInt32 scale)
{
    using T = typename DecimalType::NativeType;
    return decimalScaleMultiplier<T>(std::numeric_limits<T>::digits10 - scale);
}

template <typename DecimalType>
DecimalType decimalFromComponents(const typename DecimalType::NativeType & whole, const typename DecimalType::NativeType & fractional, UInt32 scale)
{
    using T = typename DecimalType::NativeType;

    return decimalFromComponentsWithMultipliers<DecimalType>(whole, fractional, decimalScaleMultiplier<T>(scale), decimalFractionalDivider<DecimalType>(scale));
}

template <typename DecimalType>
DecimalType decimalFromComponents(const DecimalComponents<typename DecimalType::NativeType> & components, UInt32 scale)
{
    return decimalFromComponents<DecimalType>(components.whole, components.fractional, scale);
}

template <typename DecimalType>
DecimalComponents<typename DecimalType::NativeType> decimalSplitWithScaleMultiplier(const DecimalType & decimal, typename DecimalType::NativeType scale_multiplier)
{
    using T = typename DecimalType::NativeType;
    const auto whole = decimal.value / scale_multiplier;
    auto fractional = decimal.value % scale_multiplier;
    if (fractional < T(0))
        fractional *= T(-1);

    return {whole, fractional};
}

template <typename DecimalType>
DecimalComponents<typename DecimalType::NativeType> decimalSplit(const DecimalType & decimal, UInt32 scale)
{
    if (scale == 0)
    {
        return {decimal.value, 0};
    }
    return decimalSplitWithScaleMultiplier(decimal, decimalScaleMultiplier<typename DecimalType::NativeType>(scale));
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

/** Basic wrapper for Tansform-types for DateTime64.
 *
 * Allows reusing existing Transform (that takes DateTime-values as UInt32) with DateTime64-values,
 * by discarding fractional part and producing SAME return type as original Transform.
 *
 * Such Transfotm-types are commonly used in Date/DateTime manipulation functions,
 * and implement static execute fucntion with following signature:
 *      R execute(UInt32, T, const DateLUTImpl &)
 *
 * Wehere R and T could be arbitrary types.
*/
template <typename Transform>
class DateTime64BasicTransformWrapper : public Transform
{
public:
    using Transform::execute;

    explicit DateTime64BasicTransformWrapper(UInt32 scale_)
        : scale_multiplier(decimalScaleMultiplier<DateTime64::NativeType>(scale_))
    {}

    template <typename T>
    auto execute(DateTime64 t, T v, const DateLUTImpl & time_zone) const
        -> decltype(Transform::execute(UInt32{}, v, time_zone))
    {
        const auto components = decimalSplitWithScaleMultiplier(t, scale_multiplier);
        return static_cast<const Transform *>(this)->execute(
            static_cast<UInt32>(components.whole), v, time_zone);
    }

private:
    UInt32 scale_multiplier = 1;
};

}
