#pragma once
// Moved Decimal-related functions out from Core/Types.h to reduce compilation time.

#include <Core/Types.h>
#include <Common/Exception.h>
#include <common/arithmeticOverflow.h>

#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
}

namespace DecimalUtils
{

static constexpr size_t minPrecision() { return 1; }
template <typename T> static constexpr size_t maxPrecision() { return 0; }
template <> constexpr size_t maxPrecision<Decimal32>() { return 9; }
template <> constexpr size_t maxPrecision<Decimal64>() { return 18; }
template <> constexpr size_t maxPrecision<Decimal128>() { return 38; }
template <> constexpr size_t maxPrecision<Decimal256>() { return 77; }

template <typename T> T scaleMultiplier(UInt32 scale);
template <> inline Int32 scaleMultiplier<Int32>(UInt32 scale) { return common::exp10_i32(scale); }
template <> inline Int64 scaleMultiplier<Int64>(UInt32 scale) { return common::exp10_i64(scale); }
template <> inline Int128 scaleMultiplier<Int128>(UInt32 scale) { return common::exp10_i128(scale); }
template <> inline bInt256 scaleMultiplier<bInt256>(UInt32 scale) { return common::exp10_i256(scale); }

/** Components of DecimalX value:
 * whole - represents whole part of decimal, can be negatve or positive.
 * fractional - for fractional part of decimal, always positive.
 */
template <typename T>
struct DecimalComponents
{
    T whole;
    T fractional;
};

/** Make a decimal value from whole and fractional components with given scale multiplier.
  * where scale_multiplier = scaleMultiplier<T>(scale)
  * this is to reduce number of calls to scaleMultiplier when scale is known.
  *
  * Sign of `whole` controls sign of result: negative whole => negative result, positive whole => positive result.
  * Sign of `fractional` is expected to be positive, otherwise result is undefined.
  * If `scale` is to big (scale > maxPrecision<DecimalType::NativeType>), result is undefined.
  */
template <typename DecimalType>
inline DecimalType decimalFromComponentsWithMultiplier(
        const typename DecimalType::NativeType & whole,
        const typename DecimalType::NativeType & fractional,
        typename DecimalType::NativeType scale_multiplier)
{
    using T = typename DecimalType::NativeType;
    const auto fractional_sign = whole < 0 ? -1 : 1;

    T whole_scaled = 0;
    if (common::mulOverflow(whole, scale_multiplier, whole_scaled))
        throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);

    const T value = whole_scaled + fractional_sign * (fractional % scale_multiplier);
    return DecimalType(value);
}

/** Make a decimal value from whole and fractional components with given scale.
 *
 * @see `decimalFromComponentsWithMultiplier` for details.
 */
template <typename DecimalType>
inline DecimalType decimalFromComponents(
        const typename DecimalType::NativeType & whole,
        const typename DecimalType::NativeType & fractional,
        UInt32 scale)
{
    using T = typename DecimalType::NativeType;

    return decimalFromComponentsWithMultiplier<DecimalType>(whole, fractional, scaleMultiplier<T>(scale));
}

/** Make a decimal value from whole and fractional components with given scale.
 * @see `decimalFromComponentsWithMultiplier` for details.
 */
template <typename DecimalType>
inline DecimalType decimalFromComponents(
        const DecimalComponents<typename DecimalType::NativeType> & components,
        UInt32 scale)
{
    return decimalFromComponents<DecimalType>(components.whole, components.fractional, scale);
}

/** Split decimal into whole and fractional parts with given scale_multiplier.
 * This is an optimization to reduce number of calls to scaleMultiplier on known scale.
 */
template <typename DecimalType>
inline DecimalComponents<typename DecimalType::NativeType> splitWithScaleMultiplier(
        const DecimalType & decimal,
        typename DecimalType::NativeType scale_multiplier)
{
    using T = typename DecimalType::NativeType;
    const auto whole = decimal.value / scale_multiplier;
    auto fractional = decimal.value % scale_multiplier;
    if (fractional < T(0))
        fractional *= T(-1);

    return {whole, fractional};
}

/// Split decimal into components: whole and fractional part, @see `DecimalComponents` for details.
template <typename DecimalType>
inline DecimalComponents<typename DecimalType::NativeType> split(const DecimalType & decimal, UInt32 scale)
{
    if (scale == 0)
    {
        return {decimal.value, 0};
    }
    return splitWithScaleMultiplier(decimal, scaleMultiplier<typename DecimalType::NativeType>(scale));
}

/** Get whole part from decimal.
 *
 * Sign of result follows sign of `decimal` value.
 * If scale is to big, result is undefined.
 */
template <typename DecimalType>
inline typename DecimalType::NativeType getWholePart(const DecimalType & decimal, size_t scale)
{
    if (scale == 0)
        return decimal.value;

    return decimal.value / scaleMultiplier<typename DecimalType::NativeType>(scale);
}


template <typename DecimalType, bool keep_sign = false>
inline typename DecimalType::NativeType getFractionalPartWithScaleMultiplier(
        const DecimalType & decimal,
        typename DecimalType::NativeType scale_multiplier)
{
    using T = typename DecimalType::NativeType;

    T result = decimal.value;
    if constexpr (!keep_sign)
        if (result < T(0))
            result = -result;

    return result % scale_multiplier;
}

/** Get fractional part from decimal
 *
 * Result is always positive.
 * If scale is to big, result is undefined.
 */
template <typename DecimalType>
inline typename DecimalType::NativeType getFractionalPart(const DecimalType & decimal, size_t scale)
{
    if (scale == 0)
        return 0;

    return getFractionalPartWithScaleMultiplier(decimal, scaleMultiplier<typename DecimalType::NativeType>(scale));
}

template <typename To, typename DecimalType>
To convertTo(const DecimalType & decimal, size_t scale)
{
    using NativeType = typename DecimalType::NativeType;

    if constexpr (std::is_floating_point_v<To>)
        return static_cast<To>(decimal.value) / static_cast<To>(scaleMultiplier<NativeType>(scale));
    else if constexpr (std::is_same_v<NativeType, To>)
        return getWholePart(decimal, scale);
    else if constexpr (sizeof(To) > sizeof(NativeType) && is_signed_v<To>)
        return static_cast<To>(getWholePart(decimal, scale));
    else if constexpr (sizeof(To) >= sizeof(NativeType) && is_unsigned_v<To>)
    {
        const NativeType whole = getWholePart(decimal, scale);

        if (whole < 0)
            throw Exception("Convert overflow", ErrorCodes::DECIMAL_OVERFLOW);
        return static_cast<To>(whole);
    }
    else if constexpr (std::is_same_v<To, UInt8>)
    {
        // big integers can not be build from uint8_t
        const NativeType whole = getWholePart(decimal, scale);

        if (whole > 255)
            throw Exception("Convert overflow", ErrorCodes::DECIMAL_OVERFLOW);
        return static_cast<UInt8>(static_cast<UInt32>(whole));
    }
    else if constexpr (IsDecimalNumber<To>)
    {
        using ToNativeType = typename To::NativeType;

        const NativeType whole = getWholePart(decimal, scale);

        static const NativeType min_to = static_cast<NativeType>(std::numeric_limits<ToNativeType>::min());
        static const NativeType max_to = static_cast<NativeType>(std::numeric_limits<ToNativeType>::max());

        if (whole < min_to || whole > max_to)
            throw Exception("Convert overflow", ErrorCodes::DECIMAL_OVERFLOW);
        return static_cast<To>(static_cast<ToNativeType>(whole));
    }
    else
    {
        const NativeType whole = getWholePart(decimal, scale);

        static const NativeType min_to = static_cast<NativeType>(std::numeric_limits<To>::min());
        static const NativeType max_to = static_cast<NativeType>(std::numeric_limits<To>::max());

        if (whole < min_to || whole > max_to)
            throw Exception("Convert overflow", ErrorCodes::DECIMAL_OVERFLOW);
        return static_cast<To>(whole);
    }
}

}

}
