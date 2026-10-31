#pragma once

#include <bit>
#include <limits>
#include <vector>

#include <base/DecomposedFloat.h>
#include <base/extended_types.h>
#include <base/sort.h>

#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/NaNUtils.h>
#include <Common/iota.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int PARAMETER_OUT_OF_BOUND;
}


/** Parameters of different functions quantiles*.
  * - list of levels of quantiles.
  * It is also necessary to calculate an array of indices of levels that go in ascending order.
  *
  * Example: quantiles(0.5, 0.99, 0.95)(x).
  * levels: 0.5, 0.99, 0.95
  * levels_permutation: 0, 2, 1
  */
template <typename T>    /// float or double
struct QuantileLevels
{
    using Levels = VectorWithMemoryTracking<T>;
    using Permutation = VectorWithMemoryTracking<size_t>;

    Levels levels;
    Permutation permutation;    /// Index of the i-th level in `levels`.

    size_t size() const { return levels.size(); }

    QuantileLevels(const Array & params, bool require_at_least_one_param)
    {
        if (params.empty())
        {
            if (require_at_least_one_param)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "Aggregate function for calculation "
                                "of multiple quantiles require at least one parameter");

            /// If levels are not specified, default is 0.5 (median).
            levels.push_back(0.5);
            permutation.push_back(0);
            return;
        }

        size_t size = params.size();
        levels.resize(size);
        permutation.resize(size);

        for (size_t i = 0; i < size; ++i)
        {
            levels[i] = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[i]);

            if (isNaN(levels[i]) || levels[i] < 0 || levels[i] > 1)
                throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Quantile level is out of range [0..1]");
        }

        iota(permutation.data(), size, Permutation::value_type(0));
        ::sort(permutation.begin(), permutation.end(), [this] (size_t a, size_t b) { return levels[a] < levels[b]; });
    }
};


/** Interpolation between two endpoints of an integer-backed quantile result.
  *
  * Every caller passes its own `Float64` expression as a callback: the shapes differ between
  * callers (two independently rounded endpoint products versus divide-before-multiply), and
  * they do not agree, so one cannot be substituted for another.
  */
namespace QuantileInterpolation
{

template <size_t Bits> struct UnsignedIntOfWidth;
template <> struct UnsignedIntOfWidth<64> { using Type = UInt64; };
template <> struct UnsignedIntOfWidth<128> { using Type = UInt128; };
template <> struct UnsignedIntOfWidth<256> { using Type = UInt256; };
template <> struct UnsignedIntOfWidth<512> { using Type = wide::integer<512, unsigned>; };

constexpr size_t widthHolding(size_t bits)
{
    size_t width = 64;
    while (width < bits)
        width *= 2;
    return width;
}

/// An N-bit difference times a 53-bit significand needs N + 53 bits.
template <typename T> using Intermediate = typename UnsignedIntOfWidth<widthHolding(sizeof(T) * 8 + 53)>::Type;

template <typename U>
size_t bitLength(const U & x)
{
    if constexpr (std::is_integral_v<U>)
        return static_cast<size_t>(std::bit_width(static_cast<UInt64>(x)));
    else
    {
        for (size_t offset = sizeof(U) * 8; offset >= 64; offset -= 64)
            if (auto chunk = static_cast<UInt64>(x >> static_cast<int>(offset - 64)); chunk != 0)
                return (offset - 64) + static_cast<size_t>(std::bit_width(chunk));
        return 0;
    }
}

/// |min()| is not representable in the signed type itself.
template <typename T>
make_unsigned_t<T> magnitude(T x)
{
    using U = make_unsigned_t<T>;
    if constexpr (is_signed_v<T>)
        return x < 0 ? static_cast<U>(static_cast<U>(0) - static_cast<U>(x)) : static_cast<U>(x);
    else
        return static_cast<U>(x);
}

template <typename T>
bool differenceFitsNative(T lo, T hi)
{
    using U = make_unsigned_t<T>;
    constexpr U highest = static_cast<U>(std::numeric_limits<T>::max());
    if (hi >= lo)
        return static_cast<U>(static_cast<U>(hi) - static_cast<U>(lo)) <= highest;
    if constexpr (is_signed_v<T>)
        return static_cast<U>(static_cast<U>(lo) - static_cast<U>(hi)) <= static_cast<U>(highest + static_cast<U>(1));
    else
        return false;
}

/** Whether the caller's own `Float64` expression is used: both endpoints are below the magnitude
  * at which the `Float64` spacing exceeds one. The expression over them still rounds.
  *
  * `forms_native_difference` must be true only for a caller that subtracts the endpoints in
  * the native type before converting, since that subtraction overflows at the extremes. It
  * must be false for a caller that weights each endpoint separately, which is well defined
  * there.
  */
template <typename T>
bool endpointsAreExactAsFloat64(T lo, T hi, bool forms_native_difference)
{
    if (forms_native_difference && !differenceFitsNative(lo, hi))
        return false;
    if constexpr (sizeof(T) * 8 <= 54)
        return true;
    else
    {
        /// Inclusive: 2^53 is itself exactly representable.
        constexpr auto limit = static_cast<make_unsigned_t<T>>(1) << 53;
        return magnitude(lo) <= limit && magnitude(hi) <= limit;
    }
}

/// A finite positive double is exactly `significand / 2^shift`.
struct DyadicDouble
{
    UInt64 significand;
    size_t shift;
};

inline DyadicDouble decomposeToDyadic(Float64 x)
{
    const DecomposedFloat64 parts(x);
    const uint16_t biased_exponent = parts.exponent();
    chassert(biased_exponent <= 1075);
    if (biased_exponent == 0) /// Subnormal: a level of 5e-324 is accepted by `QuantileLevels`.
        return {parts.mantissa(), 1074};
    return {parts.mantissa() | (1ULL << 52), static_cast<size_t>(1075 - biased_exponent)};
}

/** `0 <= offset <= difference` puts the result inside the endpoints by construction.
  * The truncation is applied to the final value, not to the offset: rounding toward zero is a
  * property of the whole interpolated expression.
  */
template <typename T>
T applyOffset(T lo, make_unsigned_t<T> difference, make_unsigned_t<T> offset, bool ascending, bool fraction_dropped)
{
    using U = make_unsigned_t<T>;
    chassert(offset <= difference);
    const U shifted = ascending
        ? static_cast<U>(static_cast<U>(lo) + offset)
        : static_cast<U>(static_cast<U>(lo) - offset);
    T result = static_cast<T>(shifted);
    if (fraction_dropped)
    {
        if constexpr (is_signed_v<T>)
        {
            if (ascending && result < 0)
                ++result;
        }
        if (!ascending && result > static_cast<T>(0))
            --result;
    }
    return result;
}

/** `coef` is the weight of `hi`. `legacy` returns the caller's own `Float64` expression; it is
  * a callback because for some callers evaluating it is only defined inside the exact window.
  */
template <typename T, typename LegacyFn>
requires is_integer<T>
T interpolate(T lo, T hi, Float64 coef, LegacyFn && legacy, bool forms_native_difference)
{
    if (lo == hi)
        return lo;
    /// The endpoints are returned without forming a product: `1.0 * Float64(UInt64::max)` is
    /// 2^64, which no native type represents.
    if (isNaN(coef) || coef <= 0.0)
        return lo;
    if (coef >= 1.0)
        return hi;
    if (endpointsAreExactAsFloat64(lo, hi, forms_native_difference))
        return static_cast<T>(legacy());

    using U = make_unsigned_t<T>;
    using W = Intermediate<T>;
    const bool ascending = hi > lo;
    const U difference = ascending
        ? static_cast<U>(static_cast<U>(hi) - static_cast<U>(lo))
        : static_cast<U>(static_cast<U>(lo) - static_cast<U>(hi));
    const DyadicDouble weight = decomposeToDyadic(coef);

    U offset = 0;
    bool fraction_dropped = true;
    /// A shift of at least the width is undefined, and the quotient there is zero anyway.
    if (weight.shift < sizeof(W) * 8)
    {
        const W product = static_cast<W>(difference) * static_cast<W>(weight.significand);
        const W quotient = product >> static_cast<int>(weight.shift);
        fraction_dropped = (quotient << static_cast<int>(weight.shift)) != product;
        offset = static_cast<U>(quotient);
    }
    return applyOffset<T>(lo, difference, offset, ascending, fraction_dropped);
}

/** For a caller whose coefficient is a ratio of two dyadic rationals, both factors are kept:
  * normalising them into one `Float64` first rounds, and the rounding then propagates through
  * the exact multiplication.
  */
template <typename T, typename LegacyFn>
requires is_integer<T>
T interpolateRatio(T lo, T hi, Float64 numerator, Float64 denominator, LegacyFn && legacy)
{
    if (lo == hi)
        return lo;
    if (isNaN(numerator) || isNaN(denominator) || !(numerator > 0.0) || !(denominator > 0.0))
        return lo;
    if (!(numerator < denominator))
        return hi;
    if (endpointsAreExactAsFloat64(lo, hi, /* forms_native_difference= */ true))
        return static_cast<T>(legacy());

    using U = make_unsigned_t<T>;
    using W = Intermediate<T>;
    const bool ascending = hi > lo;
    const U difference = ascending
        ? static_cast<U>(static_cast<U>(hi) - static_cast<U>(lo))
        : static_cast<U>(static_cast<U>(lo) - static_cast<U>(hi));

    DyadicDouble scaled_up = decomposeToDyadic(numerator);
    DyadicDouble scaled_down = decomposeToDyadic(denominator);
    /// Cancel the common power of two and compare bit lengths before materialising: a level of
    /// 5e-324 has a shift of 1074, which no available width holds.
    const size_t common = std::min(scaled_up.shift, scaled_down.shift);
    scaled_up.shift -= common;
    scaled_down.shift -= common;
    const size_t numerator_bits = bitLength(difference) + bitLength(scaled_up.significand) + scaled_down.shift;
    const size_t denominator_bits = bitLength(scaled_down.significand) + scaled_up.shift;

    U offset = 0;
    bool fraction_dropped = true;
    if (numerator_bits >= denominator_bits)
    {
        chassert(numerator_bits <= sizeof(W) * 8 && denominator_bits <= sizeof(W) * 8);
        const W scaled_numerator
            = (static_cast<W>(difference) * static_cast<W>(scaled_up.significand)) << static_cast<int>(scaled_down.shift);
        const W scaled_denominator = static_cast<W>(scaled_down.significand) << static_cast<int>(scaled_up.shift);
        chassert(scaled_denominator != 0);
        W quotient = scaled_numerator / scaled_denominator;
        fraction_dropped = quotient * scaled_denominator != scaled_numerator;
        /// `numerator < denominator` above bounds the quotient strictly below the difference.
        chassert(quotient < static_cast<W>(difference));
        if (quotient >= static_cast<W>(difference))
        {
            quotient = static_cast<W>(difference);
            fraction_dropped = false;
        }
        offset = static_cast<U>(quotient);
    }
    return applyOffset<T>(lo, difference, offset, ascending, fraction_dropped);
}

}

}
