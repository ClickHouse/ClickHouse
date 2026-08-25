#pragma once
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <type_traits>
#include <base/defines.h>
#include <base/types.h>

namespace DB
{

template <typename T>
concept FLOAT = std::is_same_v<T, Float32> || std::is_same_v<T, Float64>;

template <FLOAT T>
struct ALPFloatTraits
{
    static constexpr bool IS_FLOAT32 = std::is_same_v<T, Float32>;
    static constexpr UInt8 EXPONENT_COUNT = IS_FLOAT32 ? 10 : 19;
    using Unsigned = std::conditional_t<IS_FLOAT32, UInt32, UInt64>;
};

struct ALPFloatUtils
{
    static constexpr UInt8 EXPONENT_COUNT = ALPFloatTraits<Float64>::EXPONENT_COUNT;

    static constexpr std::array<Float64, EXPONENT_COUNT> EXPONENTS
        = {1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18};
    static constexpr std::array<Float64, EXPONENT_COUNT> FRACTIONS
        = {1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18};

    static constexpr Float64 UPPER = (1ull << 63) - 2048;
    static constexpr Float64 LOWER = -UPPER;
    static constexpr Float64 ROUND_MAGIC = (1ull << 51) + (1ull << 52);

    static ALWAYS_INLINE Int64 encodeValue(Float32 value, UInt8 exponent, UInt8 fraction)
    {
        return encodeValue(static_cast<Float64>(value), exponent, fraction);
    }

    static Int64 encodeValue(Float64 value, UInt8 exponent, UInt8 fraction)
    {
        Float64 value_enc = value * EXPONENTS[exponent] * FRACTIONS[fraction];
        const bool invalid
            = std::isnan(value_enc) || value_enc < LOWER || value_enc > UPPER || (value_enc == 0.0 && std::signbit(value_enc));
        if (invalid)
            return static_cast<Int64>(UPPER);
        value_enc = value_enc + ROUND_MAGIC - ROUND_MAGIC;
        return static_cast<Int64>(value_enc);
    }

    template <FLOAT T>
    static T decodeValue(Int64 value, UInt8 exponent, UInt8 fraction)
    {
        Float64 value_dec = static_cast<Float64>(value) * EXPONENTS[fraction] * FRACTIONS[exponent];
        return static_cast<T>(value_dec);
    }
};

template <FLOAT T>
struct ALPUtils
{
    static UInt8 calculateBitWidth(const UInt64 value) { return static_cast<UInt8>(std::bit_width(value)); }
};

}
