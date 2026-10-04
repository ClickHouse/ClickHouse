#pragma once

/// Unsigned 16.16 fixed point numbers (floating point must not be used in the allocator core).
/// jemalloc: `fxp.h`, `src/fxp.c`.

#include <allocator/Common.h>

#include <cstdint>

namespace jemalloc
{

/// High 16 bits are the integer part, low 16 are the fractional part (`fxp_t`).
using FixedPoint = uint32_t;

namespace fxp
{

/// jemalloc: FXP_INIT_INT
constexpr FixedPoint initInt(uint32_t x)
{
    return x << 16;
}

/// jemalloc: FXP_INIT_PERCENT
constexpr FixedPoint initPercent(uint32_t pct)
{
    return (pct << 16) / 100;
}

/// Number of digits used in parsing and printing (`FXP_INTEGER_PART_DIGITS`, `FXP_FRACTIONAL_PART_DIGITS`).
inline constexpr int INTEGER_PART_DIGITS = 5;
inline constexpr int FRACTIONAL_PART_DIGITS = 14;

/// Integer part, fractional part, a decimal point and the NUL (`FXP_BUF_SIZE`).
inline constexpr size_t BUF_SIZE = INTEGER_PART_DIGITS + FRACTIONAL_PART_DIGITS + 2;

/// jemalloc: fxp_add
constexpr FixedPoint add(FixedPoint a, FixedPoint b)
{
    return a + b;
}

/// jemalloc: fxp_sub
constexpr FixedPoint sub(FixedPoint a, FixedPoint b)
{
    JE_ASSERT(a >= b);
    return a - b;
}

/// jemalloc: fxp_mul
constexpr FixedPoint mul(FixedPoint a, FixedPoint b)
{
    uint64_t unshifted = uint64_t(a) * uint64_t(b);
    return static_cast<uint32_t>(unshifted >> 16);
}

/// jemalloc: fxp_div
constexpr FixedPoint div(FixedPoint a, FixedPoint b)
{
    JE_ASSERT(b != 0);
    uint64_t unshifted = (uint64_t(a) << 32) / uint64_t(b);
    return static_cast<uint32_t>(unshifted >> 16);
}

/// jemalloc: fxp_round_down
constexpr uint32_t roundDown(FixedPoint a)
{
    return a >> 16;
}

/// jemalloc: fxp_round_nearest
constexpr uint32_t roundNearest(FixedPoint a)
{
    uint32_t fractional_part = a & ((1U << 16) - 1);
    uint32_t increment = static_cast<uint32_t>(fractional_part >= (1U << 15));
    return (a >> 16) + increment;
}

/// Approximately computes `x * frac`, without the size limitations of converting `x` to a `FixedPoint`.
/// jemalloc: fxp_mul_frac
constexpr size_t mulFrac(size_t x_orig, FixedPoint frac)
{
    JE_ASSERT(frac <= (1U << 16));
    uint64_t x = x_orig;
    /// If we can guarantee no overflow, multiply first before shifting, to preserve some precision.
    if (x < (1ULL << 48))
        return static_cast<size_t>((x * frac) >> 16);
    else
        return static_cast<size_t>((x >> 16) * uint64_t(frac));
}

/// Returns true on error. Otherwise, returns false and sets `*end` (if not null) to the first character not parsed.
/// jemalloc: fxp_parse
bool parse(FixedPoint * result, const char * str, const char ** end);

inline bool parse(FixedPoint * result, const char * str, char ** end)
{
    return parse(result, str, const_cast<const char **>(end));
}

/// jemalloc: fxp_print
void print(FixedPoint a, char (&buf)[BUF_SIZE]);

}

}
