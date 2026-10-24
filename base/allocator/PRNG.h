#pragma once

/// Simple linear congruential pseudo-random number generator (jemalloc: `prng.h`).
///
///     prng(x) = (a * x + c) % m
///
/// with constants that ensure maximal period (Knuth TAOCP vol. 2, 3rd ed., p. 17): `a` odd and `(a - 1)` a multiple
/// of 4, `c` odd, `m` = 2^32 or 2^64. The quality of the bits is proportional to their position (the lowest bit has a
/// cycle of 2, ...), so the results are taken from the upper bits.
///
/// The exact streams matter: they decide prof sampling, decay deadline jitter, ticker_geom firing, arena/bin shard
/// selection, so the constants and the shifts must stay exactly as in jemalloc.

#include <allocator/Common.h>

namespace jemalloc
{

inline constexpr uint32_t PRNG_A_32 = 1103515241u;
inline constexpr uint32_t PRNG_C_32 = 12347u;
inline constexpr uint64_t PRNG_A_64 = 6364136223846793005ull;
inline constexpr uint64_t PRNG_C_64 = 1442695040888963407ull;

static_assert(sizeof(size_t) == 8, "Only 64-bit platforms are supported");

/// jemalloc: prng_state_next_u32
ALLOCATOR_ALWAYS_INLINE constexpr uint32_t prngStateNextU32(uint32_t state)
{
    return (state * PRNG_A_32) + PRNG_C_32;
}

/// jemalloc: prng_state_next_u64
ALLOCATOR_ALWAYS_INLINE constexpr uint64_t prngStateNextU64(uint64_t state)
{
    return (state * PRNG_A_64) + PRNG_C_64;
}

/// jemalloc: prng_state_next_zu (LG_SIZEOF_PTR == 3)
ALLOCATOR_ALWAYS_INLINE constexpr size_t prngStateNextSize(size_t state)
{
    return (state * PRNG_A_64) + PRNG_C_64;
}

/// A uniform integer in [0, 2^lg_range); `log2_range` in [1, 32]. Advances `state`.
/// jemalloc: prng_lg_range_u32
ALLOCATOR_ALWAYS_INLINE constexpr uint32_t prngLog2RangeU32(uint32_t & state, unsigned log2_range)
{
    ALLOCATOR_ASSERT(log2_range > 0);
    ALLOCATOR_ASSERT(log2_range <= 32);
    state = prngStateNextU32(state);
    return state >> (32 - log2_range);
}

/// A uniform integer in [0, 2^lg_range); `log2_range` in [1, 64]. Advances `state`.
/// jemalloc: prng_lg_range_u64
ALLOCATOR_ALWAYS_INLINE constexpr uint64_t prngLog2RangeU64(uint64_t & state, unsigned log2_range)
{
    ALLOCATOR_ASSERT(log2_range > 0);
    ALLOCATOR_ASSERT(log2_range <= 64);
    state = prngStateNextU64(state);
    return state >> (64 - log2_range);
}

/// jemalloc: prng_lg_range_zu
ALLOCATOR_ALWAYS_INLINE constexpr size_t prngLog2RangeSize(size_t & state, unsigned log2_range)
{
    ALLOCATOR_ASSERT(log2_range > 0);
    ALLOCATOR_ASSERT(log2_range <= (size_t(1) << (3 + LG_SIZEOF_PTR)));
    state = prngStateNextSize(state);
    return state >> ((size_t(1) << (3 + LG_SIZEOF_PTR)) - log2_range);
}

/// A uniform integer in [0, range) by repeated trial; `range` must be non-zero.
/// jemalloc: prng_range_u32
ALLOCATOR_ALWAYS_INLINE constexpr uint32_t prngRangeU32(uint32_t & state, uint32_t range)
{
    ALLOCATOR_ASSERT(range != 0);
    /// With range == 1, lg_range would be 0 and the shift would be by 32 bits (UB). The state is not advanced.
    if (range == 1)
        return 0;

    /// `ffs_u32(pow2_ceil_u32(range))` = ceil(lg(range)).
    unsigned log2_range = findFirstSet(static_cast<uint32_t>(pow2Ceil(range)));

    uint32_t result;
    do
    {
        result = prngLog2RangeU32(state, log2_range);
    } while (result >= range);
    return result;
}

/// jemalloc: prng_range_u64
ALLOCATOR_ALWAYS_INLINE constexpr uint64_t prngRangeU64(uint64_t & state, uint64_t range)
{
    ALLOCATOR_ASSERT(range != 0);
    if (range == 1)
        return 0;

    unsigned log2_range = findFirstSet(pow2Ceil(range));

    uint64_t result;
    do
    {
        result = prngLog2RangeU64(state, log2_range);
    } while (result >= range);
    return result;
}

/// jemalloc: prng_range_zu
ALLOCATOR_ALWAYS_INLINE constexpr size_t prngRangeSize(size_t & state, size_t range)
{
    ALLOCATOR_ASSERT(range != 0);
    if (range == 1)
        return 0;

    unsigned log2_range = findFirstSet(pow2Ceil(range));

    size_t result;
    do
    {
        result = prngLog2RangeSize(state, log2_range);
    } while (result >= range);
    return result;
}

}
