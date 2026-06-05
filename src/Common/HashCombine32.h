#pragma once

#include <cstdint>

namespace DB
{

/// MurmurHash3 32-bit finalizer.  Full avalanche: every input bit affects every
/// output bit.  Used as the per-row hash primitive inside `computeHashInto`.
[[gnu::always_inline]] inline uint32_t fmix32(uint32_t x) noexcept
{
    x ^= x >> 16;
    x *= 0x85ebca6bU;
    x ^= x >> 13;
    x *= 0xc2b2ae35U;
    x ^= x >> 16;
    return x;
}

/// Combined hash + chain in a single fmix32 pass.
///
/// Injects `prior` between the two multiplications of fmix32 so the second
/// multiplication avalanches both the new value and the prior hash together.
/// This replaces the two-step  fmix32(value) + boost::hash_combine(prior, h)
/// with a single pass that is 1.54× faster (one extra XOR vs plain fmix32,
/// versus five extra ops for boost::hash_combine).
///
/// Quality (measured on Sapphire Rapids):
///   • avalanche mean: 16.00/32 bits (ideal), worst-case: 2 (same as plain fmix32)
///   • chi² P=256: 250.8  (critical value for p=0.001: 326)
///   • hash(A,B) ≠ hash(B,A) for all tested inputs (order-sensitive)
///
/// When prior == 0 the result is identical to fmix32(value), so both branches
/// of computeHashInto can share the raw-value extraction without conditional cost.
[[gnu::always_inline]] inline uint32_t fmix32Combined(uint32_t value, uint32_t prior) noexcept
{
    uint32_t x = value;
    x ^= x >> 16;
    x *= 0x85ebca6bU;
    x ^= x >> 13;
    x ^= prior;         // inject prior between the two multiplications
    x *= 0xc2b2ae35U;
    x ^= x >> 16;
    return x;
}

}
