#pragma once

// SIMD vertical bit-packing for full 128-value uint32_t blocks. Independently authored.
//
// Values are laid out in 4 interleaved 32-bit lanes: value i -> lane (i & 3), row (i >> 2),
// 32 rows per lane. The 4 lanes share one bit cursor, so a 16-byte stripe carries one
// 32-bit chunk of every lane and decode emits 4 values per vector step. The byte layout is
// exactly packedBytes(128, b) = 16*b bytes -- identical in size to the scalar horizontal
// packing, just reordered for parallel extraction. Used only for b in [1, 31]; b == 0 / 32
// and partial blocks / uint64 stay on the scalar path.
//
// GCC/Clang vector extensions lower each lane op to one SSE/NEON instruction.

#include <Compression/PFor/common.h>

#include <bit>
#include <cstring>

#if defined(__GNUC__) || defined(__clang__)
#    define PFOR_HAS_VERTICAL 1
#else
#    define PFOR_HAS_VERTICAL 0
#endif

#if PFOR_HAS_VERTICAL

namespace DB::PFor::detail
{

using v4u32 = uint32_t __attribute__((vector_size(16)));

// The packed stream is canonical little-endian (matching bitpack.h), but a vector store/load is
// native-endian, so byte-swap each 32-bit lane on big-endian targets. On little-endian this is the
// identity and folds back to a plain 16-byte move, leaving the fast path unchanged.
inline ALWAYS_INLINE v4u32 bswapLanes(v4u32 v) noexcept
{
    return v4u32{__builtin_bswap32(v[0]), __builtin_bswap32(v[1]), __builtin_bswap32(v[2]), __builtin_bswap32(v[3])};
}

inline ALWAYS_INLINE void storeStripeLE(uint8_t * p, v4u32 v) noexcept
{
    if constexpr (std::endian::native == std::endian::big)
        v = bswapLanes(v);
    std::memcpy(p, &v, 16);
}

inline ALWAYS_INLINE v4u32 loadStripeLE(const uint8_t * p) noexcept
{
    v4u32 v;
    std::memcpy(&v, p, 16);
    if constexpr (std::endian::native == std::endian::big)
        v = bswapLanes(v);
    return v;
}

inline ALWAYS_INLINE void packVertical32(const uint32_t * r, unsigned b, uint8_t * out) noexcept
{
    const uint32_t m = (1u << b) - 1u; // b in [1,31]
    const v4u32 mask = {m, m, m, m};
    v4u32 acc = {0, 0, 0, 0};
    unsigned bits = 0;
    uint8_t * p = out;
    for (unsigned row = 0; row < 32; ++row)
    {
        v4u32 v;
        std::memcpy(&v, r + 4u * row, 16);
        v &= mask;
        acc |= v << bits;
        const unsigned nb = bits + b;
        if (nb >= 32)
        {
            storeStripeLE(p, acc);
            p += 16;
            if (nb == 32)
            {
                acc = v4u32{0, 0, 0, 0};
                bits = 0;
            }
            else
            {
                acc = v >> (32 - bits); // bits > 0 here, so shift in [1,31]
                bits = nb - 32;
            }
        }
        else
        {
            bits = nb;
        }
    }
}

inline ALWAYS_INLINE void unpackVertical32(const uint8_t * in, unsigned b, uint32_t * out) noexcept
{
    const uint32_t m = (1u << b) - 1u; // b in [1,31]
    const v4u32 mask = {m, m, m, m};
    v4u32 acc = {0, 0, 0, 0};
    unsigned bits = 0;
    const uint8_t * p = in;
    for (unsigned row = 0; row < 32; ++row)
    {
        v4u32 outv;
        if (bits >= b)
        {
            outv = acc & mask;
            acc >>= b;
            bits -= b;
        }
        else
        {
            v4u32 w = loadStripeLE(p);
            p += 16;
            outv = (acc | (w << bits)) & mask; // low `bits` from acc, the rest from w
            acc = w >> (b - bits); // b - bits in [1,31]
            bits = 32 - (b - bits);
        }
        std::memcpy(out + 4u * row, &outv, 16);
    }
}

// SIMD delta reconstruction (inclusive prefix sum) over a contiguous uint32 residual
// array, with a running carry across blocks. `plus` is 0 for d0 and 1 for d1 (gap-1).
// Replaces the scalar prefix-sum: each 4-lane group does a 2-step in-vector scan
// (lane-wise left shifts, which lower to a single byte-shift each) plus the carry.
template <uint32_t plus>
inline ALWAYS_INLINE void deltaDecode32(uint32_t * out, unsigned cnt, uint32_t & carry) noexcept
{
    const v4u32 plusv = {plus, plus, plus, plus};
    const v4u32 zero = {0, 0, 0, 0};
    v4u32 cv = {carry, carry, carry, carry};
    unsigned i = 0;
    /// Two groups per step: their local sums do not depend on the carry, so the carry chain is one add and one broadcast per 8 values.
    for (; i + 8 <= cnt; i += 8)
    {
        v4u32 x;
        v4u32 y;
        std::memcpy(&x, out + i, 16);
        std::memcpy(&y, out + i + 4, 16);
        x += plusv;
        y += plusv;
        x += __builtin_shufflevector(zero, x, 3, 4, 5, 6);
        y += __builtin_shufflevector(zero, y, 3, 4, 5, 6);
        x += __builtin_shufflevector(zero, x, 2, 3, 4, 5);
        y += __builtin_shufflevector(zero, y, 2, 3, 4, 5);
        y += __builtin_shufflevector(x, x, 3, 3, 3, 3);
        x += cv;
        y += cv;
        std::memcpy(out + i, &x, 16);
        std::memcpy(out + i + 4, &y, 16);
        cv = __builtin_shufflevector(y, y, 3, 3, 3, 3);
    }
    uint32_t c = cv[0];
    for (; i < cnt; ++i) // tail of at most 7 values
    {
        c += out[i] + plus;
        out[i] = c;
    }
    carry = c;
}

// Fused single-pass unpack + delta: like unpackVertical32 but each row's 4 residuals are
// prefix-summed with the running carry and stored as final values, so there is no second
// pass over the output. Valid only for exception-free blocks (residuals == decoded base).
// plus is 0 for d0, 1 for d1.
template <uint32_t plus>
inline ALWAYS_INLINE void unpackVertical32FusedDelta(
    const uint8_t * in, unsigned b, uint32_t * out, uint32_t & carry) noexcept
{
    const uint32_t m = (1u << b) - 1u; // b in [1,31]
    const v4u32 mask = {m, m, m, m};
    const v4u32 plusv = {plus, plus, plus, plus};
    v4u32 acc = {0, 0, 0, 0};
    unsigned bits = 0;
    const uint8_t * p = in;
    const v4u32 zero = {0, 0, 0, 0};
    v4u32 cv = {carry, carry, carry, carry};
    auto unpack_row = [&]() ALWAYS_INLINE
    {
        v4u32 v;
        if (bits >= b)
        {
            v = acc & mask;
            acc >>= b;
            bits -= b;
        }
        else
        {
            v4u32 w = loadStripeLE(p);
            p += 16;
            v = (acc | (w << bits)) & mask;
            acc = w >> (b - bits);
            bits = 32 - (b - bits);
        }
        return v + plusv;
    };
    /// Two rows per step, as in `deltaDecode32`: the carry chain is one add and one broadcast per 8 values.
    for (unsigned row = 0; row < 32; row += 2)
    {
        v4u32 x = unpack_row();
        v4u32 y = unpack_row();
        x += __builtin_shufflevector(zero, x, 3, 4, 5, 6);
        y += __builtin_shufflevector(zero, y, 3, 4, 5, 6);
        x += __builtin_shufflevector(zero, x, 2, 3, 4, 5);
        y += __builtin_shufflevector(zero, y, 2, 3, 4, 5);
        y += __builtin_shufflevector(x, x, 3, 3, 3, 3);
        x += cv;
        y += cv;
        std::memcpy(out + 4u * row, &x, 16);
        std::memcpy(out + 4u * row + 4, &y, 16);
        cv = __builtin_shufflevector(y, y, 3, 3, 3, 3);
    }
    carry = cv[0];
}

}

#endif // PFOR_HAS_VERTICAL
