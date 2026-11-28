#pragma once

// Shared low-level helpers: bit-width/mask utilities, packed-size arithmetic, little-endian byte loads/stores.

#include <base/defines.h>

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace DB::PFor
{

/// Delta mode applied before block encoding: none = as-is, d0 = gaps v[i]-v[i-1] (non-decreasing), d1 = gaps-1 (strictly increasing).
enum class Delta : uint8_t { none = 0, d0 = 1, d1 = 2 };

namespace detail
{

/// Values per block. The last block of a stream may hold fewer.
inline constexpr unsigned BLOCK = 128;

template <typename T>
inline constexpr unsigned typeBits = 8u * sizeof(T);

/// Accumulator wide enough to hold a T value shifted by up to 7 carry bits.
template <typename T>
using Wide = std::conditional_t<(sizeof(T) <= 4), uint64_t, unsigned __int128>;

/// Number of significant bits in v (0 when v == 0).
template <typename T>
inline ALWAYS_INLINE unsigned bitWidth(T v) noexcept
{
    if (v == 0)
        return 0;
    if constexpr (sizeof(T) <= 4)
        return 32u - static_cast<unsigned>(__builtin_clz(static_cast<uint32_t>(v)));
    else
        return 64u - static_cast<unsigned>(__builtin_clzll(static_cast<uint64_t>(v)));
}

/// Mask of the low b bits (b may be 0 or >= typeBits<T>).
template <typename T>
inline ALWAYS_INLINE T lowMask(unsigned b) noexcept
{
    if (b == 0)
        return T(0);
    if (b >= typeBits<T>)
        return static_cast<T>(~T(0));
    return static_cast<T>((T(1) << b) - 1);
}

/// Bytes needed to bit-pack n values at width b.
inline ALWAYS_INLINE constexpr size_t packedBytes(size_t n, unsigned b) noexcept
{
    return (n * static_cast<size_t>(b) + 7) / 8;
}

/// Store the low k (0..8) bytes of v, little-endian. Endianness-independent on the wire.
inline ALWAYS_INLINE void storeLE(uint8_t * p, uint64_t v, unsigned k) noexcept
{
    for (unsigned i = 0; i < k; ++i)
        p[i] = static_cast<uint8_t>(v >> (8u * i));
}

/// Load k (0..8) little-endian bytes into a u64.
inline ALWAYS_INLINE uint64_t loadLE(const uint8_t * p, unsigned k) noexcept
{
    uint64_t v = 0;
    for (unsigned i = 0; i < k; ++i)
        v |= static_cast<uint64_t>(p[i]) << (8u * i);
    return v;
}

}
}
