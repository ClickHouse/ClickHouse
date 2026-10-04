#pragma once

#include <base/types.h>
#include <base/unaligned.h>

#include <bit>


/** Comparison of 16-byte values that are ordered by their byte representation: `IPv6` addresses,
  * `FixedString(16)` and the network-order form of a `UUID`.
  *
  * Both implementations below produce the same order. They differ only in how the value is handed
  * to the optimizer:
  *
  * - One native 128-bit integer compiles to `movbe`, `cmp`, `sbb`, `setcc` on x86-64 and to `ldp`,
  *   `rev`, `cmp`, `sbcs`, `cset` on AArch64. Both are branchless and neither needs a vector unit.
  * - Two separate 64-bit halves let a loop over these functions vectorize where the target has an
  *   unsigned 64-bit vector compare, which on x86-64 means AVX-512 `vpcmpuq`. Without it the
  *   vectorizer has no such instruction, extracts every lane back into a general purpose register
  *   and the result is slower than the 128-bit form, so that form stays the default.
  */

namespace detail
{

inline UInt64 loadBigEndian64(const void * p)
{
    UInt64 v = unalignedLoad<UInt64>(p);
    if constexpr (std::endian::native == std::endian::little)
        v = std::byteswap(v);
    return v;
}

inline unsigned __int128 loadBigEndian128(const void * p)
{
    const UInt64 hi = loadBigEndian64(p);
    const UInt64 lo = loadBigEndian64(static_cast<const char *>(p) + sizeof(UInt64));
    return static_cast<unsigned __int128>(hi) << 64 | lo;
}

}


#if defined(__AVX512F__) && defined(__AVX512VL__)

inline bool lessBigEndian16(const void * a, const void * b)
{
    const UInt64 a_hi = detail::loadBigEndian64(a);
    const UInt64 b_hi = detail::loadBigEndian64(b);
    const UInt64 a_lo = detail::loadBigEndian64(static_cast<const char *>(a) + sizeof(UInt64));
    const UInt64 b_lo = detail::loadBigEndian64(static_cast<const char *>(b) + sizeof(UInt64));
    return (a_hi < b_hi) | ((a_hi == b_hi) & (a_lo < b_lo));
}

inline int compareBigEndian16(const void * a, const void * b)
{
    const UInt64 a_hi = detail::loadBigEndian64(a);
    const UInt64 b_hi = detail::loadBigEndian64(b);
    const UInt64 a_lo = detail::loadBigEndian64(static_cast<const char *>(a) + sizeof(UInt64));
    const UInt64 b_lo = detail::loadBigEndian64(static_cast<const char *>(b) + sizeof(UInt64));
    const bool hi_equal = a_hi == b_hi;
    const int less = (a_hi < b_hi) | (hi_equal & (a_lo < b_lo));
    const int greater = (a_hi > b_hi) | (hi_equal & (a_lo > b_lo));
    return greater - less;
}

#else

inline bool lessBigEndian16(const void * a, const void * b)
{
    return detail::loadBigEndian128(a) < detail::loadBigEndian128(b);
}

inline int compareBigEndian16(const void * a, const void * b)
{
    const unsigned __int128 x = detail::loadBigEndian128(a);
    const unsigned __int128 y = detail::loadBigEndian128(b);
    return (x > y) - (x < y);
}

#endif
