#pragma once

#include <cstddef>
#include <type_traits>


/** Returns log2 of number, rounded down.
  * Compiles to single 'bsr' instruction on x86.
  * For zero argument, result is unspecified.
  */
inline unsigned int bitScanReverse(unsigned int x)
{
    return sizeof(unsigned int) * 8 - 1 - __builtin_clz(x);
}


/** For zero argument, result is zero.
  * For arguments with most significand bit set, result is zero.
  * For other arguments, returns value, rounded up to power of two.
  */
inline size_t roundUpToPowerOfTwoOrZero(size_t n)
{
    --n;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    ++n;

    return n;
}


template <typename T>
inline std::enable_if_t<std::is_integral_v<T> && (sizeof(T) <= sizeof(unsigned int)), int>
getLeadingZeroBits(T x)
{
    return x == 0 ? sizeof(x) * 8 : __builtin_clz(x);
}

template <typename T>
inline std::enable_if_t<std::is_integral_v<T> && (sizeof(T) == sizeof(unsigned long long int)), int>
getLeadingZeroBits(T x)
{
    return x == 0 ? sizeof(x) * 8 : __builtin_clzll(x);
}

template <typename T>
inline std::enable_if_t<std::is_integral_v<T> && (sizeof(T) <= sizeof(unsigned int)), int>
getTrailingZeroBits(T x)
{
    return x == 0 ? sizeof(x) * 8 : __builtin_ctz(x);
}

template <typename T>
inline std::enable_if_t<std::is_integral_v<T> && (sizeof(T) == sizeof(unsigned long long int)), int>
getTrailingZeroBits(T x)
{
    return x == 0 ? sizeof(x) * 8 : __builtin_ctzll(x);
}
