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
inline size_t getLeadingZeroBits(T x)
{
    if (!x)
        return sizeof(x) * 8;

    if constexpr (sizeof(T) <= sizeof(unsigned int))
    {
        return __builtin_clz(x);
    }
    else if constexpr (sizeof(T) <= sizeof(unsigned long int))
    {
        return __builtin_clzl(x);
    }
    else
    {
        return __builtin_clzll(x);
    }
}

// Unsafe since __builtin_ctz()-family explicitly state that result is undefined on x == 0
template <typename T>
inline size_t getTrailingZeroBitsUnsafe(T x)
{
    if constexpr (sizeof(T) <= sizeof(unsigned int))
    {
        return __builtin_ctz(x);
    }
    else if constexpr (sizeof(T) <= sizeof(unsigned long int))
    {
        return __builtin_ctzl(x);
    }
    else
    {
        return __builtin_ctzll(x);
    }
}

template <typename T>
inline size_t getTrailingZeroBits(T x)
{
    if (!x)
        return sizeof(x) * 8;

    return getTrailingZeroBitsUnsafe(x);
}

/** Returns a mask that has '1' for `bits` LSB set:
 * maskLowBits<UInt8>(3) => 00000111
 */
template <typename T>
inline T maskLowBits(unsigned char bits)
{
    if (bits == 0)
    {
        return 0;
    }

    T result = static_cast<T>(~T{0});
    if (bits < sizeof(T) * 8)
    {
        result = static_cast<T>(result >> (sizeof(T) * 8 - bits));
    }

    return result;
}
