#pragma once

#include <base/extended_types.h>

#include <bit>
#include <cmath>


/** In IEEE 754 negative zero is equal to positive zero by the rules of comparison,
  * but it has a different binary representation - the sign bit and nothing else.
  *
  * Hash tables in ClickHouse compare floating point keys bitwise (see `bitEquals`),
  * so that NaN is equal to itself - otherwise `GROUP BY` and `DISTINCT` would not work on NaN values.
  * The price of this decision is that negative zero has to be canonicalized to positive zero
  * before a floating point value is hashed or used as a hash table key.
  *
  * Otherwise the operations based on hashing (`GROUP BY`, `DISTINCT`, `IN`, hash `JOIN`)
  * disagree with the `equals` function and with the operations based on comparison,
  * such as the sorting merge join.
  *
  * Note that there is no similar canonicalization for NaN: it is not equal to itself,
  * so there is no way to make hash tables agree with `equals` on NaN values,
  * and grouping equal binary representations together is the most useful behaviour.
  *
  * The check is written on the binary representation on purpose: a floating point comparison
  * with zero is noticeably slower in the hashing loops.
  */

template <typename T>
inline bool isNegativeZero(T x)
{
    if constexpr (std::is_same_v<T, BFloat16>)
        return x.raw() == UInt16(1) << 15;
    else if constexpr (is_floating_point<T> && sizeof(T) == sizeof(UInt32))
        return std::bit_cast<UInt32>(x) == UInt32(1) << 31;
    else if constexpr (is_floating_point<T> && sizeof(T) == sizeof(UInt64))
        return std::bit_cast<UInt64>(x) == UInt64(1) << 63;
    else if constexpr (is_floating_point<T>)
        return x == T{} && std::signbit(x);
    else
        return false;
}

template <typename T>
inline T normalizeNegativeZero(T x)
{
    return isNegativeZero(x) ? T{} : x;
}
