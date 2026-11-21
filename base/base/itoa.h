#pragma once

#include <array>
#include <bit>
#include <cstring>
#include <string_view>
#include <base/defines.h>
#include <base/extended_types.h>

#include <type_traits>

/// The text of every `UInt8` value, 4 bytes each: the digits from the lowest address on, and the length in the last byte.
extern const std::array<UInt32, 256> one_byte_to_string_lookup_table;

/// The text of `value`, pointing into `one_byte_to_string_lookup_table`.
inline std::string_view oneByteToString(UInt8 value)
{
    const UInt32 & entry = one_byte_to_string_lookup_table[value];
    size_t length = std::endian::native == std::endian::little ? entry >> 24 : entry & 0xFF;
    return {reinterpret_cast<const char *>(&entry), length};
}

/// Values below 10000 are written here, so that they are inlined into the callers: one table load and store below 256,
/// two multiplications for 3 and 4 digits. Longer values, and the 128- and 256-bit types, are written in `itoa.cpp`.

/*
    MIT License

    Copyright (c) 2022 James Edward Anhalt III - https://github.com/jeaiii/itoa

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

namespace itoa_impl
{
/// Out of line: the values of 5 digits and more.
char * writeUInt32TextFrom10000(UInt32 value, char * p);
char * writeUInt64TextFrom10000(UInt64 value, char * p);

/// The shift that places byte `i` of an integer's in-memory representation.
template <typename T>
constexpr UInt32 byteShift(UInt32 i)
{
    return 8 * (std::endian::native == std::endian::little ? i : sizeof(T) - 1 - i);
}

/// `value` is below 256. One load and one 4-byte store, with no branch on the length. The bytes after the text are the
/// rest of the entry, overwritten by whatever is written next; `max_int_width` reserves them.
ALWAYS_INLINE inline char * writeUInt8Text(UInt32 value, char * p)
{
    UInt32 entry = one_byte_to_string_lookup_table[value];
    memcpy(p, &entry, 4);
    return p + ((entry >> byteShift<UInt32>(3)) & 0xFF);
}

/// The 2 bytes from byte `offset` of the text of `value` in `one_byte_to_string_lookup_table`.
ALWAYS_INLINE inline void copyTwoTextBytes(UInt32 value, UInt32 offset, char * p)
{
    memcpy(p, reinterpret_cast<const char *>(&one_byte_to_string_lookup_table[value]) + offset, 2);
}

/// `value` is from 100 to 9999, as jeaiii (https://github.com/jeaiii/itoa) does: the value times a rounded-up
/// 2^24 / 10^2 has the first two digits above bit 24 and the rest as a fraction below, and the fraction times 100
/// brings the last two above bit 24. The first pair is the text of a value below 100, a digit and a zero byte when
/// below 10, overwritten next. The second is the last two digits of the text of 100 to 199.
ALWAYS_INLINE inline char * writeThreeOrFourDigits(UInt32 value, char * p)
{
    UInt32 first = UInt32(10 * (1 << 24) / 1000 + 1) * value;
    copyTwoTextBytes(first >> 24, 0, p);
    p -= value < 1000;
    UInt32 second = (first & ((1 << 24) - 1)) * 100;
    copyTwoTextBytes(100 + (second >> 24), 1, p + 2);
    return p + 4;
}

template <typename U>
ALWAYS_INLINE inline char * writeUnsignedText(U value, char * p)
{
    /// One branch for the longer values, which are out of line anyway.
    if (value < 10000)
    {
        if (value < 256)
            return writeUInt8Text(static_cast<UInt32>(value), p);
        return writeThreeOrFourDigits(static_cast<UInt32>(value), p);
    }
    if constexpr (sizeof(U) <= 4)
        return writeUInt32TextFrom10000(value, p);
    else
        return writeUInt64TextFrom10000(value, p);
}

/// The sign without a branch, which mispredicts on mixed-sign data: '-' is always written, and the magnitude
/// `(i ^ sign) - sign`, in unsigned arithmetic so that the minimum value fits, is written over it when `i` is
/// not negative.
template <typename T>
ALWAYS_INLINE inline char * writeSignedText(T i, char * p)
{
    using U = std::make_unsigned_t<T>;
    U sign = static_cast<U>(i >> (sizeof(T) * 8 - 1));
    U magnitude = static_cast<U>((static_cast<U>(i) ^ sign) - sign);
    *p = '-';
    p += sign & 1;
    if constexpr (sizeof(T) == 1)
        return writeUInt8Text(magnitude, p);
    else
        return writeUnsignedText(magnitude, p);
}
}

inline char * itoa(UInt8 i, char * p)
{
    return itoa_impl::writeUInt8Text(i, p);
}

inline char * itoa(uint8_t i, char * p)
{
    return itoa_impl::writeUInt8Text(i, p);
}

inline char * itoa(Int8 i, char * p)
{
    return itoa_impl::writeSignedText(static_cast<int8_t>(i), p);
}

inline char * itoa(int8_t i, char * p)
{
    return itoa_impl::writeSignedText(i, p);
}

inline char * itoa(UInt16 i, char * p)
{
    return itoa_impl::writeUnsignedText(i, p);
}

inline char * itoa(Int16 i, char * p)
{
    return itoa_impl::writeSignedText(i, p);
}

inline char * itoa(UInt32 i, char * p)
{
    return itoa_impl::writeUnsignedText(i, p);
}

inline char * itoa(Int32 i, char * p)
{
    return itoa_impl::writeSignedText(i, p);
}

inline char * itoa(UInt64 i, char * p)
{
    return itoa_impl::writeUnsignedText(i, p);
}

inline char * itoa(Int64 i, char * p)
{
    return itoa_impl::writeSignedText(i, p);
}

/// `long` is not one of the types above where it is a distinct type.
/// Naming the type is the whole point here, so `google-runtime-int` has nothing to suggest.
// NOLINTBEGIN(google-runtime-int)
#if defined(LONG_IS_A_DISTINCT_TYPE)
static_assert(
    !std::is_same_v<long, Int32> && !std::is_same_v<long, Int64>,
    "`long` is one of the fixed-width types here, so it must not be defined separately");
inline char * itoa(unsigned long i, char * p)
{
    return itoa_impl::writeUnsignedText(i, p);
}

inline char * itoa(long i, char * p)
{
    return itoa_impl::writeSignedText(i, p);
}
#else
static_assert(
    std::is_same_v<long, Int32> || std::is_same_v<long, Int64>,
    "`long` is a distinct type here and needs an overload of its own");
#endif
// NOLINTEND(google-runtime-int)

char * itoa(UInt128 i, char * p);
char * itoa(Int128 i, char * p);

/// The 256-bit types are taken by reference. By value they are `byval` aggregates holding an array, and
/// that alone puts a `-fstack-protector-strong` canary on the function.
char * itoa(const UInt256 & i, char * p);
char * itoa(const Int256 & i, char * p);

char * writeFixedDigits(UInt64 value, UInt32 width, char * p);
char * writeFixedDigits(UInt128 value, UInt32 width, char * p);
char * writeFixedDigits(const UInt256 & value, UInt32 width, char * p);

void setUseAVX512ItoaForTests(bool value);
bool getUseAVX512ItoaForTests();
