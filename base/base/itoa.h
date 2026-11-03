#pragma once

#include <array>
#include <bit>
#include <string_view>
#include <base/defines.h>
#include <base/extended_types.h>

#include <type_traits>

#define FOR_INTEGER_TYPES(M) \
    M(uint8_t) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(UInt128) \
    M(int8_t) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Int128)

#define INSTANTIATION(T) char * itoa(T i, char * p);
FOR_INTEGER_TYPES(INSTANTIATION)

/// The 256-bit types are taken by reference. By value they are `byval` aggregates holding an array, and
/// that alone puts a `-fstack-protector-strong` canary on the function.
char * itoa(const UInt256 & i, char * p);
char * itoa(const Int256 & i, char * p);

/// `long` is not covered by the list above where it is a distinct type.
/// Naming the type is the whole point here, so `google-runtime-int` has nothing to suggest.
// NOLINTBEGIN(google-runtime-int)
#if defined(LONG_IS_A_DISTINCT_TYPE)
static_assert(
    !std::is_same_v<long, Int32> && !std::is_same_v<long, Int64>,
    "`long` is one of the fixed-width types here, so it must not be instantiated separately");
INSTANTIATION(unsigned long)
INSTANTIATION(long)
#else
static_assert(
    std::is_same_v<long, Int32> || std::is_same_v<long, Int64>,
    "`long` is a distinct type here and needs an instantiation of its own");
#endif
// NOLINTEND(google-runtime-int)

#undef FOR_INTEGER_TYPES
#undef INSTANTIATION

char * writeFixedDigits(UInt64 value, UInt32 width, char * p);
char * writeFixedDigits(UInt128 value, UInt32 width, char * p);
char * writeFixedDigits(const UInt256 & value, UInt32 width, char * p);

/// The text of every `UInt8` value, 4 bytes each: the digits from the lowest address on, and the length in the last byte.
extern const std::array<UInt32, 256> one_byte_to_string_lookup_table;

/// The text of `value`, pointing into `one_byte_to_string_lookup_table`.
inline std::string_view oneByteToString(UInt8 value)
{
    const UInt32 & entry = one_byte_to_string_lookup_table[value];
    size_t length = std::endian::native == std::endian::little ? entry >> 24 : entry & 0xFF;
    return {reinterpret_cast<const char *>(&entry), length};
}

void setUseAVX512ItoaForTests(bool value);
bool getUseAVX512ItoaForTests();
