#pragma once

#include <type_traits>

#include <base/types.h>
#include <base/wide_integer.h>

using Int128 = wide::integer<128, signed>;
using UInt128 = wide::integer<128, unsigned>;
using Int256 = wide::integer<256, signed>;
using UInt256 = wide::integer<256, unsigned>;

static_assert(sizeof(Int256) == 32);
static_assert(sizeof(UInt256) == 32);

/// The standard library type traits, such as std::is_arithmetic, with one exception
/// (std::common_type), are "set in stone". Attempting to specialize them causes undefined behavior.
/// So instead of using the std type_traits, we use our own version which allows extension.
template <typename T>
struct is_signed // NOLINT(readability-identifier-naming)
{
    static constexpr bool value = std::is_signed_v<T>;
};

template <> struct is_signed<Int128> { static constexpr bool value = true; };
template <> struct is_signed<Int256> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_signed_v = is_signed<T>::value;

template <typename T>
struct is_unsigned // NOLINT(readability-identifier-naming)
{
    static constexpr bool value = std::is_unsigned_v<T>;
};

template <> struct is_unsigned<UInt128> { static constexpr bool value = true; };
template <> struct is_unsigned<UInt256> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_unsigned_v = is_unsigned<T>::value;

template <class T> concept is_integer =
    std::is_integral_v<T>
    || std::is_same_v<T, Int128>
    || std::is_same_v<T, UInt128>
    || std::is_same_v<T, Int256>
    || std::is_same_v<T, UInt256>;

template <class T> concept is_floating_point = std::is_floating_point_v<T>;

template <typename T>
struct is_arithmetic // NOLINT(readability-identifier-naming)
{
    static constexpr bool value = std::is_arithmetic_v<T>;
};

template <> struct is_arithmetic<Int128> { static constexpr bool value = true; };
template <> struct is_arithmetic<UInt128> { static constexpr bool value = true; };
template <> struct is_arithmetic<Int256> { static constexpr bool value = true; };
template <> struct is_arithmetic<UInt256> { static constexpr bool value = true; };


template <typename T>
inline constexpr bool is_arithmetic_v = is_arithmetic<T>::value;

#define FOR_EACH_ARITHMETIC_TYPE(M) \
    M(DataTypeDate) \
    M(DataTypeDate32) \
    M(DataTypeDateTime) \
    M(DataTypeInt8) \
    M(DataTypeUInt8) \
    M(DataTypeInt16) \
    M(DataTypeUInt16) \
    M(DataTypeInt32) \
    M(DataTypeUInt32) \
    M(DataTypeInt64) \
    M(DataTypeUInt64) \
    M(DataTypeInt128) \
    M(DataTypeUInt128) \
    M(DataTypeInt256) \
    M(DataTypeUInt256) \
    M(DataTypeFloat32) \
    M(DataTypeFloat64)

#define FOR_EACH_ARITHMETIC_TYPE_PASS(M, X) \
    M(DataTypeDate, X) \
    M(DataTypeDate32, X) \
    M(DataTypeDateTime, X) \
    M(DataTypeInt8, X) \
    M(DataTypeUInt8, X) \
    M(DataTypeInt16, X) \
    M(DataTypeUInt16, X) \
    M(DataTypeInt32, X) \
    M(DataTypeUInt32, X) \
    M(DataTypeInt64, X) \
    M(DataTypeUInt64, X) \
    M(DataTypeInt128, X) \
    M(DataTypeUInt128, X) \
    M(DataTypeInt256, X) \
    M(DataTypeUInt256, X) \
    M(DataTypeFloat32, X) \
    M(DataTypeFloat64, X)

template <typename T>
struct make_unsigned // NOLINT(readability-identifier-naming)
{
    using type = std::make_unsigned_t<T>;
};

template <> struct make_unsigned<Int8> { using type = UInt8; };
template <> struct make_unsigned<UInt8> { using type = UInt8; };
template <> struct make_unsigned<Int16> { using type = UInt16; };
template <> struct make_unsigned<UInt16> { using type = UInt16; };
template <> struct make_unsigned<Int32> { using type = UInt32; };
template <> struct make_unsigned<UInt32> { using type = UInt32; };
template <> struct make_unsigned<Int64> { using type = UInt64; };
template <> struct make_unsigned<UInt64> { using type = UInt64; };
template <> struct make_unsigned<Int128> { using type = UInt128; };
template <> struct make_unsigned<UInt128> { using type = UInt128; };
template <> struct make_unsigned<Int256>  { using type = UInt256; };
template <> struct make_unsigned<UInt256> { using type = UInt256; };

template <typename T> using make_unsigned_t = typename make_unsigned<T>::type;

template <typename T>
struct make_signed // NOLINT(readability-identifier-naming)
{
    using type = std::make_signed_t<T>;
};

template <> struct make_signed<Int8> { using type = Int8; };
template <> struct make_signed<UInt8> { using type = Int8; };
template <> struct make_signed<Int16> { using type = Int16; };
template <> struct make_signed<UInt16> { using type = Int16; };
template <> struct make_signed<Int32> { using type = Int32; };
template <> struct make_signed<UInt32> { using type = Int32; };
template <> struct make_signed<Int64> { using type = Int64; };
template <> struct make_signed<UInt64> { using type = Int64; };
template <> struct make_signed<Int128>  { using type = Int128; };
template <> struct make_signed<UInt128> { using type = Int128; };
template <> struct make_signed<Int256>  { using type = Int256; };
template <> struct make_signed<UInt256> { using type = Int256; };

template <typename T> using make_signed_t = typename make_signed<T>::type;

template <typename T>
struct is_big_int // NOLINT(readability-identifier-naming)
{
    static constexpr bool value = false;
};

template <> struct is_big_int<Int128> { static constexpr bool value = true; };
template <> struct is_big_int<UInt128> { static constexpr bool value = true; };
template <> struct is_big_int<Int256> { static constexpr bool value = true; };
template <> struct is_big_int<UInt256> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_big_int_v = is_big_int<T>::value;
