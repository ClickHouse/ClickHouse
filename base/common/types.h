#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <type_traits>

#include <boost/multiprecision/cpp_int.hpp>

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;
using bInt128 = boost::multiprecision::int128_t;
using bInt256 = boost::multiprecision::int256_t;

#if __cplusplus <= 201703L
using char8_t = unsigned char;
#endif

using UInt8 = char8_t;
using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;
using bUInt128 = boost::multiprecision::uint128_t;
using bUInt256 = boost::multiprecision::uint256_t;

using String = std::string;

/// The standard library type traits, such as std::is_arithmetic, with one exception
/// (std::common_type), are "set in stone". Attempting to specialize them causes undefined behavior.
/// So instead of using the std type_traits, we use our own version which allows extension.
template <typename T>
struct is_signed
{
    static constexpr bool value = std::is_signed_v<T>;
};

template <> struct is_signed<bInt128> { static constexpr bool value = true; };
template <> struct is_signed<bInt256> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_signed_v = is_signed<T>::value;

template <typename T>
struct is_unsigned
{
    static constexpr bool value = std::is_unsigned_v<T>;
};

template <> struct is_unsigned<bUInt128> { static constexpr bool value = true; };
template <> struct is_unsigned<bUInt256> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_unsigned_v = is_unsigned<T>::value;

template <typename T>
struct is_integral
{
    static constexpr bool value = std::is_integral_v<T>;
};

template <> struct is_integral<bUInt128> { static constexpr bool value = true; };
template <> struct is_integral<bInt128> { static constexpr bool value = true; };
template <> struct is_integral<bUInt256> { static constexpr bool value = true; };
template <> struct is_integral<bInt256> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_integral_v = is_integral<T>::value;

template <typename T>
struct is_arithmetic
{
    static constexpr bool value = std::is_arithmetic_v<T>;
};

template <typename T>
inline constexpr bool is_arithmetic_v = is_arithmetic<T>::value;

template <typename T>
struct make_unsigned
{
    typedef std::make_unsigned_t<T> type;
};

template <> struct make_unsigned<bInt128>  { typedef bUInt128 type; };
template <> struct make_unsigned<bUInt128> { typedef bUInt128 type; };
template <> struct make_unsigned<bInt256>  { typedef bUInt256 type; };
template <> struct make_unsigned<bUInt256> { typedef bUInt256 type; };

template <typename T> using make_unsigned_t = typename make_unsigned<T>::type;

template <typename T>
struct make_signed
{
    typedef std::make_signed_t<T> type;
};

template <> struct make_signed<bInt128>  { typedef bInt128 type; };
template <> struct make_signed<bUInt128> { typedef bInt128 type; };
template <> struct make_signed<bInt256>  { typedef bInt256 type; };
template <> struct make_signed<bUInt256> { typedef bInt256 type; };

template <typename T> using make_signed_t = typename make_signed<T>::type;

template <typename T>
struct is_big_int
{
    static constexpr bool value = false;
};

template <> struct is_big_int<bUInt128> { static constexpr bool value = true; };
template <> struct is_big_int<bInt128> { static constexpr bool value = true; };
template <> struct is_big_int<bUInt256> { static constexpr bool value = true; };
template <> struct is_big_int<bInt256> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_big_int_v = is_big_int<T>::value;
