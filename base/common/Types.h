#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <type_traits>

#include <boost/multiprecision/integer.hpp>

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;
using bInt256 = boost::multiprecision::int256_t;

using UInt8 = uint8_t;
using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;
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

template <>
struct is_signed<bInt256>
{
    static constexpr bool value = true;
};

template <typename T>
inline constexpr bool is_signed_v = is_signed<T>::value;

template <typename T>
struct is_unsigned
{
    static constexpr bool value = std::is_unsigned_v<T>;
};

template <>
struct is_unsigned<bUInt256>
{
    static constexpr bool value = true;
};

template <typename T>
inline constexpr bool is_unsigned_v = is_unsigned<T>::value;

template <typename T>
struct is_integral
{
    static constexpr bool value = std::is_integral_v<T>;
};

template <>
struct is_integral<bInt256>
{
    static constexpr bool value = true;
};

template <>
struct is_integral<bUInt256>
{
    static constexpr bool value = true;
};

template <typename T>
inline constexpr bool is_integral_v = is_integral<T>::value;

template <typename T>
struct is_arithmetic
{
    static constexpr bool value = std::is_arithmetic_v<T>;
};

template <>
struct is_arithmetic<bInt256>
{
    static constexpr bool value = true;
};

template <>
struct is_arithmetic<bUInt256>
{
    static constexpr bool value = true;
};

template <typename T>
inline constexpr bool is_arithmetic_v = is_arithmetic<T>::value;
