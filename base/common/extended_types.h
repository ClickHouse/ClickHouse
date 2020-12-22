#pragma once

#include <type_traits>

#include <common/types.h>
#include <common/wide_integer.h>

using Int128 = __int128;

using wInt256 = wide::integer<256, signed>;
using wUInt256 = wide::integer<256, unsigned>;

static_assert(sizeof(wInt256) == 32);
static_assert(sizeof(wUInt256) == 32);

static constexpr __int128 minInt128() { return static_cast<unsigned __int128>(1) << 127; }
static constexpr __int128 maxInt128() { return (static_cast<unsigned __int128>(1) << 127) - 1; }

/// The standard library type traits, such as std::is_arithmetic, with one exception
/// (std::common_type), are "set in stone". Attempting to specialize them causes undefined behavior.
/// So instead of using the std type_traits, we use our own version which allows extension.
template <typename T>
struct is_signed
{
    static constexpr bool value = std::is_signed_v<T>;
};

template <> struct is_signed<Int128> { static constexpr bool value = true; };
template <> struct is_signed<wInt256> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_signed_v = is_signed<T>::value;

template <typename T>
struct is_unsigned
{
    static constexpr bool value = std::is_unsigned_v<T>;
};

template <> struct is_unsigned<wUInt256> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_unsigned_v = is_unsigned<T>::value;


/// TODO: is_integral includes char, char8_t and wchar_t.
template <typename T>
struct is_integer
{
    static constexpr bool value = std::is_integral_v<T>;
};

template <> struct is_integer<Int128> { static constexpr bool value = true; };
template <> struct is_integer<wInt256> { static constexpr bool value = true; };
template <> struct is_integer<wUInt256> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_integer_v = is_integer<T>::value;


template <typename T>
struct is_arithmetic
{
    static constexpr bool value = std::is_arithmetic_v<T>;
};

template <> struct is_arithmetic<__int128> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_arithmetic_v = is_arithmetic<T>::value;

template <typename T>
struct make_unsigned
{
    typedef std::make_unsigned_t<T> type;
};

template <> struct make_unsigned<Int128> { using type = unsigned __int128; };
template <> struct make_unsigned<wInt256>  { using type = wUInt256; };
template <> struct make_unsigned<wUInt256> { using type = wUInt256; };

template <typename T> using make_unsigned_t = typename make_unsigned<T>::type;

template <typename T>
struct make_signed
{
    typedef std::make_signed_t<T> type;
};

template <> struct make_signed<wInt256>  { using type = wInt256; };
template <> struct make_signed<wUInt256> { using type = wInt256; };

template <typename T> using make_signed_t = typename make_signed<T>::type;

template <typename T>
struct is_big_int
{
    static constexpr bool value = false;
};

template <> struct is_big_int<wInt256> { static constexpr bool value = true; };
template <> struct is_big_int<wUInt256> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_big_int_v = is_big_int<T>::value;

template <typename To, typename From>
inline To bigint_cast(const From & x [[maybe_unused]])
{
    return static_cast<To>(x);
}
