#pragma once
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <type_traits>

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;

using UInt8 = uint8_t;
using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

/// The standard library type traits, such as std::is_arithmetic, with one exception
/// (std::common_type), are "set in stone". Attempting to specialize them causes undefined behavior.
/// So instead of using the std type_traits, we use our own version which allows extension.
template <typename T>
struct is_signed
{
    static constexpr bool value = std::is_signed_v<T>;
};

template <typename T>
inline constexpr bool is_signed_v = is_signed<T>::value;

template <typename T>
struct is_unsigned
{
    static constexpr bool value = std::is_unsigned_v<T>;
};

template <typename T>
inline constexpr bool is_unsigned_v = is_unsigned<T>::value;

template <typename T>
struct is_integral
{
    static constexpr bool value = std::is_integral_v<T>;
};

template <typename T>
inline constexpr bool is_integral_v = is_integral<T>::value;

template <typename T>
struct is_arithmetic
{
    static constexpr bool value = std::is_arithmetic_v<T>;
};

template <typename T>
inline constexpr bool is_arithmetic_v = is_arithmetic<T>::value;

struct UInt8NoAlias
{
    uint8_t value;
    operator uint8_t & () { return value; }
    operator const uint8_t & () const { return value; }
    UInt8NoAlias() {}
    template <typename T, typename = std::enable_if_t<std::is_arithmetic_v<T>>>
    UInt8NoAlias(const T & o) : value(o) {}
    template <typename T, typename = std::enable_if_t<std::is_arithmetic_v<T>>>
    UInt8NoAlias operator=(const T & o) { return value = o; }

    bool inline operator== (const UInt8NoAlias rhs) const { return value == rhs.value; }
    bool inline operator!= (const UInt8NoAlias rhs) const { return value != rhs.value; }
    bool inline operator<  (const UInt8NoAlias rhs) const { return value < rhs.value; }
    bool inline operator<= (const UInt8NoAlias rhs) const { return value <= rhs.value; }
    bool inline operator>  (const UInt8NoAlias rhs) const { return value > rhs.value; }
    bool inline operator>= (const UInt8NoAlias rhs) const { return value >= rhs.value; }

    template <typename T> bool inline operator== (const T rhs) const { return value == rhs; }
    template <typename T> bool inline operator!= (const T rhs) const { return value != rhs; }
    template <typename T> bool inline operator>= (const T rhs) const { return value >= rhs; }
    template <typename T> bool inline operator>  (const T rhs) const { return value >  rhs; }
    template <typename T> bool inline operator<= (const T rhs) const { return value <= rhs; }
    template <typename T> bool inline operator<  (const T rhs) const { return value <  rhs; }
};

inline void memset(UInt8NoAlias * data, int c, size_t n)
{
    memset(&data->value, c, n);
}

template <typename T> decltype(auto) toNativeValue(T && v) { return std::forward<T>(v); }
inline UInt8 * toNativeValue(UInt8NoAlias * v) { return &v->value; }
inline const UInt8 * toNativeValue(const UInt8NoAlias * v) { return &v->value; }
inline UInt8 & toNativeValue(UInt8NoAlias & v) { return v.value; }
inline const UInt8 & toNativeValue(const UInt8NoAlias & v) { return v.value; }
inline UInt8 toNativeValue(UInt8NoAlias && v) { return v.value; }

template <> struct is_signed<UInt8NoAlias>
{
    static constexpr bool value = false;
};

template <> struct is_unsigned<UInt8NoAlias>
{
    static constexpr bool value = true;
};

template <> struct is_integral<UInt8NoAlias>
{
    static constexpr bool value = true;
};

template <> struct is_arithmetic<UInt8NoAlias>
{
    static constexpr bool value = true;
};
