#pragma once

#include <type_traits>
#include "IsAny.h"
#include "extended_types.h"

/// A more common use case for concepts is a more general one: when we see an "Integral T", we should assume
/// T can be one of our extended integral types. If extended types are not allowed, one can use a NativeIntegral
/// concept otherwise.

template <class T> concept Signed = std::is_signed_v<T> || is_any<T, Int128, Int256>;
template <class T> concept Unsigned = std::is_unsigned_v<T> || is_any<T, UInt128, UInt256>;

template <class T> concept ExtIntegral = is_any<T, Int128, UInt128, Int256, UInt256>;
template <class T> concept NativeIntegral = std::is_integral_v<T>;

template <class T> concept Integral = NativeIntegral<T> || ExtIntegral<T>;

template <class T> concept Float = std::is_floating_point_v<T>;

template <class T> concept NativeArithmetic = std::is_arithmetic_v<T>;
template <class T> concept Arithmetic = NativeArithmetic<T> || ExtIntegral<T>;

template <class T>
struct make_unsigned { using type = std::make_unsigned_t<T>; }; //NOLINT

template <> struct make_unsigned<Int128> { using type = UInt128; };
template <> struct make_unsigned<UInt128> { using type = UInt128; };
template <> struct make_unsigned<Int256>  { using type = UInt256; };
template <> struct make_unsigned<UInt256> { using type = UInt256; };

template <class T> using make_unsigned_t = typename make_unsigned<T>::type;

template <class T>
struct make_signed { using type = std::make_signed_t<T>; }; //NOLINT

template <> struct make_signed<Int128>  { using type = Int128; };
template <> struct make_signed<UInt128> { using type = Int128; };
template <> struct make_signed<Int256>  { using type = Int256; };
template <> struct make_signed<UInt256> { using type = Int256; };

template <class T> using make_signed_t = typename make_signed<T>::type;
