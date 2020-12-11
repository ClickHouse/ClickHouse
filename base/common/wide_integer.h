#pragma once

///////////////////////////////////////////////////////////////
//  Distributed under the Boost Software License, Version 1.0.
//  (See at http://www.boost.org/LICENSE_1_0.txt)
///////////////////////////////////////////////////////////////

/*  Divide and multiply
 *
 *
 * Copyright (c) 2008
 * Evan Teran
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose and without fee is hereby granted, provided
 * that the above copyright notice appears in all copies and that both the
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the same name not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission. We make no representations about the
 * suitability this software for any purpose. It is provided "as is"
 * without express or implied warranty.
 */

#include <climits> // CHAR_BIT
#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace std
{
template <size_t Bits, typename Signed>
class wide_integer;

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
struct common_type<wide_integer<Bits, Signed>, wide_integer<Bits2, Signed2>>;

template <size_t Bits, typename Signed, typename Arithmetic>
struct common_type<wide_integer<Bits, Signed>, Arithmetic>;

template <typename Arithmetic, size_t Bits, typename Signed>
struct common_type<Arithmetic, wide_integer<Bits, Signed>>;

template <size_t Bits, typename Signed>
class wide_integer
{
public:
    using base_type = uint8_t;
    using signed_base_type = int8_t;

    // ctors
    wide_integer() = default;

    template <typename T>
    constexpr wide_integer(T rhs) noexcept;
    template <typename T>
    constexpr wide_integer(std::initializer_list<T> il) noexcept;

    // assignment
    template <size_t Bits2, typename Signed2>
    constexpr wide_integer<Bits, Signed> & operator=(const wide_integer<Bits2, Signed2> & rhs) noexcept;

    template <typename Arithmetic>
    constexpr wide_integer<Bits, Signed> & operator=(Arithmetic rhs) noexcept;

    template <typename Arithmetic>
    constexpr wide_integer<Bits, Signed> & operator*=(const Arithmetic & rhs);

    template <typename Arithmetic>
    constexpr wide_integer<Bits, Signed> & operator/=(const Arithmetic & rhs);

    template <typename Arithmetic>
    constexpr wide_integer<Bits, Signed> & operator+=(const Arithmetic & rhs) noexcept(is_same<Signed, unsigned>::value);

    template <typename Arithmetic>
    constexpr wide_integer<Bits, Signed> & operator-=(const Arithmetic & rhs) noexcept(is_same<Signed, unsigned>::value);

    template <typename Integral>
    constexpr wide_integer<Bits, Signed> & operator%=(const Integral & rhs);

    template <typename Integral>
    constexpr wide_integer<Bits, Signed> & operator&=(const Integral & rhs) noexcept;

    template <typename Integral>
    constexpr wide_integer<Bits, Signed> & operator|=(const Integral & rhs) noexcept;

    template <typename Integral>
    constexpr wide_integer<Bits, Signed> & operator^=(const Integral & rhs) noexcept;

    constexpr wide_integer<Bits, Signed> & operator<<=(int n);
    constexpr wide_integer<Bits, Signed> & operator>>=(int n) noexcept;

    constexpr wide_integer<Bits, Signed> & operator++() noexcept(is_same<Signed, unsigned>::value);
    constexpr wide_integer<Bits, Signed> operator++(int) noexcept(is_same<Signed, unsigned>::value);
    constexpr wide_integer<Bits, Signed> & operator--() noexcept(is_same<Signed, unsigned>::value);
    constexpr wide_integer<Bits, Signed> operator--(int) noexcept(is_same<Signed, unsigned>::value);

    // observers

    constexpr explicit operator bool() const noexcept;

    template <class T>
    using __integral_not_wide_integer_class = typename std::enable_if<std::is_arithmetic<T>::value, T>::type;

    template <class T, class = __integral_not_wide_integer_class<T>>
    constexpr operator T() const noexcept;

    constexpr operator long double() const noexcept;
    constexpr operator double() const noexcept;
    constexpr operator float() const noexcept;

    struct _impl;

private:
    template <size_t Bits2, typename Signed2>
    friend class wide_integer;

    friend class numeric_limits<wide_integer<Bits, signed>>;
    friend class numeric_limits<wide_integer<Bits, unsigned>>;

    base_type m_arr[_impl::arr_size];
};

template <typename T>
static constexpr bool ArithmeticConcept() noexcept;
template <class T1, class T2>
using __only_arithmetic = typename std::enable_if<ArithmeticConcept<T1>() && ArithmeticConcept<T2>()>::type;

template <typename T>
static constexpr bool IntegralConcept() noexcept;
template <class T, class T2>
using __only_integer = typename std::enable_if<IntegralConcept<T>() && IntegralConcept<T2>()>::type;

// Unary operators
template <size_t Bits, typename Signed>
constexpr wide_integer<Bits, Signed> operator~(const wide_integer<Bits, Signed> & lhs) noexcept;

template <size_t Bits, typename Signed>
constexpr wide_integer<Bits, Signed> operator-(const wide_integer<Bits, Signed> & lhs) noexcept(is_same<Signed, unsigned>::value);

template <size_t Bits, typename Signed>
constexpr wide_integer<Bits, Signed> operator+(const wide_integer<Bits, Signed> & lhs) noexcept(is_same<Signed, unsigned>::value);

// Binary operators
template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<wide_integer<Bits, Signed>, wide_integer<Bits2, Signed2>> constexpr
operator*(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Arithmetic, typename Arithmetic2, class = __only_arithmetic<Arithmetic, Arithmetic2>>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator*(const Arithmetic & rhs, const Arithmetic2 & lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<wide_integer<Bits, Signed>, wide_integer<Bits2, Signed2>> constexpr
operator/(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Arithmetic, typename Arithmetic2, class = __only_arithmetic<Arithmetic, Arithmetic2>>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator/(const Arithmetic & rhs, const Arithmetic2 & lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<wide_integer<Bits, Signed>, wide_integer<Bits2, Signed2>> constexpr
operator+(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Arithmetic, typename Arithmetic2, class = __only_arithmetic<Arithmetic, Arithmetic2>>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator+(const Arithmetic & rhs, const Arithmetic2 & lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<wide_integer<Bits, Signed>, wide_integer<Bits2, Signed2>> constexpr
operator-(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Arithmetic, typename Arithmetic2, class = __only_arithmetic<Arithmetic, Arithmetic2>>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator-(const Arithmetic & rhs, const Arithmetic2 & lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<wide_integer<Bits, Signed>, wide_integer<Bits2, Signed2>> constexpr
operator%(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Integral, typename Integral2, class = __only_integer<Integral, Integral2>>
std::common_type_t<Integral, Integral2> constexpr operator%(const Integral & rhs, const Integral2 & lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<wide_integer<Bits, Signed>, wide_integer<Bits2, Signed2>> constexpr
operator&(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Integral, typename Integral2, class = __only_integer<Integral, Integral2>>
std::common_type_t<Integral, Integral2> constexpr operator&(const Integral & rhs, const Integral2 & lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<wide_integer<Bits, Signed>, wide_integer<Bits2, Signed2>> constexpr
operator|(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Integral, typename Integral2, class = __only_integer<Integral, Integral2>>
std::common_type_t<Integral, Integral2> constexpr operator|(const Integral & rhs, const Integral2 & lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<wide_integer<Bits, Signed>, wide_integer<Bits2, Signed2>> constexpr
operator^(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Integral, typename Integral2, class = __only_integer<Integral, Integral2>>
std::common_type_t<Integral, Integral2> constexpr operator^(const Integral & rhs, const Integral2 & lhs);

// TODO: Integral
template <size_t Bits, typename Signed>
constexpr wide_integer<Bits, Signed> operator<<(const wide_integer<Bits, Signed> & lhs, int n) noexcept;
template <size_t Bits, typename Signed>
constexpr wide_integer<Bits, Signed> operator>>(const wide_integer<Bits, Signed> & lhs, int n) noexcept;

template <size_t Bits, typename Signed, typename Int, typename = std::enable_if_t<!std::is_same_v<Int, int>>>
constexpr wide_integer<Bits, Signed> operator<<(const wide_integer<Bits, Signed> & lhs, Int n) noexcept
{
    return lhs << int(n);
}
template <size_t Bits, typename Signed, typename Int, typename = std::enable_if_t<!std::is_same_v<Int, int>>>
constexpr wide_integer<Bits, Signed> operator>>(const wide_integer<Bits, Signed> & lhs, Int n) noexcept
{
    return lhs >> int(n);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator<(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Arithmetic, typename Arithmetic2, class = __only_arithmetic<Arithmetic, Arithmetic2>>
constexpr bool operator<(const Arithmetic & rhs, const Arithmetic2 & lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator>(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Arithmetic, typename Arithmetic2, class = __only_arithmetic<Arithmetic, Arithmetic2>>
constexpr bool operator>(const Arithmetic & rhs, const Arithmetic2 & lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator<=(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Arithmetic, typename Arithmetic2, class = __only_arithmetic<Arithmetic, Arithmetic2>>
constexpr bool operator<=(const Arithmetic & rhs, const Arithmetic2 & lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator>=(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Arithmetic, typename Arithmetic2, class = __only_arithmetic<Arithmetic, Arithmetic2>>
constexpr bool operator>=(const Arithmetic & rhs, const Arithmetic2 & lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator==(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Arithmetic, typename Arithmetic2, class = __only_arithmetic<Arithmetic, Arithmetic2>>
constexpr bool operator==(const Arithmetic & rhs, const Arithmetic2 & lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator!=(const wide_integer<Bits, Signed> & lhs, const wide_integer<Bits2, Signed2> & rhs);
template <typename Arithmetic, typename Arithmetic2, class = __only_arithmetic<Arithmetic, Arithmetic2>>
constexpr bool operator!=(const Arithmetic & rhs, const Arithmetic2 & lhs);

template <size_t Bits, typename Signed>
std::string to_string(const wide_integer<Bits, Signed> & n);

template <size_t Bits, typename Signed>
struct hash<wide_integer<Bits, Signed>>;

}

#include "wide_integer_impl.h"
