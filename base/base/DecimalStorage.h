#pragma once

#include "types.h"
#include "IsAny.h"

namespace DB
{
template <class> struct DecimalStorage;
class DateTime64;

using Decimal32 = DecimalStorage<Int32>;
using Decimal64 = DecimalStorage<Int64>;
using Decimal128 = DecimalStorage<Int128>;
using Decimal256 = DecimalStorage<Int256>;

/**
 * Most common use case: operate on a decimal-like object (decimal functions etc.), so Decimal is most general.
 * Sometimes we need to check for DateTime64 separately, so DecimalStrict allows to check only for Decimal32...256.
 * Finally, we may want to check for decimals which underlying object is language-native (e.g. for vectorization
 * purposes), so DecimalNativeIntegral is the right concept.
 */

template <class T>
concept DecimalNativeIntegral = is_any<T, Decimal32, Decimal64>;

template <class T>
concept DecimalExtIntegral = is_any<T, Decimal128, Decimal256>;

template <class T>
concept DecimalStrict = DecimalExtIntegral<T> || DecimalNativeIntegral<T>;

template <class T>
concept Decimal = DecimalStrict<T> || is_any<T, DateTime64>;

template <class T> struct NativeTypeT { using Type = T; };
template <Decimal T> struct NativeTypeT<T> { using Type = typename T::NativeType; };
template <class T> using NativeType = typename NativeTypeT<T>::Type;

/// Own FieldType for Decimal.
/// It is only a "storage" for decimal.
/// To perform operations, you also have to provide a scale (number of digits after point).
template <class T>
struct DecimalStorage
{
    using NativeType = T;

    constexpr DecimalStorage() = default;
    constexpr DecimalStorage(DecimalStorage<T> &&) = default;
    constexpr DecimalStorage(const DecimalStorage<T> &) = default;

    constexpr DecimalStorage(const T & value_): value(value_) {}

    template <typename U>
    constexpr DecimalStorage(const DecimalStorage<U> & x): value(x.value) {}

    constexpr DecimalStorage<T> & operator=(DecimalStorage<T> &&) = default;
    constexpr DecimalStorage<T> & operator=(const DecimalStorage<T> &) = default;

    constexpr operator T () const { return value; }

    template <typename U>
    constexpr U convertTo() const
    {
        if constexpr (Decimal<U>)
            return convertTo<typename U::NativeType>();
        else
            return static_cast<U>(value);
    }

    constexpr const DecimalStorage & operator+=(const T & x) { value += x; return *this; }
    constexpr const DecimalStorage & operator-=(const T & x) { value -= x; return *this; }
    constexpr const DecimalStorage & operator*=(const T & x) { value *= x; return *this; }
    constexpr const DecimalStorage & operator/=(const T & x) { value /= x; return *this; }
    constexpr const DecimalStorage & operator%=(const T & x) { value %= x; return *this; }

    template <class U> constexpr const DecimalStorage & operator+=(const DecimalStorage<U> & x) { value += x.value; return *this; }
    template <class U> constexpr const DecimalStorage & operator-=(const DecimalStorage<U> & x) { value -= x.value; return *this; }
    template <class U> constexpr const DecimalStorage & operator*=(const DecimalStorage<U> & x) { value *= x.value; return *this; }
    template <class U> constexpr const DecimalStorage & operator/=(const DecimalStorage<U> & x) { value /= x.value; return *this; }
    template <class U> constexpr const DecimalStorage & operator%=(const DecimalStorage<U> & x) { value %= x.value; return *this; }

    /// This is to avoid UB for sumWithOverflow()
    [[clang::no_sanitize("undefined")]] void addOverflow(const T & x) { value += x; }

    T value;
};

template <class T> constexpr bool operator< (const DecimalStorage<T> & x, const DecimalStorage<T> & y) { return x.value < y.value; }
template <class T> constexpr bool operator> (const DecimalStorage<T> & x, const DecimalStorage<T> & y) { return x.value > y.value; }
template <class T> constexpr bool operator<=(const DecimalStorage<T> & x, const DecimalStorage<T> & y) { return x.value <= y.value; }
template <class T> constexpr bool operator>=(const DecimalStorage<T> & x, const DecimalStorage<T> & y) { return x.value >= y.value; }
template <class T> constexpr bool operator==(const DecimalStorage<T> & x, const DecimalStorage<T> & y) { return x.value == y.value; }
template <class T> constexpr bool operator!=(const DecimalStorage<T> & x, const DecimalStorage<T> & y) { return x.value != y.value; }

template <class T> constexpr DecimalStorage<T> operator+(const DecimalStorage<T> & x, const DecimalStorage<T> & y) { return x.value + y.value; }
template <class T> constexpr DecimalStorage<T> operator-(const DecimalStorage<T> & x, const DecimalStorage<T> & y) { return x.value - y.value; }
template <class T> constexpr DecimalStorage<T> operator*(const DecimalStorage<T> & x, const DecimalStorage<T> & y) { return x.value * y.value; }
template <class T> constexpr DecimalStorage<T> operator/(const DecimalStorage<T> & x, const DecimalStorage<T> & y) { return x.value / y.value; }
template <class T> constexpr DecimalStorage<T> operator-(const DecimalStorage<T> & x) { return -x.value; }

/// Distinguishable type to allow function resolution/deduction based on value type,
/// but also relatively easy to convert to/from Decimal64.
class DateTime64 : public Decimal64
{
public:
    using Base = Decimal64;
    using Base::Base;
    using NativeType = Base::NativeType;

    constexpr DateTime64(const Base & v): Base(v) {} //NOLINT
};
}

constexpr DB::UInt64 max_uint_mask = std::numeric_limits<DB::UInt64>::max();

namespace std
{
    template <typename T>
    struct hash<DB::DecimalStorage<T>>
    {
        size_t operator()(const DB::DecimalStorage<T> & x) const { return hash<T>()(x.value); }
    };

    template <>
    struct hash<DB::Decimal128>
    {
        size_t operator()(const DB::Decimal128 & x) const
        {
            return std::hash<DB::Int64>()(x.value >> 64)
                ^ std::hash<DB::Int64>()(x.value & max_uint_mask);
        }
    };

    template <>
    struct hash<DB::DateTime64>
    {
        size_t operator()(const DB::DateTime64 & x) const
        {
            return std::hash<DB::DateTime64::NativeType>()(x);
        }
    };

    template <>
    struct hash<DB::Decimal256>
    {
        size_t operator()(const DB::Decimal256 & x) const
        {
            // FIXME temp solution
            return std::hash<DB::Int64>()(static_cast<DB::Int64>(x.value >> 64 & max_uint_mask))
                ^ std::hash<DB::Int64>()(static_cast<DB::Int64>(x.value & max_uint_mask));
        }
    };
}
