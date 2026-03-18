#pragma once

#include <base/extended_types.h>
#include <base/Decimal_fwd.h>
#include <base/types.h>
#include <base/defines.h>


namespace DB
{
template <class> struct Decimal;
class DateTime64;

#define FOR_EACH_UNDERLYING_DECIMAL_TYPE(M) \
    M(Int32) \
    M(Int64) \
    M(Int128) \
    M(Int256)

#define FOR_EACH_UNDERLYING_DECIMAL_TYPE_PASS(M, X) \
    M(Int32,  X) \
    M(Int64,  X) \
    M(Int128, X) \
    M(Int256, X)

using Decimal32 = Decimal<Int32>;
using Decimal64 = Decimal<Int64>;
using Decimal128 = Decimal<Int128>;
using Decimal256 = Decimal<Int256>;

template <class T> struct NativeTypeT { using Type = T; };
template <is_decimal T> struct NativeTypeT<T> { using Type = typename T::NativeType; };
template <class T> using NativeType = typename NativeTypeT<T>::Type;

/// Own FieldType for Decimal.
/// It is only a "storage" for decimal.
/// To perform operations, you also have to provide a scale (number of digits after point).
template <typename T>
struct Decimal
{
    using NativeType = T;

    constexpr Decimal() = default;
    constexpr Decimal(Decimal<T> &&) noexcept = default;
    constexpr Decimal(const Decimal<T> &) = default;

    constexpr Decimal(const T & value_): value(value_) {} // NOLINT(google-explicit-constructor)

    template <typename U>
    constexpr Decimal(const Decimal<U> & x): value(x.value) {} // NOLINT(google-explicit-constructor)

    constexpr Decimal<T> & operator=(Decimal<T> &&) noexcept = default;
    constexpr Decimal<T> & operator = (const Decimal<T> &) = default;

    constexpr operator T () const { return value; } // NOLINT(google-explicit-constructor)

    template <typename U>
    constexpr U convertTo() const
    {
        if constexpr (is_decimal<U>)
            return convertTo<typename U::NativeType>();
        else
            return static_cast<U>(value);
    }

    const Decimal<T> & operator += (const T & x);
    const Decimal<T> & operator -= (const T & x);
    const Decimal<T> & operator *= (const T & x);
    const Decimal<T> & operator /= (const T & x);
    const Decimal<T> & operator %= (const T & x);

    template <typename U> const Decimal<T> & operator += (const Decimal<U> & x);
    template <typename U> const Decimal<T> & operator -= (const Decimal<U> & x);
    template <typename U> const Decimal<T> & operator *= (const Decimal<U> & x);
    template <typename U> const Decimal<T> & operator /= (const Decimal<U> & x);
    template <typename U> const Decimal<T> & operator %= (const Decimal<U> & x);

    /// This is to avoid UB for sumWithOverflow()
    void NO_SANITIZE_UNDEFINED addOverflow(const T & x);

    T value;
};

#define DISPATCH(TYPE) extern template struct Decimal<TYPE>;
FOR_EACH_UNDERLYING_DECIMAL_TYPE(DISPATCH)
#undef DISPATCH

#define DISPATCH(TYPE_T, TYPE_U) \
    extern template const Decimal<TYPE_T> & Decimal<TYPE_T>::operator += (const Decimal<TYPE_U> & x); \
    extern template const Decimal<TYPE_T> & Decimal<TYPE_T>::operator -= (const Decimal<TYPE_U> & x); \
    extern template const Decimal<TYPE_T> & Decimal<TYPE_T>::operator *= (const Decimal<TYPE_U> & x); \
    extern template const Decimal<TYPE_T> & Decimal<TYPE_T>::operator /= (const Decimal<TYPE_U> & x); \
    extern template const Decimal<TYPE_T> & Decimal<TYPE_T>::operator %= (const Decimal<TYPE_U> & x);
#define INVOKE(X) FOR_EACH_UNDERLYING_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_UNDERLYING_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH

template <typename T> bool operator< (const Decimal<T> & x, const Decimal<T> & y);
template <typename T> bool operator> (const Decimal<T> & x, const Decimal<T> & y);
template <typename T> bool operator<= (const Decimal<T> & x, const Decimal<T> & y);
template <typename T> bool operator>= (const Decimal<T> & x, const Decimal<T> & y);
template <typename T> bool operator== (const Decimal<T> & x, const Decimal<T> & y);
template <typename T> bool operator!= (const Decimal<T> & x, const Decimal<T> & y);

#define DISPATCH(TYPE) \
extern template bool operator< (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
extern template bool operator> (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
extern template bool operator<= (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
extern template bool operator>= (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
extern template bool operator== (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
extern template bool operator!= (const Decimal<TYPE> & x, const Decimal<TYPE> & y);
FOR_EACH_UNDERLYING_DECIMAL_TYPE(DISPATCH)
#undef DISPATCH

template <typename T> Decimal<T> operator+ (const Decimal<T> & x, const Decimal<T> & y);
template <typename T> Decimal<T> operator- (const Decimal<T> & x, const Decimal<T> & y);
template <typename T> Decimal<T> operator* (const Decimal<T> & x, const Decimal<T> & y);
template <typename T> Decimal<T> operator/ (const Decimal<T> & x, const Decimal<T> & y);
template <typename T> Decimal<T> operator- (const Decimal<T> & x);

#define DISPATCH(TYPE) \
extern template Decimal<TYPE> operator+ (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
extern template Decimal<TYPE> operator- (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
extern template Decimal<TYPE> operator* (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
extern template Decimal<TYPE> operator/ (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
extern template Decimal<TYPE> operator- (const Decimal<TYPE> & x);
FOR_EACH_UNDERLYING_DECIMAL_TYPE(DISPATCH)
#undef DISPATCH

#undef FOR_EACH_UNDERLYING_DECIMAL_TYPE_PASS
#undef FOR_EACH_UNDERLYING_DECIMAL_TYPE

/// Distinguishable type to allow function resolution/deduction based on value type,
/// but also relatively easy to convert to/from Decimal64.
class DateTime64 : public Decimal64
{
public:
    using Base = Decimal64;
    using Base::Base;
    using NativeType = Base::NativeType;

    constexpr DateTime64(const Base & v): Base(v) {} // NOLINT(google-explicit-constructor)
};
}

constexpr UInt64 max_uint_mask = std::numeric_limits<UInt64>::max();

namespace std
{
    template <typename T>
    struct hash<DB::Decimal<T>>
    {
        size_t operator()(const DB::Decimal<T> & x) const { return hash<T>()(x.value); }
    };

    template <>
    struct hash<DB::Decimal128>
    {
        size_t operator()(const DB::Decimal128 & x) const
        {
            return std::hash<Int64>()(x.value >> 64)
                ^ std::hash<Int64>()(x.value & max_uint_mask);
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
            return std::hash<Int64>()(static_cast<Int64>(x.value >> 64 & max_uint_mask))
                ^ std::hash<Int64>()(static_cast<Int64>(x.value & max_uint_mask));
        }
    };
}
