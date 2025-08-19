#include <base/Decimal.h>
#include <base/extended_types.h>

namespace DB
{

/// Explicit template instantiations.

#define FOR_EACH_UNDERLYING_DECIMAL_TYPE(M) \
    M(Int32)  \
    M(Int64)  \
    M(Int128) \
    M(Int256)

#define FOR_EACH_UNDERLYING_DECIMAL_TYPE_PASS(M, X) \
    M(Int32, X) \
    M(Int64, X) \
    M(Int128, X) \
    M(Int256, X)

template <typename T> const Decimal<T> & Decimal<T>::operator += (const T & x) { value += x; return *this; }
template <typename T> const Decimal<T> & Decimal<T>::operator -= (const T & x) { value -= x; return *this; }
template <typename T> const Decimal<T> & Decimal<T>::operator *= (const T & x) { value *= x; return *this; }
template <typename T> const Decimal<T> & Decimal<T>::operator /= (const T & x) { value /= x; return *this; }
template <typename T> const Decimal<T> & Decimal<T>::operator %= (const T & x) { value %= x; return *this; }

template <typename T> void NO_SANITIZE_UNDEFINED Decimal<T>::addOverflow(const T & x) { value += x; }

/// Maybe this explicit instantiation affects performance since operators cannot be inlined.

template <typename T> template <typename U> const Decimal<T> & Decimal<T>::operator += (const Decimal<U> & x) { value += static_cast<T>(x.value); return *this; }
template <typename T> template <typename U> const Decimal<T> & Decimal<T>::operator -= (const Decimal<U> & x) { value -= static_cast<T>(x.value); return *this; }
template <typename T> template <typename U> const Decimal<T> & Decimal<T>::operator *= (const Decimal<U> & x) { value *= static_cast<T>(x.value); return *this; }
template <typename T> template <typename U> const Decimal<T> & Decimal<T>::operator /= (const Decimal<U> & x) { value /= static_cast<T>(x.value); return *this; }
template <typename T> template <typename U> const Decimal<T> & Decimal<T>::operator %= (const Decimal<U> & x) { value %= static_cast<T>(x.value); return *this; }

#define DISPATCH(TYPE_T, TYPE_U) \
    template const Decimal<TYPE_T> & Decimal<TYPE_T>::operator += (const Decimal<TYPE_U> & x); \
    template const Decimal<TYPE_T> & Decimal<TYPE_T>::operator -= (const Decimal<TYPE_U> & x); \
    template const Decimal<TYPE_T> & Decimal<TYPE_T>::operator *= (const Decimal<TYPE_U> & x); \
    template const Decimal<TYPE_T> & Decimal<TYPE_T>::operator /= (const Decimal<TYPE_U> & x); \
    template const Decimal<TYPE_T> & Decimal<TYPE_T>::operator %= (const Decimal<TYPE_U> & x);
#define INVOKE(X) FOR_EACH_UNDERLYING_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_UNDERLYING_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH

#define DISPATCH(TYPE) template struct Decimal<TYPE>;
FOR_EACH_UNDERLYING_DECIMAL_TYPE(DISPATCH)
#undef DISPATCH

template <typename T> bool operator< (const Decimal<T> & x, const Decimal<T> & y) { return x.value < y.value; }
template <typename T> bool operator> (const Decimal<T> & x, const Decimal<T> & y) { return x.value > y.value; }
template <typename T> bool operator<= (const Decimal<T> & x, const Decimal<T> & y) { return x.value <= y.value; }
template <typename T> bool operator>= (const Decimal<T> & x, const Decimal<T> & y) { return x.value >= y.value; }
template <typename T> bool operator== (const Decimal<T> & x, const Decimal<T> & y) { return x.value == y.value; }
template <typename T> bool operator!= (const Decimal<T> & x, const Decimal<T> & y) { return x.value != y.value; }

#define DISPATCH(TYPE) \
template bool operator< (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
template bool operator> (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
template bool operator<= (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
template bool operator>= (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
template bool operator== (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
template bool operator!= (const Decimal<TYPE> & x, const Decimal<TYPE> & y);
FOR_EACH_UNDERLYING_DECIMAL_TYPE(DISPATCH)
#undef DISPATCH


template <typename T> Decimal<T> operator+ (const Decimal<T> & x, const Decimal<T> & y) { return x.value + y.value; }
template <typename T> Decimal<T> operator- (const Decimal<T> & x, const Decimal<T> & y) { return x.value - y.value; }
template <typename T> Decimal<T> operator* (const Decimal<T> & x, const Decimal<T> & y) { return x.value * y.value; }
template <typename T> Decimal<T> operator/ (const Decimal<T> & x, const Decimal<T> & y) { return x.value / y.value; }
template <typename T> Decimal<T> operator- (const Decimal<T> & x) { return -x.value; }

#define DISPATCH(TYPE) \
template Decimal<TYPE> operator+ (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
template Decimal<TYPE> operator- (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
template Decimal<TYPE> operator* (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
template Decimal<TYPE> operator/ (const Decimal<TYPE> & x, const Decimal<TYPE> & y); \
template Decimal<TYPE> operator- (const Decimal<TYPE> & x);
FOR_EACH_UNDERLYING_DECIMAL_TYPE(DISPATCH)
#undef DISPATCH

#undef FOR_EACH_UNDERLYING_DECIMAL_TYPE_PASS
#undef FOR_EACH_UNDERLYING_DECIMAL_TYPE
}
