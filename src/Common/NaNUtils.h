#pragma once

#include <cmath>
#include <limits>
#include <type_traits>
#include <base/DecomposedFloat.h>


template <typename T>
inline bool isNaN(T x)
{
    /// To be sure, that this function is zero-cost for non-floating point types.
    if constexpr (is_floating_point<T>)
        return DecomposedFloat<T>(x).isNaN();
    else
        return false;
}

template <typename T>
inline bool isFinite(T x)
{
    if constexpr (is_floating_point<T>)
        return DecomposedFloat<T>(x).isFinite();
    else
        return true;
}

template <typename T>
bool canConvertTo(Float64 x)
{
    if constexpr (is_floating_point<T>)
        return true;
    if (!isFinite(x))
        return false;
    /// Use precision-correct float-vs-integer comparison via `DecomposedFloat`.
    /// `Float64(numeric_limits<T>::max())` rounds UP for wide integer types (`size_t` / `UInt64` etc.),
    /// so a naive `x > Float64(...)` would falsely report convertibility for values just above the
    /// upper bound, leading to undefined behavior in subsequent `static_cast<T>(x)`. See issue #103817.
    ///
    /// Bool is special-cased: `numeric_limits<bool>` is exactly representable in `Float64`, and
    /// `DecomposedFloat::compare<bool>` is ill-formed (`make_unsigned_t<bool>` is forbidden).
    if constexpr (std::is_same_v<T, bool>)
    {
        if (x > Float64(std::numeric_limits<T>::max()) || x < Float64(std::numeric_limits<T>::lowest()))
            return false;
    }
    else
    {
        const DecomposedFloat<Float64> x_decomposed(x);
        if (x_decomposed.greater(std::numeric_limits<T>::max())
            || x_decomposed.less(std::numeric_limits<T>::lowest()))
            return false;
    }

    return true;
}

template <typename T>
T NaNOrZero()
{
    if constexpr (std::is_floating_point_v<T>)
        return std::numeric_limits<T>::quiet_NaN();
    if constexpr (std::is_same_v<T, BFloat16>)
        return BFloat16(std::numeric_limits<Float32>::quiet_NaN());
    return {};
}

/// Canonical positive quiet NaN for a floating-point type. For BFloat16 we
/// construct it from the Float32 canonical NaN; for the other floats we use the
/// implementation-defined `std::numeric_limits<T>::quiet_NaN()`.
template <typename T>
inline T canonicalNaN()
{
    static_assert(is_floating_point<T>, "canonicalNaN is only defined for floating-point types");
    if constexpr (std::is_same_v<T, BFloat16>)
        return BFloat16(std::numeric_limits<Float32>::quiet_NaN());
    else
        return std::numeric_limits<T>::quiet_NaN();
}

/// If `x` is a NaN, return the canonical NaN for type `T`. Otherwise return `x`
/// unchanged. For non-floating-point types this is a compile-time no-op.
///
/// Different operations and platforms can produce NaN bit patterns that differ
/// in the sign bit or in the payload (for example glibc's scalar `log(-1.)`
/// versus the ARM NEON fastops `log` after PR #98230, or division by zero
/// versus the literal `nan` in SQL). Hash tables in ClickHouse compare keys
/// byte-wise via `bitEquals`, so two NaNs that differ only in bit pattern are
/// treated as distinct, which breaks `DISTINCT`, `GROUP BY`, `uniqExact`, and
/// related set semantics. Call this helper at the hash/equality boundary to
/// fold every NaN bit pattern onto a single canonical representation.
template <typename T>
inline T canonicalizeNaN(T x)
{
    if constexpr (is_floating_point<T>)
    {
        if (isNaN(x))
            return canonicalNaN<T>();
    }
    return x;
}

template <typename T>
bool signBit(T x)
{
    if constexpr (is_floating_point<T>)
        return DecomposedFloat<T>(x).isNegative();
    else
        return x < 0;
}
