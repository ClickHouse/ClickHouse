#include <Functions/HighwayMath.h>

#if USE_HIGHWAY

/// Highway declares many of its functions with `HWY_MAYBE_UNUSED`.
#pragma clang diagnostic ignored "-Wused-but-marked-unused"

/// Compile the code below once for every target enabled in `contrib/highway-cmake`, and dispatch at runtime.
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "Functions/HighwayMath.cpp" // NOLINT(bugprone-suspicious-include)
#include <hwy/foreach_target.h> // IWYU pragma: keep
#include <hwy/highway.h>
#include <hwy/contrib/math/math-inl.h>

HWY_BEFORE_NAMESPACE();
namespace DB::HighwayMath::HWY_NAMESPACE
{

namespace hn = hwy::HWY_NAMESPACE;

/// Highway's functions are only defined for finite arguments in their domain,
/// so the special values are patched to match libm.

template <class D, class V>
HWY_INLINE V expImpl(D d, V x)
{
    using T = hn::TFromD<D>;
    /// The largest argument with a finite result. `hn::Exp` gives NaN for larger arguments and +inf.
    constexpr T max_finite_arg = sizeof(T) == 8 ? static_cast<T>(709.782712893384) : static_cast<T>(88.72283172607422f);

    V res = hn::Exp(d, x);
    res = hn::IfThenElse(hn::Gt(x, hn::Set(d, max_finite_arg)), hn::Inf(d), res);
    /// `hn::Exp` gives 0 for NaN.
    return hn::IfThenElse(hn::IsNaN(x), x, res);
}

template <class D, class V>
HWY_INLINE V logImpl(D d, V x)
{
    V res = hn::Log(d, x);
    res = hn::IfThenElse(hn::Lt(x, hn::Zero(d)), hn::NaN(d), res);
    /// Both zeros.
    res = hn::IfThenElse(hn::Eq(x, hn::Zero(d)), hn::Neg(hn::Inf(d)), res);
    /// log(+inf) = +inf, log(NaN) = NaN.
    return hn::IfThenElse(hn::Or(hn::IsNaN(x), hn::Eq(x, hn::Inf(d))), x, res);
}

template <class D, class V>
HWY_INLINE V tanhImpl(D d, V x)
{
    /// `hn::Tanh` handles infinities and NaN.
    return hn::Tanh(d, x);
}

template <class D, class V>
HWY_INLINE V sigmoidImpl(D d, V x)
{
    /// With e = exp(-|x|): 1 / (1 + e) for x >= 0 and e / (1 + e) for x < 0.
    /// Unlike 1 / (1 + exp(-x)), this does not overflow to 0 for large negative x, where the result is subnormal.
    const V one = hn::Set(d, static_cast<hn::TFromD<D>>(1));
    const V e = hn::Exp(d, hn::Neg(hn::Abs(x)));
    const V res = hn::Div(hn::IfThenElse(hn::Lt(x, hn::Zero(d)), e, one), hn::Add(one, e));
    /// `hn::Exp` gives 0 for NaN.
    return hn::IfThenElse(hn::IsNaN(x), x, res);
}

/// Applies `F` to an array. No `HWY_RESTRICT`: `src` and `dst` may be the same array.
template <typename T, typename F>
HWY_INLINE void apply(const T * src, size_t size, T * dst, F && f)
{
    const hn::ScalableTag<T> d;
    const size_t lanes = hn::Lanes(d);

    size_t i = 0;
    for (; i + lanes <= size; i += lanes)
        hn::StoreU(f(d, hn::LoadU(d, src + i)), d, dst + i);

    if (i < size)
        hn::StoreN(f(d, hn::LoadN(d, src + i, size - i)), d, dst + i, size - i);
}

static void expFloat32(const float * src, size_t size, float * dst)
{
    apply(src, size, dst, [](auto d, auto x) HWY_ATTR { return expImpl(d, x); });
}

static void expFloat64(const double * src, size_t size, double * dst)
{
    apply(src, size, dst, [](auto d, auto x) HWY_ATTR { return expImpl(d, x); });
}

static void logFloat32(const float * src, size_t size, float * dst)
{
    apply(src, size, dst, [](auto d, auto x) HWY_ATTR { return logImpl(d, x); });
}

static void logFloat64(const double * src, size_t size, double * dst)
{
    apply(src, size, dst, [](auto d, auto x) HWY_ATTR { return logImpl(d, x); });
}

static void tanhFloat32(const float * src, size_t size, float * dst)
{
    apply(src, size, dst, [](auto d, auto x) HWY_ATTR { return tanhImpl(d, x); });
}

static void tanhFloat64(const double * src, size_t size, double * dst)
{
    apply(src, size, dst, [](auto d, auto x) HWY_ATTR { return tanhImpl(d, x); });
}

static void sigmoidFloat32(const float * src, size_t size, float * dst)
{
    apply(src, size, dst, [](auto d, auto x) HWY_ATTR { return sigmoidImpl(d, x); });
}

static void sigmoidFloat64(const double * src, size_t size, double * dst)
{
    apply(src, size, dst, [](auto d, auto x) HWY_ATTR { return sigmoidImpl(d, x); });
}

}
HWY_AFTER_NAMESPACE();

#if HWY_ONCE

namespace DB::HighwayMath
{

HWY_EXPORT(expFloat32);
HWY_EXPORT(expFloat64);
HWY_EXPORT(logFloat32);
HWY_EXPORT(logFloat64);
HWY_EXPORT(tanhFloat32);
HWY_EXPORT(tanhFloat64);
HWY_EXPORT(sigmoidFloat32);
HWY_EXPORT(sigmoidFloat64);

void exp(const float * src, size_t size, float * dst) { HWY_DYNAMIC_DISPATCH(expFloat32)(src, size, dst); }
void exp(const double * src, size_t size, double * dst) { HWY_DYNAMIC_DISPATCH(expFloat64)(src, size, dst); }
void log(const float * src, size_t size, float * dst) { HWY_DYNAMIC_DISPATCH(logFloat32)(src, size, dst); }
void log(const double * src, size_t size, double * dst) { HWY_DYNAMIC_DISPATCH(logFloat64)(src, size, dst); }
void tanh(const float * src, size_t size, float * dst) { HWY_DYNAMIC_DISPATCH(tanhFloat32)(src, size, dst); }
void tanh(const double * src, size_t size, double * dst) { HWY_DYNAMIC_DISPATCH(tanhFloat64)(src, size, dst); }
void sigmoid(const float * src, size_t size, float * dst) { HWY_DYNAMIC_DISPATCH(sigmoidFloat32)(src, size, dst); }
void sigmoid(const double * src, size_t size, double * dst) { HWY_DYNAMIC_DISPATCH(sigmoidFloat64)(src, size, dst); }

}

#endif

#endif
