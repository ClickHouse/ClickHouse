#pragma once

#include <base/types.h>

#include <bit>
#include <cmath>

namespace DB
{

/// Auto-vectorizable sin/cos/tan kernels.
///
/// No intrinsics and no runtime dispatch: the loops are written so that clang vectorizes them with
/// whatever SIMD width the target has. Each element goes through a Cody-Waite reduction to a
/// quadrant of pi/2 and then through the Cephes minimax polynomials for sin and cos on
/// [-pi/4, pi/4] (Cephes Math Library, Stephen L. Moshier, `sin.c`). Both polynomials are
/// evaluated for every element and the quadrant selects between them, keeping the loop branch-free.
///
/// The reduction constant pi/2 is split into four parts, the first three having at most 27 significant
/// bits, so that `q * part` is exact for |q| < 2^26, and each subtraction in the chain is exact until
/// it reaches the scale of the reduced argument. The reduced argument is therefore accurate to ~1 ulp
/// of its own magnitude even when it cancels almost completely, i.e. near the zeros of sin/cos and the
/// poles of tan for large |x|, where a shorter split leaves an absolute error of ~1e-30 * q that
/// dominates the result. Inputs beyond `fast_trig_limit` (and NaN/Inf) are recomputed with libm in a
/// second, scalar pass. Inside the limit the result is accurate to ~2 ulp for sin/cos and ~4 ulp for
/// tan (measured against libm over the whole range, including inputs within a few ulp of multiples
/// of pi/2).
namespace FastTrig
{

inline constexpr double fast_trig_limit = 1e8;

/// pi/2 = dp1 + dp2 + dp3 + dp4 to ~142 bits (glibc `sysdeps/ieee754/dbl-64/usncs.h`: `mp1`, `mp2`, `pp3`, `pp4`).
/// `fast_trig_limit` keeps the quadrant number below 2^26, which the exactness of the first three products relies on.
inline constexpr double dp1 = 0x1.921FB58p0;
inline constexpr double dp2 = -0x1.DDE973Cp-27;
inline constexpr double dp3 = -0x1.CB3B398p-55;
inline constexpr double dp4 = -0x1.d747f23e32ed7p-83;
inline constexpr double two_over_pi = 6.36619772367581343076E-1;

/// 1.5 * 2^52: adding it rounds to the nearest integer and leaves that integer in the low mantissa
/// bits (modulo 2^51, in two's complement), which gives the quadrant with plain bit operations.
inline constexpr double round_magic = 6755399441055744.0;

struct SinCos
{
    double sin;
    double cos;
    Int64 quadrant;
};

/// sin and cos of `x` reduced to [-pi/4, pi/4], plus the quadrant number. Requires |x| <= fast_trig_limit.
inline SinCos reduceAndEvaluate(double x)
{
    double t = x * two_over_pi + round_magic;
    Int64 q = std::bit_cast<Int64>(t);
    double qd = t - round_magic;

    double r = x - qd * dp1;
    r -= qd * dp2;
    r -= qd * dp3;
    r -= qd * dp4;

    double r2 = r * r;

    double s = 1.58962301576546568060E-10;
    s = s * r2 - 2.50507477628578072866E-8;
    s = s * r2 + 2.75573136213857245213E-6;
    s = s * r2 - 1.98412698295895385996E-4;
    s = s * r2 + 8.33333333332211858878E-3;
    s = s * r2 - 1.66666666666666307295E-1;
    s = s * r2 * r + r;

    double c = -1.13585365213876817300E-11;
    c = c * r2 + 2.08757008419747316778E-9;
    c = c * r2 - 2.75573141792967388112E-7;
    c = c * r2 + 2.48015872888517045348E-5;
    c = c * r2 - 1.38888888888730564116E-3;
    c = c * r2 + 4.16666666666665929218E-2;
    c = c * r2 * r2 - 0.5 * r2 + 1.0;

    return {s, c, q};
}

inline bool inRange(double x)
{
    return std::fabs(x) <= fast_trig_limit;
}

inline void sin(const double * __restrict src, size_t size, double * __restrict dst)
{
    for (size_t i = 0; i < size; ++i)
    {
        double x = inRange(src[i]) ? src[i] : 0.0;
        auto [s, c, q] = reduceAndEvaluate(x);
        double res = (q & 1) ? c : s;
        res = (q & 2) ? -res : res;
        /// Keep the sign of a zero argument (sin(-0) = -0), which the polynomial loses.
        dst[i] = (x == 0.0) ? x : res;
    }
    for (size_t i = 0; i < size; ++i)
        if (!inRange(src[i]))
            dst[i] = std::sin(src[i]);
}

inline void cos(const double * __restrict src, size_t size, double * __restrict dst)
{
    for (size_t i = 0; i < size; ++i)
    {
        double x = inRange(src[i]) ? src[i] : 0.0;
        auto [s, c, q] = reduceAndEvaluate(x);
        double res = (q & 1) ? s : c;
        dst[i] = ((q + 1) & 2) ? -res : res;
    }
    for (size_t i = 0; i < size; ++i)
        if (!inRange(src[i]))
            dst[i] = std::cos(src[i]);
}

inline void tan(const double * __restrict src, size_t size, double * __restrict dst)
{
    for (size_t i = 0; i < size; ++i)
    {
        double x = inRange(src[i]) ? src[i] : 0.0;
        auto [s, c, q] = reduceAndEvaluate(x);
        double res = (q & 1) ? -c / s : s / c;
        dst[i] = (x == 0.0) ? x : res;
    }
    for (size_t i = 0; i < size; ++i)
        if (!inRange(src[i]))
            dst[i] = std::tan(src[i]);
}

}

}
