#include "config.h"

#if USE_HIGHWAY

#include <Functions/HighwayMath.h>

#include <gtest/gtest.h>
#include <hwy/targets.h>

#include <cmath>
#include <cstring>
#include <limits>
#include <random>
#include <vector>

/// Checks the vectorized math functions on every instruction set that the CPU supports, not only on the one
/// that the dispatch selects. Functional tests see only the latter.

namespace
{

template <typename T>
using Kernel = void (*)(const T *, size_t, T *);

const char * targetName(int64_t target)
{
    /// Declared with `HWY_MAYBE_UNUSED` in the header.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"
    return hwy::TargetName(target);
#pragma clang diagnostic pop
}

/// Error in units in the last place of the correctly rounded result `ref`.
template <typename T>
double ulpError(T got, long double ref)
{
    T rounded = static_cast<T>(ref);
    if (std::isnan(rounded) || std::isnan(got))
        return std::isnan(rounded) == std::isnan(got) ? 0 : std::numeric_limits<double>::infinity();
    if (std::isinf(rounded) || std::isinf(got))
        return rounded == got ? 0 : std::numeric_limits<double>::infinity();
    T abs = std::abs(rounded);
    T ulp = std::nextafter(abs, std::numeric_limits<T>::infinity()) - abs;
    return static_cast<double>(std::abs(static_cast<long double>(got) - ref) / static_cast<long double>(ulp));
}

long double sigmoidReference(long double x)
{
    return 1.0L / (1.0L + std::exp(-x));
}

template <typename T>
T sigmoidLibm(T x)
{
    T e = std::exp(-std::abs(x));
    return (x < 0 ? e : T(1)) / (T(1) + e);
}

/// Maximum error over random arguments in [lo, hi] (log-uniform when the range spans many orders of magnitude).
/// The function is called in place, as `FunctionMathUnary` does, on an array that is not a multiple of the vector size.
template <typename T>
double maxUlpError(Kernel<T> kernel, long double (*reference)(long double), T lo, T hi)
{
    const size_t size = 10007;
    std::mt19937_64 rng(42);
    const bool log_scale = lo > 0 && static_cast<double>(hi) / static_cast<double>(lo) > 1e6;
    std::vector<T> args(size);
    for (auto & x : args)
    {
        if (log_scale)
            x = static_cast<T>(std::exp(std::uniform_real_distribution<double>(std::log(double(lo)), std::log(double(hi)))(rng)));
        else
            x = static_cast<T>(std::uniform_real_distribution<double>(static_cast<double>(lo), static_cast<double>(hi))(rng));
    }

    std::vector<T> res = args;
    kernel(res.data(), size, res.data());

    double max_error = 0;
    for (size_t i = 0; i < size; ++i)
        max_error = std::max(max_error, ulpError<T>(res[i], reference(static_cast<long double>(args[i]))));
    return max_error;
}

/// Special values give exactly what libm gives: NaN for NaN, the same infinity, the same signed zero.
template <typename T>
void checkSpecialValues(const char * name, Kernel<T> kernel, T (*libm)(T))
{
    constexpr T inf = std::numeric_limits<T>::infinity();
    constexpr T max_finite_exp_arg = sizeof(T) == 8 ? static_cast<T>(709.782712893384) : static_cast<T>(88.72283172607422f);
    const std::vector<T> args = {
        T(0), T(-0.0), std::numeric_limits<T>::denorm_min(), -std::numeric_limits<T>::denorm_min(), std::numeric_limits<T>::min(),
        T(1e-20), T(-1e-20), T(1), T(-1), std::numeric_limits<T>::max(), std::numeric_limits<T>::lowest(), inf, -inf,
        std::numeric_limits<T>::quiet_NaN(), -std::numeric_limits<T>::quiet_NaN(), max_finite_exp_arg,
        std::nextafter(max_finite_exp_arg, inf), T(-745.1), T(-746), T(-103.9), T(-104)};

    std::vector<T> res(args.size());
    kernel(args.data(), args.size(), res.data());

    for (size_t i = 0; i < args.size(); ++i)
    {
        T expected = libm(args[i]);
        if (std::isnan(expected))
            EXPECT_TRUE(std::isnan(res[i])) << name << "(" << args[i] << ") = " << res[i];
        else if (std::isinf(expected) || expected == 0)
            EXPECT_TRUE(res[i] == expected && std::signbit(res[i]) == std::signbit(expected))
                << name << "(" << args[i] << ") = " << res[i] << ", expected " << expected;
        else
            EXPECT_LE(ulpError<T>(res[i], static_cast<long double>(expected)), 4) << name << "(" << args[i] << ") = " << res[i] << ", expected " << expected;
    }
}

uint64_t splitmix64(uint64_t & state)
{
    uint64_t z = (state += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/// Uniform in [-1, 1), computed exactly, without any library function.
double uniformMinusOneToOne(uint64_t & state)
{
    return static_cast<double>(splitmix64(state) >> 11) * 0x1.0p-52 - 1.0;
}

/// Arguments that cover the whole domain, generated identically on every platform: a third in [-750, 750]
/// (Float32: [-105, 105]), a third positive finite numbers with random bits (including subnormals), a third in [-20, 20].
template <typename T>
std::vector<T> platformIndependentArguments(size_t size)
{
    uint64_t state = 12345;
    std::vector<T> res(size);
    for (size_t i = 0; i < size; ++i)
    {
        if (i % 3 == 0)
            res[i] = static_cast<T>(uniformMinusOneToOne(state) * (sizeof(T) == 8 ? 750.0 : 105.0));
        else if (i % 3 == 1)
        {
            uint64_t bits = splitmix64(state);
            if constexpr (sizeof(T) == 8)
            {
                bits &= 0x7FEFFFFFFFFFFFFFULL;
                std::memcpy(&res[i], &bits, sizeof(T));
            }
            else
            {
                uint32_t bits32 = static_cast<uint32_t>(bits) & 0x7F7FFFFFU;
                std::memcpy(&res[i], &bits32, sizeof(T));
            }
        }
        else
            res[i] = static_cast<T>(uniformMinusOneToOne(state) * 20.0);
    }
    return res;
}

/// FNV-1a over the bits of the results.
template <typename T>
uint64_t resultHash(Kernel<T> kernel, const std::vector<T> & args)
{
    std::vector<T> res(args.size());
    kernel(args.data(), args.size(), res.data());
    uint64_t hash = 1469598103934665603ULL;
    for (T value : res)
    {
        uint64_t bits = 0;
        std::memcpy(&bits, &value, sizeof(T));
        hash = (hash ^ bits) * 1099511628211ULL;
    }
    return hash;
}

/// Arrays of every length up to a few vectors: the tail is processed, and nothing after the end is written.
template <typename T>
void checkTails(const char * name, Kernel<T> kernel, T (*libm)(T))
{
    for (size_t size = 0; size <= 40; ++size)
    {
        std::vector<T> args(size), res(size + 1, T(12345));
        for (size_t i = 0; i < size; ++i)
            args[i] = T(0.25) * T(i) - T(2);
        kernel(args.data(), size, res.data());
        for (size_t i = 0; i < size; ++i)
            EXPECT_LE(ulpError<T>(res[i], static_cast<long double>(libm(args[i]))), 8) << name << " size " << size << " row " << i;
        EXPECT_EQ(res[size], T(12345)) << name << " wrote past the end for size " << size;
    }
}

}

TEST(HighwayMath, AllTargets)
{
    using namespace DB;

    Kernel<double> exp64 = HighwayMath::exp;
    Kernel<double> log64 = HighwayMath::log;
    Kernel<double> tanh64 = HighwayMath::tanh;
    Kernel<double> sigmoid64 = HighwayMath::sigmoid;
    Kernel<float> exp32 = HighwayMath::exp;
    Kernel<float> log32 = HighwayMath::log;
    Kernel<float> tanh32 = HighwayMath::tanh;
    Kernel<float> sigmoid32 = HighwayMath::sigmoid;

    long double (*expl_ref)(long double) = std::exp;
    long double (*logl_ref)(long double) = std::log;
    long double (*tanhl_ref)(long double) = std::tanh;
    double (*exp_libm)(double) = std::exp;
    double (*log_libm)(double) = std::log;
    double (*tanh_libm)(double) = std::tanh;
    float (*expf_libm)(float) = std::exp;
    float (*logf_libm)(float) = std::log;
    float (*tanhf_libm)(float) = std::tanh;

    for (int64_t target : hwy::SupportedAndGeneratedTargets())
    {
        hwy::SetSupportedTargetsForTest(target);
        SCOPED_TRACE(targetName(target));

        /// Float64, including the overflow and underflow boundaries and subnormals.
        EXPECT_LE(maxUlpError<double>(exp64, expl_ref, -700, 700), 2);
        EXPECT_LE(maxUlpError<double>(exp64, expl_ref, 700, 709.782712893384), 2);
        EXPECT_LE(maxUlpError<double>(exp64, expl_ref, -745.13, -700), 2);
        EXPECT_LE(maxUlpError<double>(log64, logl_ref, 4.9e-324, 2.2e-308), 2);
        EXPECT_LE(maxUlpError<double>(log64, logl_ref, 2.2e-308, 1.7e308), 2);
        EXPECT_LE(maxUlpError<double>(log64, logl_ref, 0.999, 1.001), 2);
        EXPECT_LE(maxUlpError<double>(tanh64, tanhl_ref, -20, 20), 4);
        EXPECT_LE(maxUlpError<double>(tanh64, tanhl_ref, 1e-300, 1e-5), 4);
        EXPECT_LE(maxUlpError<double>(sigmoid64, sigmoidReference, -40, 40), 3);
        EXPECT_LE(maxUlpError<double>(sigmoid64, sigmoidReference, -745, -700), 3);

        /// Float32.
        EXPECT_LE(maxUlpError<float>(exp32, expl_ref, -87, 88.72283172607422f), 2);
        EXPECT_LE(maxUlpError<float>(exp32, expl_ref, -103.9f, -87), 2);
        EXPECT_LE(maxUlpError<float>(log32, logl_ref, 1.4e-45f, 3.4e38f), 2);
        EXPECT_LE(maxUlpError<float>(tanh32, tanhl_ref, -10, 10), 4);
        EXPECT_LE(maxUlpError<float>(sigmoid32, sigmoidReference, -103, 87), 3);

        checkSpecialValues<double>("exp", exp64, exp_libm);
        checkSpecialValues<double>("log", log64, log_libm);
        checkSpecialValues<double>("tanh", tanh64, tanh_libm);
        checkSpecialValues<double>("sigmoid", sigmoid64, sigmoidLibm<double>);
        checkSpecialValues<float>("exp", exp32, expf_libm);
        checkSpecialValues<float>("log", log32, logf_libm);
        checkSpecialValues<float>("tanh", tanh32, tanhf_libm);
        checkSpecialValues<float>("sigmoid", sigmoid32, sigmoidLibm<float>);

        checkTails<double>("exp", exp64, exp_libm);
        checkTails<float>("tanh", tanh32, tanhf_libm);

        /// Exact results that users rely on.
        double zero = 0;
        double one = 1;
        double res = 0;
        exp64(&zero, 1, &res);
        EXPECT_EQ(res, 1.0);
        log64(&one, 1, &res);
        EXPECT_EQ(res, 0.0);
        sigmoid64(&zero, 1, &res);
        EXPECT_EQ(res, 0.5);
    }

    hwy::SetSupportedTargetsForTest(0);
}

/// The results do not depend on the CPU: every instruction set with fused multiply-add gives the same bits, on x86_64
/// and on aarch64, so that a query returns the same digits everywhere (the scalar math functions are correctly rounded
/// for the same reason). The hashes were computed on aarch64.
/// The instruction sets without FMA (below x86-64-v3) round differently and are used only by builds for old CPUs.
TEST(HighwayMath, SameResultsOnEveryCPU)
{
    using namespace DB;

    const auto args64 = platformIndependentArguments<double>(30000);
    const auto args32 = platformIndependentArguments<float>(30000);

    Kernel<double> exp64 = HighwayMath::exp;
    Kernel<double> log64 = HighwayMath::log;
    Kernel<double> tanh64 = HighwayMath::tanh;
    Kernel<double> sigmoid64 = HighwayMath::sigmoid;
    Kernel<float> exp32 = HighwayMath::exp;
    Kernel<float> log32 = HighwayMath::log;
    Kernel<float> tanh32 = HighwayMath::tanh;
    Kernel<float> sigmoid32 = HighwayMath::sigmoid;

    constexpr int64_t targets_without_fma = HWY_SSE2 | HWY_SSSE3 | HWY_SSE4 | HWY_EMU128 | HWY_SCALAR;

    for (int64_t target : hwy::SupportedAndGeneratedTargets())
    {
        if (target & targets_without_fma)
            continue;

        hwy::SetSupportedTargetsForTest(target);
        SCOPED_TRACE(targetName(target));

        EXPECT_EQ(resultHash(exp64, args64), 0x163ee75cdd137a2cULL);
        EXPECT_EQ(resultHash(log64, args64), 0x1170228c69a15295ULL);
        EXPECT_EQ(resultHash(tanh64, args64), 0x4e525a23407f485fULL);
        EXPECT_EQ(resultHash(sigmoid64, args64), 0x76f31a95bf158d59ULL);
        EXPECT_EQ(resultHash(exp32, args32), 0x995784a27fce53e8ULL);
        EXPECT_EQ(resultHash(log32, args32), 0x9d2e7a6d4f507dbcULL);
        EXPECT_EQ(resultHash(tanh32, args32), 0xb6ebb597c1cb19a5ULL);
        EXPECT_EQ(resultHash(sigmoid32, args32), 0x36f6dae848409837ULL);
    }

    hwy::SetSupportedTargetsForTest(0);
}

#endif
