#pragma once

#include <Common/TargetSpecific.h>
#include <Core/Types.h>

#include <pcg_random.hpp>

#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#endif

#include <algorithm>
#include <cmath>
#include <cstring>
#include <numbers>
#include <numeric>
#include <random>
#include <vector>


namespace DB
{

/// Own Box-Muller: `std::normal_distribution` is not guaranteed identical across libstdc++/libc++.
template <typename T>
T boxMullerNormal(pcg64 & rng)
{
    std::uniform_real_distribution<T> dist(std::numeric_limits<T>::min(), T(1));
    T u1 = dist(rng);
    T u2 = dist(rng);
    return std::sqrt(T(-2) * std::log(u1)) * std::cos(T(2) * std::numbers::pi_v<T> * u2);
}

/// In-place fast Walsh-Hadamard transform; `n` must be a power of two.
template <typename T>
void fwht(T * data, size_t n)
{
    for (size_t len = 1; len < n; len <<= 1)
    {
        for (size_t i = 0; i < n; i += len << 1)
        {
            for (size_t j = 0; j < len; ++j)
            {
                T u = data[i + j];
                T v = data[i + j + len];
                data[i + j] = u + v;
                data[i + j + len] = u - v;
            }
        }
    }
}

inline size_t nextPowerOfTwo(size_t n)
{
    if (n == 0)
        return 1;
    --n;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    return n + 1;
}

template <typename T>
T dotProductScalar(const T * __restrict a, const T * __restrict b, size_t n)
{
    T sum = T(0);
    for (size_t i = 0; i < n; ++i)
        sum += a[i] * b[i];
    return sum;
}

#if USE_MULTITARGET_CODE
template <typename T>
X86_64_V3_FUNCTION_SPECIFIC_ATTRIBUTE T dotProductAVX2(const T * __restrict a, const T * __restrict b, size_t n)
{
    size_t i = 0;
    if constexpr (std::is_same_v<T, Float32>)
    {
        __m256 acc = _mm256_setzero_ps();
        for (; i + 8 <= n; i += 8)
        {
            __m256 va = _mm256_loadu_ps(a + i);
            __m256 vb = _mm256_loadu_ps(b + i);
            acc = _mm256_fmadd_ps(va, vb, acc);
        }
        __m128 hi = _mm256_extractf128_ps(acc, 1);
        __m128 lo = _mm256_castps256_ps128(acc);
        __m128 sum128 = _mm_add_ps(lo, hi);
        sum128 = _mm_hadd_ps(sum128, sum128);
        sum128 = _mm_hadd_ps(sum128, sum128);
        T result = _mm_cvtss_f32(sum128);
        for (; i < n; ++i)
            result += a[i] * b[i];
        return result;
    }
    else
    {
        __m256d acc = _mm256_setzero_pd();
        for (; i + 4 <= n; i += 4)
        {
            __m256d va = _mm256_loadu_pd(a + i);
            __m256d vb = _mm256_loadu_pd(b + i);
            acc = _mm256_fmadd_pd(va, vb, acc);
        }
        __m128d hi = _mm256_extractf128_pd(acc, 1);
        __m128d lo = _mm256_castpd256_pd128(acc);
        __m128d sum128 = _mm_add_pd(lo, hi);
        T result = _mm_cvtsd_f64(sum128) + _mm_cvtsd_f64(_mm_unpackhi_pd(sum128, sum128));
        for (; i < n; ++i)
            result += a[i] * b[i];
        return result;
    }
}

template <typename T>
X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE T dotProductAVX512(const T * __restrict a, const T * __restrict b, size_t n)
{
    size_t i = 0;
    if constexpr (std::is_same_v<T, Float32>)
    {
        __m512 acc = _mm512_setzero_ps();
        for (; i + 16 <= n; i += 16)
        {
            __m512 va = _mm512_loadu_ps(a + i);
            __m512 vb = _mm512_loadu_ps(b + i);
            acc = _mm512_fmadd_ps(va, vb, acc);
        }
        T result = _mm512_reduce_add_ps(acc);
        for (; i < n; ++i)
            result += a[i] * b[i];
        return result;
    }
    else
    {
        __m512d acc = _mm512_setzero_pd();
        for (; i + 8 <= n; i += 8)
        {
            __m512d va = _mm512_loadu_pd(a + i);
            __m512d vb = _mm512_loadu_pd(b + i);
            acc = _mm512_fmadd_pd(va, vb, acc);
        }
        T result = _mm512_reduce_add_pd(acc);
        for (; i < n; ++i)
            result += a[i] * b[i];
        return result;
    }
}
#endif

template <typename T>
T dotProductDispatch(const T * __restrict a, const T * __restrict b, size_t n)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
        return dotProductAVX512<T>(a, b, n);
    if (isArchSupported(TargetArch::x86_64_v3))
        return dotProductAVX2<T>(a, b, n);
#endif
    return dotProductScalar<T>(a, b, n);
}

template <typename T>
void axpyScalar(T * __restrict y, const T * __restrict x, T alpha, size_t n)
{
    for (size_t i = 0; i < n; ++i)
        y[i] += alpha * x[i];
}

#if USE_MULTITARGET_CODE
template <typename T>
X86_64_V3_FUNCTION_SPECIFIC_ATTRIBUTE void axpyAVX2(T * __restrict y, const T * __restrict x, T alpha, size_t n)
{
    size_t i = 0;
    if constexpr (std::is_same_v<T, Float32>)
    {
        __m256 valpha = _mm256_set1_ps(alpha);
        for (; i + 8 <= n; i += 8)
        {
            __m256 vy = _mm256_loadu_ps(y + i);
            __m256 vx = _mm256_loadu_ps(x + i);
            vy = _mm256_fmadd_ps(valpha, vx, vy);
            _mm256_storeu_ps(y + i, vy);
        }
    }
    else
    {
        __m256d valpha = _mm256_set1_pd(alpha);
        for (; i + 4 <= n; i += 4)
        {
            __m256d vy = _mm256_loadu_pd(y + i);
            __m256d vx = _mm256_loadu_pd(x + i);
            vy = _mm256_fmadd_pd(valpha, vx, vy);
            _mm256_storeu_pd(y + i, vy);
        }
    }
    for (; i < n; ++i)
        y[i] += alpha * x[i];
}

template <typename T>
X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE void axpyAVX512(T * __restrict y, const T * __restrict x, T alpha, size_t n)
{
    size_t i = 0;
    if constexpr (std::is_same_v<T, Float32>)
    {
        __m512 valpha = _mm512_set1_ps(alpha);
        for (; i + 16 <= n; i += 16)
        {
            __m512 vy = _mm512_loadu_ps(y + i);
            __m512 vx = _mm512_loadu_ps(x + i);
            vy = _mm512_fmadd_ps(valpha, vx, vy);
            _mm512_storeu_ps(y + i, vy);
        }
    }
    else
    {
        __m512d valpha = _mm512_set1_pd(alpha);
        for (; i + 8 <= n; i += 8)
        {
            __m512d vy = _mm512_loadu_pd(y + i);
            __m512d vx = _mm512_loadu_pd(x + i);
            vy = _mm512_fmadd_pd(valpha, vx, vy);
            _mm512_storeu_pd(y + i, vy);
        }
    }
    for (; i < n; ++i)
        y[i] += alpha * x[i];
}
#endif

template <typename T>
void axpyDispatch(T * __restrict y, const T * __restrict x, T alpha, size_t n)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        axpyAVX512<T>(y, x, alpha, n);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        axpyAVX2<T>(y, x, alpha, n);
        return;
    }
#endif
    axpyScalar<T>(y, x, alpha, n);
}

/// Column-major `n x n` Householder QR: upper triangle is `R`, subdiagonal holds normalized `v` (leading 1 implicit), `tau[k]` are scalars.
template <typename T>
void householderQR(T * __restrict A, T * __restrict tau, size_t n)
{
    for (size_t k = 0; k < n; ++k)
    {
        T * col_k = A + k * n;
        size_t len = n - k;
        T col_norm_sq = dotProductDispatch<T>(col_k + k, col_k + k, len);
        T col_norm = std::sqrt(col_norm_sq);

        if (col_norm < std::numeric_limits<T>::min())
        {
            tau[k] = T(0);
            continue;
        }

        T sigma = (col_k[k] >= T(0)) ? col_norm : -col_norm;

        col_k[k] += sigma;
        T v0 = col_k[k];

        tau[k] = v0 / sigma;

        T inv_v0 = T(1) / v0;
        for (size_t i = k + 1; i < n; ++i)
            col_k[i] *= inv_v0;

        for (size_t j = k + 1; j < n; ++j)
        {
            T * col_j = A + j * n;
            T dot = col_j[k];
            if (len > 1)
                dot += dotProductDispatch<T>(col_k + k + 1, col_j + k + 1, len - 1);
            T w = tau[k] * dot;
            col_j[k] -= w;
            if (len > 1)
                axpyDispatch<T>(col_j + k + 1, col_k + k + 1, -w, len - 1);
        }

        col_k[k] = -sigma;
    }
}

/// `Q` from packed Householder data: backward accumulation `H_0` ... `H_{n-1}`, column-major.
template <typename T>
void extractQ(const T * __restrict A_qr, const T * __restrict tau, T * __restrict Q, size_t n)
{
    std::memset(Q, 0, n * n * sizeof(T));
    for (size_t i = 0; i < n; ++i)
        Q[i + i * n] = T(1);

    for (size_t k_rev = 0; k_rev < n; ++k_rev)
    {
        size_t k = n - 1 - k_rev;
        if (std::abs(tau[k]) < std::numeric_limits<T>::min())
            continue;

        size_t len = n - k;
        const T * v = A_qr + k * n;

        for (size_t j = k; j < n; ++j)
        {
            T * q_col_j = Q + j * n;
            T dot = q_col_j[k];
            if (len > 1)
                dot += dotProductDispatch<T>(v + k + 1, q_col_j + k + 1, len - 1);
            T w = tau[k] * dot;
            q_col_j[k] -= w;
            if (len > 1)
                axpyDispatch<T>(q_col_j + k + 1, v + k + 1, -w, len - 1);
        }
    }
}

/// `Y(N x D) = X(N x d) * P^T` with `P` row-major `D x d`, i.e. `Y[i,j] = dot(X_i, P_j)`.
template <typename T>
void gemmBatchScalar(const T * __restrict X, const T * __restrict P, T * __restrict Y, size_t N, size_t d, size_t D)
{
    std::memset(Y, 0, N * D * sizeof(T));

    static constexpr size_t TILE_N = 32;
    static constexpr size_t TILE_D = 32;
    static constexpr size_t TILE_K = 64;

    for (size_t i0 = 0; i0 < N; i0 += TILE_N)
    {
        const size_t i_end = std::min(i0 + TILE_N, N);
        for (size_t j0 = 0; j0 < D; j0 += TILE_D)
        {
            const size_t j_end = std::min(j0 + TILE_D, D);
            for (size_t k0 = 0; k0 < d; k0 += TILE_K)
            {
                const size_t k_end = std::min(k0 + TILE_K, d);
                const size_t tile_k = k_end - k0;
                for (size_t i = i0; i < i_end; ++i)
                {
                    for (size_t j = j0; j < j_end; ++j)
                    {
                        T acc = T(0);
                        for (size_t k = 0; k < tile_k; ++k)
                            acc += X[i * d + k0 + k] * P[j * d + k0 + k];
                        Y[i * D + j] += acc;
                    }
                }
            }
        }
    }
}

#if USE_MULTITARGET_CODE
template <typename T>
X86_64_V3_FUNCTION_SPECIFIC_ATTRIBUTE void
gemmBatchAVX2(const T * __restrict X, const T * __restrict P, T * __restrict Y, size_t N, size_t d, size_t D)
{
    std::memset(Y, 0, N * D * sizeof(T));

    static constexpr size_t TILE_N = 16;
    static constexpr size_t TILE_D = 16;
    static constexpr size_t TILE_K = 64;
    static constexpr size_t J_UNROLL = 4;

    for (size_t i0 = 0; i0 < N; i0 += TILE_N)
    {
        const size_t i_end = std::min(i0 + TILE_N, N);
        for (size_t j0 = 0; j0 < D; j0 += TILE_D)
        {
            const size_t j_end = std::min(j0 + TILE_D, D);
            for (size_t k0 = 0; k0 < d; k0 += TILE_K)
            {
                const size_t k_end = std::min(k0 + TILE_K, d);
                const size_t tile_k = k_end - k0;
                for (size_t i = i0; i < i_end; ++i)
                {
                    const T * x_row = X + i * d + k0;
                    T * y_row = Y + i * D;
                    size_t j = j0;

                    if constexpr (std::is_same_v<T, Float32>)
                    {
                        for (; j + J_UNROLL <= j_end; j += J_UNROLL)
                        {
                            const T * p0 = P + (j + 0) * d + k0;
                            const T * p1 = P + (j + 1) * d + k0;
                            const T * p2 = P + (j + 2) * d + k0;
                            const T * p3 = P + (j + 3) * d + k0;

                            __m256 acc0 = _mm256_setzero_ps();
                            __m256 acc1 = _mm256_setzero_ps();
                            __m256 acc2 = _mm256_setzero_ps();
                            __m256 acc3 = _mm256_setzero_ps();

                            size_t k = 0;
                            for (; k + 8 <= tile_k; k += 8)
                            {
                                __m256 vx = _mm256_loadu_ps(x_row + k);
                                acc0 = _mm256_fmadd_ps(vx, _mm256_loadu_ps(p0 + k), acc0);
                                acc1 = _mm256_fmadd_ps(vx, _mm256_loadu_ps(p1 + k), acc1);
                                acc2 = _mm256_fmadd_ps(vx, _mm256_loadu_ps(p2 + k), acc2);
                                acc3 = _mm256_fmadd_ps(vx, _mm256_loadu_ps(p3 + k), acc3);
                            }

                            __m256 sum01 = _mm256_hadd_ps(acc0, acc1);
                            __m256 sum23 = _mm256_hadd_ps(acc2, acc3);
                            __m256 sum0123 = _mm256_hadd_ps(sum01, sum23);
                            __m128 hi4 = _mm256_extractf128_ps(sum0123, 1);
                            __m128 lo4 = _mm256_castps256_ps128(sum0123);
                            __m128 dots128 = _mm_add_ps(lo4, hi4);

                            alignas(16) float dots[4];
                            _mm_store_ps(dots, dots128);
                            T dot0 = dots[0];
                            T dot1 = dots[1];
                            T dot2 = dots[2];
                            T dot3 = dots[3];

                            for (; k < tile_k; ++k)
                            {
                                T xv = x_row[k];
                                dot0 += xv * p0[k];
                                dot1 += xv * p1[k];
                                dot2 += xv * p2[k];
                                dot3 += xv * p3[k];
                            }

                            y_row[j + 0] += dot0;
                            y_row[j + 1] += dot1;
                            y_row[j + 2] += dot2;
                            y_row[j + 3] += dot3;
                        }

                        for (; j < j_end; ++j)
                        {
                            const T * p_row = P + j * d + k0;
                            __m256 acc = _mm256_setzero_ps();
                            size_t k = 0;
                            for (; k + 8 <= tile_k; k += 8)
                                acc = _mm256_fmadd_ps(_mm256_loadu_ps(x_row + k), _mm256_loadu_ps(p_row + k), acc);
                            __m128 hi = _mm256_extractf128_ps(acc, 1);
                            __m128 lo = _mm256_castps256_ps128(acc);
                            __m128 s = _mm_add_ps(lo, hi);
                            s = _mm_hadd_ps(s, s);
                            s = _mm_hadd_ps(s, s);
                            T dot = _mm_cvtss_f32(s);
                            for (; k < tile_k; ++k)
                                dot += x_row[k] * p_row[k];
                            y_row[j] += dot;
                        }
                    }
                    else
                    {
                        for (; j + J_UNROLL <= j_end; j += J_UNROLL)
                        {
                            const T * p0 = P + (j + 0) * d + k0;
                            const T * p1 = P + (j + 1) * d + k0;
                            const T * p2 = P + (j + 2) * d + k0;
                            const T * p3 = P + (j + 3) * d + k0;

                            __m256d acc0 = _mm256_setzero_pd();
                            __m256d acc1 = _mm256_setzero_pd();
                            __m256d acc2 = _mm256_setzero_pd();
                            __m256d acc3 = _mm256_setzero_pd();

                            size_t k = 0;
                            for (; k + 4 <= tile_k; k += 4)
                            {
                                __m256d vx = _mm256_loadu_pd(x_row + k);
                                acc0 = _mm256_fmadd_pd(vx, _mm256_loadu_pd(p0 + k), acc0);
                                acc1 = _mm256_fmadd_pd(vx, _mm256_loadu_pd(p1 + k), acc1);
                                acc2 = _mm256_fmadd_pd(vx, _mm256_loadu_pd(p2 + k), acc2);
                                acc3 = _mm256_fmadd_pd(vx, _mm256_loadu_pd(p3 + k), acc3);
                            }

                            __m256d sum01 = _mm256_hadd_pd(acc0, acc1);
                            __m256d sum23 = _mm256_hadd_pd(acc2, acc3);
                            __m128d hi01 = _mm256_extractf128_pd(sum01, 1);
                            __m128d lo01 = _mm256_castpd256_pd128(sum01);
                            __m128d r01 = _mm_add_pd(lo01, hi01);
                            __m128d hi23 = _mm256_extractf128_pd(sum23, 1);
                            __m128d lo23 = _mm256_castpd256_pd128(sum23);
                            __m128d r23 = _mm_add_pd(lo23, hi23);

                            T dot0 = _mm_cvtsd_f64(r01);
                            T dot1 = _mm_cvtsd_f64(_mm_unpackhi_pd(r01, r01));
                            T dot2 = _mm_cvtsd_f64(r23);
                            T dot3 = _mm_cvtsd_f64(_mm_unpackhi_pd(r23, r23));

                            for (; k < tile_k; ++k)
                            {
                                T xv = x_row[k];
                                dot0 += xv * p0[k];
                                dot1 += xv * p1[k];
                                dot2 += xv * p2[k];
                                dot3 += xv * p3[k];
                            }

                            y_row[j + 0] += dot0;
                            y_row[j + 1] += dot1;
                            y_row[j + 2] += dot2;
                            y_row[j + 3] += dot3;
                        }

                        for (; j < j_end; ++j)
                        {
                            const T * p_row = P + j * d + k0;
                            __m256d acc = _mm256_setzero_pd();
                            size_t k = 0;
                            for (; k + 4 <= tile_k; k += 4)
                                acc = _mm256_fmadd_pd(_mm256_loadu_pd(x_row + k), _mm256_loadu_pd(p_row + k), acc);
                            __m128d hi = _mm256_extractf128_pd(acc, 1);
                            __m128d lo = _mm256_castpd256_pd128(acc);
                            __m128d s = _mm_add_pd(lo, hi);
                            T dot = _mm_cvtsd_f64(s) + _mm_cvtsd_f64(_mm_unpackhi_pd(s, s));
                            for (; k < tile_k; ++k)
                                dot += x_row[k] * p_row[k];
                            y_row[j] += dot;
                        }
                    }
                }
            }
        }
    }
}

template <typename T>
X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE void
gemmBatchAVX512(const T * __restrict X, const T * __restrict P, T * __restrict Y, size_t N, size_t d, size_t D)
{
    std::memset(Y, 0, N * D * sizeof(T));

    static constexpr size_t TILE_N = 16;
    static constexpr size_t TILE_D = 16;
    static constexpr size_t TILE_K = 64;
    static constexpr size_t J_UNROLL = 4;

    for (size_t i0 = 0; i0 < N; i0 += TILE_N)
    {
        const size_t i_end = std::min(i0 + TILE_N, N);
        for (size_t j0 = 0; j0 < D; j0 += TILE_D)
        {
            const size_t j_end = std::min(j0 + TILE_D, D);
            for (size_t k0 = 0; k0 < d; k0 += TILE_K)
            {
                const size_t k_end = std::min(k0 + TILE_K, d);
                const size_t tile_k = k_end - k0;
                for (size_t i = i0; i < i_end; ++i)
                {
                    const T * x_row = X + i * d + k0;
                    T * y_row = Y + i * D;
                    size_t j = j0;

                    if constexpr (std::is_same_v<T, Float32>)
                    {
                        for (; j + J_UNROLL <= j_end; j += J_UNROLL)
                        {
                            const T * p0 = P + (j + 0) * d + k0;
                            const T * p1 = P + (j + 1) * d + k0;
                            const T * p2 = P + (j + 2) * d + k0;
                            const T * p3 = P + (j + 3) * d + k0;

                            __m512 acc0 = _mm512_setzero_ps();
                            __m512 acc1 = _mm512_setzero_ps();
                            __m512 acc2 = _mm512_setzero_ps();
                            __m512 acc3 = _mm512_setzero_ps();

                            size_t k = 0;
                            for (; k + 16 <= tile_k; k += 16)
                            {
                                __m512 vx = _mm512_loadu_ps(x_row + k);
                                acc0 = _mm512_fmadd_ps(vx, _mm512_loadu_ps(p0 + k), acc0);
                                acc1 = _mm512_fmadd_ps(vx, _mm512_loadu_ps(p1 + k), acc1);
                                acc2 = _mm512_fmadd_ps(vx, _mm512_loadu_ps(p2 + k), acc2);
                                acc3 = _mm512_fmadd_ps(vx, _mm512_loadu_ps(p3 + k), acc3);
                            }

                            T dot0 = _mm512_reduce_add_ps(acc0);
                            T dot1 = _mm512_reduce_add_ps(acc1);
                            T dot2 = _mm512_reduce_add_ps(acc2);
                            T dot3 = _mm512_reduce_add_ps(acc3);

                            for (; k < tile_k; ++k)
                            {
                                T xv = x_row[k];
                                dot0 += xv * p0[k];
                                dot1 += xv * p1[k];
                                dot2 += xv * p2[k];
                                dot3 += xv * p3[k];
                            }

                            y_row[j + 0] += dot0;
                            y_row[j + 1] += dot1;
                            y_row[j + 2] += dot2;
                            y_row[j + 3] += dot3;
                        }

                        for (; j < j_end; ++j)
                        {
                            const T * p_row = P + j * d + k0;
                            __m512 acc = _mm512_setzero_ps();
                            size_t k = 0;
                            for (; k + 16 <= tile_k; k += 16)
                                acc = _mm512_fmadd_ps(_mm512_loadu_ps(x_row + k), _mm512_loadu_ps(p_row + k), acc);
                            T dot = _mm512_reduce_add_ps(acc);
                            for (; k < tile_k; ++k)
                                dot += x_row[k] * p_row[k];
                            y_row[j] += dot;
                        }
                    }
                    else
                    {
                        for (; j + J_UNROLL <= j_end; j += J_UNROLL)
                        {
                            const T * p0 = P + (j + 0) * d + k0;
                            const T * p1 = P + (j + 1) * d + k0;
                            const T * p2 = P + (j + 2) * d + k0;
                            const T * p3 = P + (j + 3) * d + k0;

                            __m512d acc0 = _mm512_setzero_pd();
                            __m512d acc1 = _mm512_setzero_pd();
                            __m512d acc2 = _mm512_setzero_pd();
                            __m512d acc3 = _mm512_setzero_pd();

                            size_t k = 0;
                            for (; k + 8 <= tile_k; k += 8)
                            {
                                __m512d vx = _mm512_loadu_pd(x_row + k);
                                acc0 = _mm512_fmadd_pd(vx, _mm512_loadu_pd(p0 + k), acc0);
                                acc1 = _mm512_fmadd_pd(vx, _mm512_loadu_pd(p1 + k), acc1);
                                acc2 = _mm512_fmadd_pd(vx, _mm512_loadu_pd(p2 + k), acc2);
                                acc3 = _mm512_fmadd_pd(vx, _mm512_loadu_pd(p3 + k), acc3);
                            }

                            T dot0 = _mm512_reduce_add_pd(acc0);
                            T dot1 = _mm512_reduce_add_pd(acc1);
                            T dot2 = _mm512_reduce_add_pd(acc2);
                            T dot3 = _mm512_reduce_add_pd(acc3);

                            for (; k < tile_k; ++k)
                            {
                                T xv = x_row[k];
                                dot0 += xv * p0[k];
                                dot1 += xv * p1[k];
                                dot2 += xv * p2[k];
                                dot3 += xv * p3[k];
                            }

                            y_row[j + 0] += dot0;
                            y_row[j + 1] += dot1;
                            y_row[j + 2] += dot2;
                            y_row[j + 3] += dot3;
                        }

                        for (; j < j_end; ++j)
                        {
                            const T * p_row = P + j * d + k0;
                            __m512d acc = _mm512_setzero_pd();
                            size_t k = 0;
                            for (; k + 8 <= tile_k; k += 8)
                                acc = _mm512_fmadd_pd(_mm512_loadu_pd(x_row + k), _mm512_loadu_pd(p_row + k), acc);
                            T dot = _mm512_reduce_add_pd(acc);
                            for (; k < tile_k; ++k)
                                dot += x_row[k] * p_row[k];
                            y_row[j] += dot;
                        }
                    }
                }
            }
        }
    }
}
#endif

template <typename T>
void applyDenseProjectionBatch(
    const T * __restrict input_data,
    T * __restrict output_data,
    size_t num_rows,
    size_t input_dim,
    size_t target_dim,
    const T * __restrict matrix)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        gemmBatchAVX512<T>(input_data, matrix, output_data, num_rows, input_dim, target_dim);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        gemmBatchAVX2<T>(input_data, matrix, output_data, num_rows, input_dim, target_dim);
        return;
    }
#endif
    gemmBatchScalar<T>(input_data, matrix, output_data, num_rows, input_dim, target_dim);
}

template <typename T>
struct NormalProjectionState
{
    size_t input_dim = 0;
    size_t target_dim = 0;
    UInt64 seed = 0;
    std::vector<T> matrix;

    void generate(size_t d, size_t D, UInt64 s)
    {
        input_dim = d;
        target_dim = D;
        seed = s;

        pcg64 rng(seed);
        T scale = T(1) / std::sqrt(static_cast<T>(D));

        matrix.resize(D * d);
        for (size_t i = 0; i < D * d; ++i)
            matrix[i] = boxMullerNormal<T>(rng) * scale;
    }
};

template <typename T>
struct OrthogonalProjectionState
{
    size_t input_dim = 0;
    size_t target_dim = 0;
    UInt64 seed = 0;
    std::vector<T> matrix;

    void generate(size_t d, size_t D, UInt64 s)
    {
        input_dim = d;
        target_dim = D;
        seed = s;

        size_t n = std::max(d, D);
        pcg64 rng(seed);

        std::vector<T> gauss(n * n);
        /// Column-major (i outer, j inner) matches Eigen ColMajor RNG consumption for the same seed.
        for (size_t i = 0; i < n; ++i)
            for (size_t j = 0; j < n; ++j)
                gauss[i + j * n] = boxMullerNormal<T>(rng);

        std::vector<T> tau(n);
        householderQR<T>(gauss.data(), tau.data(), n);

        std::vector<T> q_matrix(n * n);
        extractQ<T>(gauss.data(), tau.data(), q_matrix.data(), n);

        T scale = std::sqrt(static_cast<T>(n) / static_cast<T>(D));
        matrix.resize(D * d);
        for (size_t i = 0; i < D; ++i)
            for (size_t j = 0; j < d; ++j)
                matrix[i * d + j] = q_matrix[i + j * n] * scale;
    }
};

template <typename T>
struct SparseProjectionState
{
    size_t input_dim = 0;
    size_t target_dim = 0;
    UInt64 seed = 0;
    T scale = 0;

    /// Per input index `j`: pairs `(output row, +/-1)` for Achlioptas nonzeros.
    std::vector<std::vector<std::pair<UInt32, int8_t>>> columns;

    void generate(size_t d, size_t D, UInt64 s)
    {
        input_dim = d;
        target_dim = D;
        seed = s;
        scale = std::sqrt(T(3) / static_cast<T>(D));

        pcg64 rng(seed);
        std::uniform_int_distribution<int> dist6(0, 5);

        columns.resize(d);
        for (size_t j = 0; j < d; ++j)
        {
            columns[j].clear();
            for (size_t i = 0; i < D; ++i)
            {
                int val = dist6(rng);
                if (val == 0)
                    columns[j].emplace_back(static_cast<UInt32>(i), int8_t(1));
                else if (val == 5)
                    columns[j].emplace_back(static_cast<UInt32>(i), int8_t(-1));
            }
        }
    }
};

template <typename T>
struct HadamardProjectionState
{
    size_t input_dim = 0;
    size_t target_dim = 0;
    UInt64 seed = 0;
    size_t padded_dim = 0;
    T scale_factor = 0;

    struct Block
    {
        std::vector<int8_t> signs;
        std::vector<UInt32> indices;
    };
    std::vector<Block> blocks;

    void generate(size_t d, size_t D, UInt64 s)
    {
        input_dim = d;
        target_dim = D;
        seed = s;
        padded_dim = nextPowerOfTwo(d);

        pcg64 rng(seed);
        std::uniform_int_distribution<int> sign_dist(0, 1);

        size_t num_blocks = (D + padded_dim - 1) / padded_dim;
        blocks.resize(num_blocks);

        size_t remaining = D;
        for (size_t b = 0; b < num_blocks; ++b)
        {
            auto & block = blocks[b];

            block.signs.resize(padded_dim);
            for (size_t i = 0; i < padded_dim; ++i)
                block.signs[i] = sign_dist(rng) ? int8_t(1) : int8_t(-1);

            size_t samples_this_block = std::min(remaining, padded_dim);
            block.indices.resize(samples_this_block);

            std::vector<UInt32> perm(padded_dim);
            std::iota(perm.begin(), perm.end(), 0);
            for (size_t i = 0; i < samples_this_block; ++i)
            {
                std::uniform_int_distribution<size_t> idx_dist(i, padded_dim - 1);
                size_t j = idx_dist(rng);
                std::swap(perm[i], perm[j]);
            }
            std::sort(perm.begin(), perm.begin() + samples_this_block);
            for (size_t i = 0; i < samples_this_block; ++i)
                block.indices[i] = perm[i];

            remaining -= samples_this_block;
        }

        scale_factor = T(1) / std::sqrt(static_cast<T>(D));
    }
};

/// Achlioptas: scale input once per column, scatter `+/-val` to listed rows (no multiply in inner loop).
template <typename T>
void applySparseProjection(const T * __restrict input, T * __restrict output, const SparseProjectionState<T> & state)
{
    std::memset(output, 0, state.target_dim * sizeof(T));

    for (size_t j = 0; j < state.input_dim; ++j)
    {
        const T val = input[j] * state.scale;
        for (const auto & [row_i, sign] : state.columns[j])
            output[row_i] += sign * val;
    }
}

/// SRHT: sign inputs, FWHT on padded power-of-two length, then subsample coefficients per block.
/// `scratch` must have at least `state.padded_dim` elements; avoids per-call heap allocation.
template <typename T>
void applyHadamardProjection(const T * __restrict input, T * __restrict output, const HadamardProjectionState<T> & state, T * __restrict scratch)
{
    size_t out_offset = 0;
    for (const auto & block : state.blocks)
    {
        for (size_t i = 0; i < state.input_dim; ++i)
            scratch[i] = input[i] * block.signs[i];
        for (size_t i = state.input_dim; i < state.padded_dim; ++i)
            scratch[i] = T(0);

        fwht(scratch, state.padded_dim);

        for (size_t i = 0; i < block.indices.size(); ++i)
            output[out_offset + i] = scratch[block.indices[i]] * state.scale_factor;

        out_offset += block.indices.size();
    }
}

/// Convenience overload that allocates the scratch buffer internally.
template <typename T>
void applyHadamardProjection(const T * __restrict input, T * __restrict output, const HadamardProjectionState<T> & state)
{
    std::vector<T> scratch(state.padded_dim);
    applyHadamardProjection(input, output, state, scratch.data());
}

} // namespace DB
