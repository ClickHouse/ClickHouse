#pragma once

#include <base/types.h>
#include <Common/PODArray.h>

#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <string_view>
#include <type_traits>

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

/// Shared (randomized) Hadamard transform kernels: a portable scalar fast Walsh-Hadamard transform, an exact Kronecker
/// transform H_{2^k} (x) H_m for non-power-of-two dimensions of the form 2^k * m (m in {12, 20}, covering common
/// embedding dimensions such as 384/768/1536/3072 and 2560), and NEON variants for float32 on AArch64. The scalar and
/// NEON kernels perform exactly the same butterflies in the same stage order, so they are bit-for-bit identical.
///
/// Used by the `randomHadamardTransform` SQL function and by the structured random projection that backs the quantized
/// vector search codecs (`Common/VectorQuantization.cpp`).
namespace DB::HadamardTransform
{

inline UInt64 splitmix64Next(UInt64 & state)
{
    state += 0x9E3779B97F4A7C15ULL;
    UInt64 z = state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/// The unsigned integer of the same width as the compute type, used to flip its sign bit.
template <typename Compute>
using SignMask = std::conditional_t<sizeof(Compute) == 4, UInt32, UInt64>;

/// Deterministic +-1 signs of length m derived from the seed (a splitmix64 stream), written into
/// `out` as a sign-bit mask per element (the compute type's high bit for a negative sign, 0
/// otherwise) so the sign can be applied as an XOR instead of a multiply. The caller caches the
/// result and only regenerates when the working dimension changes (it is usually constant).
template <typename Compute>
void generateSignMasks(PaddedPODArray<SignMask<Compute>> & out, UInt64 seed, size_t m)
{
    using Mask = SignMask<Compute>;
    static constexpr Mask sign_bit = Mask(1) << (sizeof(Mask) * 8 - 1);
    out.resize(m);
    UInt64 state = seed;
    for (size_t i = 0; i < m; ++i)
        out[i] = (splitmix64Next(state) >> 63) ? sign_bit : Mask(0);
}

/// Apply a +-1 sign as a sign-bit flip: bit-identical to multiplying by +1 or -1 (including +-0).
template <typename Compute>
inline Compute applySign(Compute x, SignMask<Compute> mask)
{
    using Mask = SignMask<Compute>;
    return std::bit_cast<Compute>(static_cast<Mask>(std::bit_cast<Mask>(x) ^ mask));
}

/// In-place unnormalized fast Walsh-Hadamard transform on m (a power of two) elements.
template <typename T>
void fwhtScalar(T * a, size_t m)
{
    for (size_t h = 1; h < m; h <<= 1)
        for (size_t i = 0; i < m; i += (h << 1))
            for (size_t j = i; j < i + h; ++j)
            {
                const T x = a[j];
                const T y = a[j + h];
                a[j] = x + y;
                a[j + h] = x - y;
            }
}

/// The largest dense Hadamard block order supported below (must be a multiple of 4 for the NEON path).
inline constexpr size_t max_hadamard_block = 64;

/// A dimension d = 2^k * m can be transformed exactly (no zero-padding) as the Kronecker product
/// H_{2^k} (x) H_m when m has a Hadamard matrix. Hadamard matrices exist for orders that are
/// multiples of 4; we support m in {12, 20}, which covers the common embedding dimensions
/// (384, 768, 1536, 3072 = 2^k * 12; 2560 = 2^7 * 20). Returns m and the number of blocks 2^k,
/// or {0, 0} when d is not of that form (then we fall back to zero-padding to a power of two).
struct KroneckerFactor
{
    size_t m = 0;
    size_t blocks = 0;
};

inline KroneckerFactor hadamardOrderFor(size_t length)
{
    for (size_t m : {static_cast<size_t>(12), static_cast<size_t>(20)})
        if (length % m == 0 && std::has_single_bit(length / m))
            return {m, length / m};
    return {};
}

/// +-1 sign-bit masks of an m x m Hadamard matrix in both row-major (row[i*m+j]) and column-major
/// (col[j*m+i]) layouts; the column-major copy lets the NEON kernel load four output rows at once.
template <typename Compute>
struct HmMasks
{
    size_t order = 0;
    PaddedPODArray<SignMask<Compute>> row;
    PaddedPODArray<SignMask<Compute>> col;
};

/// Build the sign-bit masks of an m x m Hadamard matrix via the Paley type I construction:
/// m = p + 1 for a prime p = 3 (mod 4) (here p in {11, 19}). H = I + C, where C is the +-1
/// conference matrix from the Legendre symbol: H[0][*] = +1, H[*][0] = -1, the diagonal is +1, and
/// H[1+a][1+b] = chi(a - b) mod p otherwise. The caller caches the result per order m.
template <typename Compute>
void buildHmMasks(HmMasks<Compute> & out, size_t m)
{
    using Mask = SignMask<Compute>;
    static constexpr Mask sign_bit = Mask(1) << (sizeof(Mask) * 8 - 1);

    const size_t p = m - 1;
    std::array<uint8_t, max_hadamard_block> is_quadratic_residue{};
    for (size_t x = 1; x < p; ++x)
        is_quadratic_residue[(x * x) % p] = 1;

    auto entry_is_negative = [&](size_t i, size_t j) -> bool
    {
        if (i == 0)
            return false;
        if (j == 0)
            return true;
        const size_t a = i - 1;
        const size_t b = j - 1;
        if (a == b)
            return false;
        const Int64 pp = static_cast<Int64>(p);
        const Int64 r = ((static_cast<Int64>(a) - static_cast<Int64>(b)) % pp + pp) % pp;
        return is_quadratic_residue[static_cast<size_t>(r)] == 0;  /// non-residue -> -1
    };

    out.order = m;
    out.row.resize(m * m);
    out.col.resize(m * m);
    for (size_t i = 0; i < m; ++i)
        for (size_t j = 0; j < m; ++j)
        {
            const Mask mask = entry_is_negative(i, j) ? sign_bit : Mask(0);
            out.row[i * m + j] = mask;
            out.col[j * m + i] = mask;
        }
}

/// Apply a dense m x m Hadamard matrix to one block of m elements (the I (x) H_m part), in place.
/// The +-1 entries are applied as sign-bit flips and accumulated over j in a fixed order, so the
/// scalar and NEON variants produce bit-identical results.
template <typename Compute>
void applyHmScalar(Compute * block, size_t m, const SignMask<Compute> * row_masks)
{
    Compute z[max_hadamard_block];
    for (size_t i = 0; i < m; ++i)
    {
        Compute acc = 0;
        for (size_t j = 0; j < m; ++j)
            acc += applySign<Compute>(block[j], row_masks[i * m + j]);
        z[i] = acc;
    }
    for (size_t i = 0; i < m; ++i)
        block[i] = z[i];
}

/// Fast Walsh-Hadamard transform over `blocks` (a power of two) groups of m contiguous elements
/// each (the H_{2^k} (x) I_m part). Each butterfly is an elementwise add/sub of two m-vectors.
template <typename Compute>
void fwhtBlocksScalar(Compute * a, size_t blocks, size_t m)
{
    for (size_t h = 1; h < blocks; h <<= 1)
        for (size_t i = 0; i < blocks; i += (h << 1))
            for (size_t p = i; p < i + h; ++p)
            {
                Compute * lo = a + p * m;
                Compute * hi = a + (p + h) * m;
                for (size_t lane = 0; lane < m; ++lane)
                {
                    const Compute x = lo[lane];
                    const Compute y = hi[lane];
                    lo[lane] = x + y;
                    hi[lane] = x - y;
                }
            }
}

/// Exact Kronecker transform H_{2^k} (x) H_m on `blocks` * m elements (scalar kernel).
template <typename Compute>
void kroneckerScalar(Compute * a, size_t blocks, size_t m, const HmMasks<Compute> & hm)
{
    for (size_t p = 0; p < blocks; ++p)
        applyHmScalar<Compute>(a + p * m, m, hm.row.data());
    fwhtBlocksScalar<Compute>(a, blocks, m);
}

#if defined(__aarch64__)

/// 4-point Walsh-Hadamard transform (stages h = 1 then h = 2) within one float32x4 lane group.
/// The add/sub operands and their order match fwhtScalar exactly, so the result is bit-identical.
inline float32x4_t fourPointWHT(float32x4_t v)
{
    const float32x2_t lo = vget_low_f32(v);    /// [x0, x1]
    const float32x2_t hi = vget_high_f32(v);   /// [x2, x3]
    const float32x2_t ev = vuzp1_f32(lo, hi);  /// [x0, x2]
    const float32x2_t od = vuzp2_f32(lo, hi);  /// [x1, x3]
    const float32x2_t s1 = vadd_f32(ev, od);   /// [x0+x1, x2+x3]
    const float32x2_t d1 = vsub_f32(ev, od);   /// [x0-x1, x2-x3]
    const float32x2_t low2 = vzip1_f32(s1, d1);   /// [x0+x1, x0-x1]
    const float32x2_t high2 = vzip2_f32(s1, d1);  /// [x2+x3, x2-x3]
    return vcombine_f32(vadd_f32(low2, high2), vsub_f32(low2, high2));
}

/// In-place unnormalized FWHT on m (a power of two) float32 values using NEON + ILP. Performs the
/// same butterflies in the same stage order as fwhtScalar, so the result is bit-for-bit identical.
inline void fwhtNeon(float * a, size_t m)
{
    /// Block of 8 NEON vectors (32 floats): stages h = 1 .. 16 are done entirely in registers
    /// (they never cross a 32-element block), turning 5 memory passes into one.
    constexpr size_t vectors_per_block = 16;
    constexpr size_t block = vectors_per_block * 4;
    if (m < block)
    {
        fwhtScalar(a, m);
        return;
    }

    for (size_t i = 0; i < m; i += block)
    {
        float32x4_t v[vectors_per_block];
        for (size_t t = 0; t < vectors_per_block; ++t)
            v[t] = fourPointWHT(vld1q_f32(a + i + 4 * t));  /// stages h = 1, 2

        /// Vector-level stages h = 4, 8, 16 (a stride of hv vectors == 4*hv elements).
        for (size_t hv = 1; hv < vectors_per_block; hv <<= 1)
            for (size_t base = 0; base < vectors_per_block; base += (hv << 1))
                for (size_t t = 0; t < hv; ++t)
                {
                    const float32x4_t x = v[base + t];
                    const float32x4_t y = v[base + t + hv];
                    v[base + t] = vaddq_f32(x, y);
                    v[base + t + hv] = vsubq_f32(x, y);
                }

        for (size_t t = 0; t < vectors_per_block; ++t)
            vst1q_f32(a + i + 4 * t, v[t]);
    }

    /// High stages h = block, 2*block, ..., m/2: contiguous SIMD butterflies. Each h is a multiple
    /// of 8, so the inner loop processes two vectors per step for instruction-level parallelism.
    for (size_t h = block; h < m; h <<= 1)
        for (size_t i = 0; i < m; i += (h << 1))
            for (size_t j = i; j < i + h; j += 8)
            {
                const float32x4_t x0 = vld1q_f32(a + j);
                const float32x4_t x1 = vld1q_f32(a + j + 4);
                const float32x4_t y0 = vld1q_f32(a + j + h);
                const float32x4_t y1 = vld1q_f32(a + j + h + 4);
                vst1q_f32(a + j,         vaddq_f32(x0, y0));
                vst1q_f32(a + j + 4,     vaddq_f32(x1, y1));
                vst1q_f32(a + j + h,     vsubq_f32(x0, y0));
                vst1q_f32(a + j + h + 4, vsubq_f32(x1, y1));
            }
}

/// NEON version of applyHmScalar: computes four output rows at a time (m is a multiple of 4). Uses
/// the column-major masks so the four rows for a given column load contiguously, and accumulates
/// over j in the same order as the scalar version, so the per-row result is bit-identical.
inline void applyHmNeon(float * block, size_t m, const UInt32 * col_masks)
{
    float z[max_hadamard_block];
    for (size_t i = 0; i < m; i += 4)
    {
        float32x4_t acc = vdupq_n_f32(0.0F);
        for (size_t j = 0; j < m; ++j)
        {
            const float32x4_t bj = vdupq_n_f32(block[j]);
            const uint32x4_t mask = vld1q_u32(col_masks + j * m + i);
            acc = vaddq_f32(acc, vreinterpretq_f32_u32(veorq_u32(vreinterpretq_u32_f32(bj), mask)));
        }
        vst1q_f32(z + i, acc);
    }
    for (size_t i = 0; i < m; ++i)
        block[i] = z[i];
}

/// NEON version of fwhtBlocksScalar: each butterfly add/sub processes the m lanes four at a time.
inline void fwhtBlocksNeon(float * a, size_t blocks, size_t m)
{
    for (size_t h = 1; h < blocks; h <<= 1)
        for (size_t i = 0; i < blocks; i += (h << 1))
            for (size_t p = i; p < i + h; ++p)
            {
                float * lo = a + p * m;
                float * hi = a + (p + h) * m;
                for (size_t lane = 0; lane < m; lane += 4)
                {
                    const float32x4_t x = vld1q_f32(lo + lane);
                    const float32x4_t y = vld1q_f32(hi + lane);
                    vst1q_f32(lo + lane, vaddq_f32(x, y));
                    vst1q_f32(hi + lane, vsubq_f32(x, y));
                }
            }
}

/// Exact Kronecker transform H_{2^k} (x) H_m on `blocks` * m float32 values (NEON kernel).
inline void kroneckerNeon(float * a, size_t blocks, size_t m, const HmMasks<float> & hm)
{
    for (size_t p = 0; p < blocks; ++p)
        applyHmNeon(a + p * m, m, hm.col.data());
    fwhtBlocksNeon(a, blocks, m);
}

enum class FwhtKernel
{
    Scalar,
    Neon,
};

/// Read the kernel choice once from CLICKHOUSE_RHT_KERNEL (default: NEON on AArch64).
inline FwhtKernel selectKernel()
{
    static const FwhtKernel kernel = []
    {
        if (const char * env = std::getenv("CLICKHOUSE_RHT_KERNEL"))  /// NOLINT(concurrency-mt-unsafe)
        {
            if (std::string_view(env) == "scalar")
                return FwhtKernel::Scalar;
            if (std::string_view(env) == "neon")
                return FwhtKernel::Neon;
        }
        return FwhtKernel::Neon;
    }();
    return kernel;
}

#endif

}
