#include <Common/VectorQuantization.h>

#include <Common/Exception.h>
#include <Common/HadamardTransform.h>
#include <Common/HashTable/HashMap.h>
#include <Common/LloydMaxQuantizer.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>
#include <numbers>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

/// Quantization codec id.
enum class FlatQuantization : UInt8
{
    TurboQuant = 2,
    RaBitQ = 3,
    E8 = 4,
    Int8 = 5,
};

size_t expectedFlatBytesPerVector(FlatQuantization quantization, size_t dimensions, size_t e8_bits)
{
    switch (quantization)
    {
        case FlatQuantization::TurboQuant:
            return dimensions / 4 + sizeof(float);
        case FlatQuantization::RaBitQ:
            return dimensions / 8 + sizeof(float);
        case FlatQuantization::E8:
            return (dimensions / 8) * (e8_bits <= 8 ? 1 : 2) + sizeof(float);
        case FlatQuantization::Int8:
            /// One Int8 Lloyd-Max code per coordinate, followed by the 4-byte original L2 norm.
            return dimensions + sizeof(float);
    }
    return 0;
}

constexpr UInt64 RANDOM_PROJECTION_SEED = 0x9E3779B97F4A7C15ULL;

/// Second, independent seed for the 'turboquant' QJL (Quantized Johnson-Lindenstrauss) projection, applied to the
/// MSE-quantization residual. Must differ from RANDOM_PROJECTION_SEED so the two projections are independent.
constexpr UInt64 RANDOM_PROJECTION_SEED_QJL = 0xD1B54A32D192ED03ULL;

/// Number of (sign-flip + Hadamard) rounds. Three rounds of HD approximate a random rotation well, so the
/// resulting sign bits behave as random-hyperplane hashes (angular LSH).
constexpr int PROJECTION_ROUNDS = 3;

/// Smallest power of two >= n.
inline size_t projectionPaddedSize(size_t n)
{
    size_t p = 1;
    while (p < n)
        p <<= 1;
    return p;
}

/// Working dimension of the structured transform: the input dimension itself when it admits an exact Hadamard matrix
/// (`dimensions = 2^k * m`, m in {12, 20} - e.g. 384/768/1536/3072/2560), otherwise the input zero-padded to the next
/// power of two. Using the exact dimension avoids both the padding work and the geometry distortion of padding.
inline size_t projectionWorkingDim(size_t dimensions)
{
    return HadamardTransform::hadamardOrderFor(dimensions).m ? dimensions : projectionPaddedSize(dimensions);
}

/// A fast structured random projection: PROJECTION_ROUNDS blocks of (random ±1 diagonal) * Hadamard transform,
/// replacing a dense d*d Gaussian matrix (O(d*d) per vector) with an O(d log d) transform. Returns the concatenated
/// sign-flip diagonals (PROJECTION_ROUNDS * working_dim entries, each +-1), generated deterministically from the seed.
std::vector<float> generateRandomProjection(size_t dimensions, UInt64 seed = RANDOM_PROJECTION_SEED)
{
    const size_t working_dim = projectionWorkingDim(dimensions);
    std::vector<float> sign_flips(static_cast<size_t>(PROJECTION_ROUNDS) * working_dim);

    /// splitmix64 bit stream -> +-1
    UInt64 state = seed;
    for (auto & value : sign_flips)
    {
        UInt64 z = (state += 0x9E3779B97F4A7C15ULL);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        z = z ^ (z >> 31);
        value = (z & 1) ? 1.0f : -1.0f;
    }
    return sign_flips;
}

/// Projects `x` (dimensions elements) using the structured transform into `work`. The first `dimensions` entries of
/// `work` are the projected coordinates whose signs form the code. `work` must have at least `working_dim`
/// (= sign_flips.size() / PROJECTION_ROUNDS) elements. The Hadamard step is the exact Kronecker transform when
/// `dimensions` factors as 2^k * m (m in {12, 20}), otherwise an FWHT over the zero-padded power-of-two working size.
template <typename T>
void applyRandomProjection(const std::vector<float> & sign_flips, const T * x, size_t dimensions, float * work)
{
    const size_t working_dim = sign_flips.size() / PROJECTION_ROUNDS;
    for (size_t i = 0; i < dimensions; ++i)
        work[i] = static_cast<float>(x[i]);
    for (size_t i = dimensions; i < working_dim; ++i)
        work[i] = 0.0f;

    const auto [kron_m, kron_blocks] = HadamardTransform::hadamardOrderFor(dimensions);
    HadamardTransform::HmMasks<float> hm;
    if (kron_m)
        HadamardTransform::buildHmMasks<float>(hm, kron_m);
#if defined(__aarch64__)
    const bool neon = HadamardTransform::selectKernel() == HadamardTransform::FwhtKernel::Neon;
#endif

    for (int round = 0; round < PROJECTION_ROUNDS; ++round)
    {
        const float * flip = sign_flips.data() + static_cast<size_t>(round) * working_dim;
        for (size_t i = 0; i < working_dim; ++i)
            work[i] *= flip[i];

        if (kron_m)
        {
#if defined(__aarch64__)
            if (neon)
                HadamardTransform::kroneckerNeon(work, kron_blocks, kron_m, hm);
            else
                HadamardTransform::kroneckerScalar<float>(work, kron_blocks, kron_m, hm);
#else
            HadamardTransform::kroneckerScalar<float>(work, kron_blocks, kron_m, hm);
#endif
        }
        else
        {
#if defined(__aarch64__)
            if (neon)
                HadamardTransform::fwhtNeon(work, working_dim);
            else
                HadamardTransform::fwhtScalar(work, working_dim);
#else
            HadamardTransform::fwhtScalar(work, working_dim);
#endif
        }
    }

    /// The Hadamard rounds are unnormalized, so a finite (but large) Float32 input can overflow to Inf/NaN here. The
    /// downstream encoders/scans feed `work` into norms, `std::lround`, and integer casts, so a non-finite value would
    /// be undefined behaviour or a garbage code. Fail loud instead. This is a single linear pass over the used
    /// coordinates, cheap relative to the O(working_dim log working_dim) transform above, and only fires for extreme inputs.
    for (size_t i = 0; i < dimensions; ++i)
        if (!std::isfinite(work[i]))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Vector for vector similarity index overflowed to a non-finite value during the random projection; "
                "its magnitude is too large for the 'fastknn' projection-based quantizations");
}

/// RaBitQ (1-bit, asymmetric estimator), data-oblivious / no codebook (Gao & Long, "RaBitQ", SIGMOD 2024).
/// After the structured random projection (a random rotation), the data vector is sign-binarized (1 bit/coordinate).
/// RaBitQ additionally stores a per-vector correction factor that turns the sign code into an
/// UNBIASED estimator of the cosine similarity. With `o` the unit (rotated) data vector and `s_i = sign(o_i)`, the
/// stored value is `inv_factor = ||o||_2 / ||o||_1` (note `||o||_2 = 1`, so `factor = sum_i |o_i| >= 1` and
/// `inv_factor` in (0, 1]). The estimator is asymmetric (the query has more resolution than the 1-bit data):
///   cos(o, q) ~= (sum_i s_i * q_i) * (1 / ||q||_2) * inv_factor,    with sum_i s_i * q_i = 2 * sum_{s_i = +1} q_i - sum_i q_i.
/// Layout per vector: `dimensions / 8` packed sign bits, followed by the 4-byte `inv_factor`. The extra factor and the
/// query resolution are what lift recall above plain SimHash.
///
/// The data half `sum_{s_i = +1} q_i` is the inner product of the 1-bit data code with the query. Rather than a
/// full-precision float dot product (which costs like the multi-bit 'turboquant' scan, not like the 1-bit sign scan),
/// the query is uniformly quantized to RABITQ_QUERY_BITS bits and stored "bit-sliced" as one bit-plane per bit. The
/// inner product then reduces to a few AND+popcount passes over the data code (see `RaBitQQuery`), keeping the 1-bit
/// scan in the popcount regime - the same trick RaBitQ/BBQ use to keep it fast.
template <typename T>
void encodeRaBitQ(const std::vector<float> & projection, const T * x, size_t dimensions, float * work, char * dst)
{
    applyRandomProjection(projection, x, dimensions, work);

    const size_t code_bytes = dimensions / 8;
    std::memset(dst, 0, code_bytes);
    double l1 = 0.0;
    double l2sq = 0.0;
    for (size_t i = 0; i < dimensions; ++i)
    {
        const float w = work[i];
        l1 += std::abs(static_cast<double>(w));
        l2sq += static_cast<double>(w) * static_cast<double>(w);
        if (w >= 0.0f)
            dst[i >> 3] |= static_cast<char>(1u << (i & 7u));
    }
    /// inv_factor = ||o||_2 / ||o||_1 = sqrt(l2sq) / l1. A zero vector -> 0 (estimated cosine 0, i.e. distance 1).
    const float inv_factor = (l1 > 0.0) ? static_cast<float>(std::sqrt(l2sq) / l1) : 0.0f;
    std::memcpy(dst + code_bytes, &inv_factor, sizeof(float));
}

/// Number of bits the query is uniformly quantized to for the bit-sliced scan. 4 bits gives the query 16 levels, which
/// (with the per-vector correction factor) keeps the asymmetric estimator's accuracy while making the scan popcount-based.
constexpr int RABITQ_QUERY_BITS = 4;

/// The query side of the RaBitQ estimator, prepared once per query. The (rotated) query is uniformly quantized to
/// RABITQ_QUERY_BITS bits per coordinate: `q_tilde_i = round((q_i - q_min) / delta)` in [0, 2^bits - 1]. It is stored
/// "bit-sliced" as RABITQ_QUERY_BITS bit-planes, each `code_bytes = dimensions / 8` bytes with the same bit layout as a
/// data code (coordinate i -> byte i/8, bit i%8). `q_total = sum_i q_i` and `inv_qnorm = 1 / ||q||_2` use the exact
/// (un-quantized) query so only the data-code inner product is approximate.
struct RaBitQQuery
{
    std::vector<UInt8> planes; /// RABITQ_QUERY_BITS * code_bytes, plane j at offset j * code_bytes
    size_t code_bytes = 0;
    float delta = 0.0f;
    float q_min = 0.0f;
    float q_total = 0.0f;
    float inv_qnorm = 0.0f;
};

/// Build the bit-sliced query from a (rotated, full-precision) query vector `q` of `dimensions` coordinates.
RaBitQQuery buildRaBitQQuery(const float * q, size_t dimensions)
{
    RaBitQQuery out;
    out.code_bytes = dimensions / 8;
    out.planes.assign(static_cast<size_t>(RABITQ_QUERY_BITS) * out.code_bytes, 0);

    double total = 0.0;
    double sumsq = 0.0;
    float q_min = q[0];
    float q_max = q[0];
    for (size_t i = 0; i < dimensions; ++i)
    {
        total += static_cast<double>(q[i]);
        sumsq += static_cast<double>(q[i]) * static_cast<double>(q[i]);
        q_min = std::min(q_min, q[i]);
        q_max = std::max(q_max, q[i]);
    }
    out.q_total = static_cast<float>(total);
    out.inv_qnorm = (sumsq > 0.0) ? static_cast<float>(1.0 / std::sqrt(sumsq)) : 0.0f;
    out.q_min = q_min;

    constexpr int levels = (1 << RABITQ_QUERY_BITS) - 1; /// 15 for 4 bits
    const float range = q_max - q_min;
    out.delta = (range > 0.0f) ? (range / static_cast<float>(levels)) : 0.0f;
    const float inv_delta = (out.delta > 0.0f) ? (1.0f / out.delta) : 0.0f;

    for (size_t i = 0; i < dimensions; ++i)
    {
        int q_tilde = static_cast<int>(std::lround((q[i] - q_min) * inv_delta));
        q_tilde = std::clamp(q_tilde, 0, levels);
        for (int j = 0; j < RABITQ_QUERY_BITS; ++j)
            if ((q_tilde >> j) & 1)
                out.planes[static_cast<size_t>(j) * out.code_bytes + (i >> 3)] |= static_cast<UInt8>(1u << (i & 7u));
    }
    return out;
}

/// Reconstruct `sum_i s_i * q_i` (the inner product of the +-1 sign code with the bit-sliced query) from the popcounts.
/// `pc = popcount(code)` = number of +1 sign bits; `plane_pc[j] = popcount(code AND query_plane_j)`. This is the shared
/// estimator core used by both 'rabitq' and the exact 'turboquant' (which sums two such dots).
inline float raBitQNumeratorFromCounts(const RaBitQQuery & q, UInt64 pc, const UInt64 * plane_pc)
{
    /// sum_i b_i * q_tilde_i = sum_j 2^j * popcount(code AND plane_j); approx sum over set bits of q_i:
    ///   sum_{b_i=1} q_i ~= delta * (sum_j 2^j * plane_pc[j]) + q_min * pc.
    UInt64 weighted = 0;
    for (int j = 0; j < RABITQ_QUERY_BITS; ++j)
        weighted += plane_pc[j] << j;
    const float sum_set = q.delta * static_cast<float>(weighted) + q.q_min * static_cast<float>(pc);
    return 2.0f * sum_set - q.q_total; /// sum_i s_i * q_i
}

/// Scalar bit-sliced dot: AND+popcount the +-1 sign code against each query bit-plane, 8 bytes at a time, and return
/// `sum_i s_i * q_i`. `code` points at `q.code_bytes` packed sign bits (no trailing factor is read here).
inline float raBitQNumeratorScalar(const RaBitQQuery & q, const char * code)
{
    const size_t code_bytes = q.code_bytes;
    const UInt8 * planes = q.planes.data();
    UInt64 pc = 0;
    UInt64 plane_pc[RABITQ_QUERY_BITS] = {};
    size_t b = 0;
    for (; b + 8 <= code_bytes; b += 8)
    {
        UInt64 cw = 0;
        std::memcpy(&cw, code + b, sizeof(cw));
        pc += static_cast<UInt64>(std::popcount(cw));
        for (int j = 0; j < RABITQ_QUERY_BITS; ++j)
        {
            UInt64 pw = 0;
            std::memcpy(&pw, planes + static_cast<size_t>(j) * code_bytes + b, sizeof(pw));
            plane_pc[j] += static_cast<UInt64>(std::popcount(cw & pw));
        }
    }
    for (; b < code_bytes; ++b)
    {
        const unsigned cw = static_cast<UInt8>(code[b]);
        pc += static_cast<UInt64>(std::popcount(cw));
        for (int j = 0; j < RABITQ_QUERY_BITS; ++j)
            plane_pc[j] += static_cast<UInt64>(std::popcount(cw & static_cast<unsigned>(planes[static_cast<size_t>(j) * code_bytes + b])));
    }
    return raBitQNumeratorFromCounts(q, pc, plane_pc);
}

/// Scalar-only dispatch (the standalone module does not use the AVX-512 fast scan).
inline float raBitQNumeratorFast(const RaBitQQuery & q, const char * code)
{
    return raBitQNumeratorScalar(q, code);
}

/// 'rabitq' estimator -> cosineDistance.
inline float raBitQDistanceFast(const RaBitQQuery & q, const char * code)
{
    const float numerator = raBitQNumeratorFast(q, code);
    float inv_factor = NAN;
    std::memcpy(&inv_factor, code + q.code_bytes, sizeof(float));
    return 1.0f - numerator * q.inv_qnorm * inv_factor;
}

/// Each <., .> is a sum of a full-precision (bit-sliced) query against a +-1 sign code - the same popcount dot as RaBitQ.
/// Layout per vector: d/8 MSE sign bits, d/8 QJL sign bits, then the 4-byte `gamma` (d/4 + sizeof(float) bytes total).
constexpr double TURBOQUANT_PI = std::numbers::pi;

template <typename T>
void encodeTurboQuant(const std::vector<float> & p1, const std::vector<float> & p2,
                      const T * x, size_t dimensions, float * work1, float * work2, char * dst)
{
    const float c0 = static_cast<float>(std::sqrt(2.0 / (TURBOQUANT_PI * static_cast<double>(dimensions))));

    /// Stage 1: rotate by P1 and normalize the first `dimensions` coordinates to unit L2.
    applyRandomProjection(p1, x, dimensions, work1);
    double nrm2 = 0.0;
    for (size_t i = 0; i < dimensions; ++i)
        nrm2 += static_cast<double>(work1[i]) * static_cast<double>(work1[i]);
    const float inv_norm = (nrm2 > 0.0) ? static_cast<float>(1.0 / std::sqrt(nrm2)) : 0.0f;

    const size_t code_bytes = dimensions / 8;
    std::memset(dst, 0, 2 * code_bytes);

    /// MSE signs s = sign(a); residual r = a - c0 * s into `work2`.
    for (size_t i = 0; i < dimensions; ++i)
    {
        const float a = work1[i] * inv_norm;
        if (a >= 0.0f)
        {
            dst[i >> 3] |= static_cast<char>(1u << (i & 7u));
            work2[i] = a - c0;
        }
        else
            work2[i] = a + c0;
    }

    double g2 = 0.0;
    for (size_t i = 0; i < dimensions; ++i)
        g2 += static_cast<double>(work2[i]) * static_cast<double>(work2[i]);
    const float gamma = static_cast<float>(std::sqrt(g2));

    /// Stage 2 (QJL): z = sign(P2 * r). `work1` (P1 output) is free to reuse as the P2 output buffer.
    applyRandomProjection(p2, work2, dimensions, work1);
    char * z_dst = dst + code_bytes;
    for (size_t i = 0; i < dimensions; ++i)
        if (work1[i] >= 0.0f)
            z_dst[i >> 3] |= static_cast<char>(1u << (i & 7u));

    std::memcpy(dst + 2 * code_bytes, &gamma, sizeof(float));
}

/// The query state for the exact TurboQuant scan: the bit-sliced unit rotated query (for the MSE-sign dot) and the
/// bit-sliced P2-projection of it (for the QJL-sign dot), plus the two estimator constants.
struct TurboQuantQuery
{
    RaBitQQuery mse;
    RaBitQQuery qjl;
    float c0 = 0.0f;
    float k = 0.0f;
};

TurboQuantQuery buildTurboQuantQuery(const std::vector<float> & p1, const std::vector<float> & p2,
                                     const Float64 * y, size_t dimensions)
{
    const size_t padded = p1.size() / PROJECTION_ROUNDS;
    std::vector<float> work1(padded);
    std::vector<float> work2(padded);

    applyRandomProjection(p1, y, dimensions, work1.data());
    double nrm2 = 0.0;
    for (size_t i = 0; i < dimensions; ++i)
        nrm2 += static_cast<double>(work1[i]) * static_cast<double>(work1[i]);
    const float inv_norm = (nrm2 > 0.0) ? static_cast<float>(1.0 / std::sqrt(nrm2)) : 0.0f;
    for (size_t i = 0; i < dimensions; ++i)
        work1[i] *= inv_norm; /// q1 = unit(P1 y)

    TurboQuantQuery out;
    out.mse = buildRaBitQQuery(work1.data(), dimensions);
    applyRandomProjection(p2, work1.data(), dimensions, work2.data()); /// P2 * q1
    out.qjl = buildRaBitQQuery(work2.data(), dimensions);
    out.c0 = static_cast<float>(std::sqrt(2.0 / (TURBOQUANT_PI * static_cast<double>(dimensions))));
    out.k = static_cast<float>(std::sqrt(TURBOQUANT_PI / 2.0) / (static_cast<double>(dimensions) * static_cast<double>(padded)));
    return out;
}

/// Exact TurboQuant cosine estimator -> cosineDistance. `code` is `d/8` MSE sign bits, `d/8` QJL sign bits, then `gamma`.
inline float turboQuantDistanceFast(const TurboQuantQuery & q, const char * code)
{
    const size_t code_bytes = q.mse.code_bytes; /// = dimensions / 8
    const float mse_dot = raBitQNumeratorFast(q.mse, code);
    const float qjl_dot = raBitQNumeratorFast(q.qjl, code + code_bytes);
    float gamma = NAN;
    std::memcpy(&gamma, code + 2 * code_bytes, sizeof(float));
    const float cosine = q.c0 * mse_dot + q.k * gamma * qjl_dot;
    return 1.0f - cosine;
}

/// Layout per vector: M = dimensions/8 sub-quantizer codes (1 byte each for bits <= 8, else 2 bytes), then the 4-byte
/// L2 norm of the original vector.
constexpr size_t E8_SUBDIM = 8;

/// Nearest point of D8 = { integer vectors with even coordinate sum } to `y` (8 doubles), returned as integers in `out`.
inline void nearestD8(const double * y, int * out)
{
    Int64 isum = 0;
    size_t worst = 0;
    double worst_delta = -1.0;
    for (size_t i = 0; i < E8_SUBDIM; ++i)
    {
        const double r = std::nearbyint(y[i]);
        const int ri = static_cast<int>(r);
        out[i] = ri;
        isum += ri;
        const double delta = std::abs(y[i] - r);
        if (delta > worst_delta)
        {
            worst_delta = delta;
            worst = i;
        }
    }
    /// Odd sum -> not in D8; flip the worst-rounded coordinate to its second-nearest integer to restore an even sum.
    if ((isum & 1) != 0)
        out[worst] += (y[worst] >= static_cast<double>(out[worst])) ? 1 : -1;
}

/// Nearest E8 point to `y`, written to `out2` as 2*coordinate integers (so all-integer: even entries = the integer
/// coset, odd entries = the half-integer coset). The half-integer coset is handled by quantizing y - 1/2 to D8 and
/// shifting back. Returns nothing; this is O(d).
inline void nearestE8(const double * y, int * out2)
{
    int a[E8_SUBDIM];
    nearestD8(y, a);
    double da = 0.0;
    for (size_t i = 0; i < E8_SUBDIM; ++i)
    {
        const double d = y[i] - a[i];
        da += d * d;
    }

    double yshift[E8_SUBDIM];
    for (size_t i = 0; i < E8_SUBDIM; ++i)
        yshift[i] = y[i] - 0.5;
    int b[E8_SUBDIM];
    nearestD8(yshift, b);
    double db = 0.0;
    for (size_t i = 0; i < E8_SUBDIM; ++i)
    {
        const double d = yshift[i] - b[i];
        db += d * d;
    }

    if (da <= db)
        for (size_t i = 0; i < E8_SUBDIM; ++i)
            out2[i] = 2 * a[i];          /// integer coset -> even entries
    else
        for (size_t i = 0; i < E8_SUBDIM; ++i)
            out2[i] = 2 * b[i] + 1;      /// half-integer coset -> odd entries
}

/// Pack the 8 (2*coordinate) integers of a lattice point into a 64-bit key. Entries are small (|2*coord| <= 7 for the
/// shells we enumerate), so int8 per entry is exact.
inline UInt64 e8Key(const int * coord2)
{
    UInt64 k = 0;
    for (size_t i = 0; i < E8_SUBDIM; ++i)
        k = (k << 8) | static_cast<UInt8>(static_cast<int8_t>(coord2[i]));
    return k;
}

struct E8Codebook
{
    size_t bits = 0;
    size_t num_points = 0;                              /// 2^bits
    float scale = 0.0f;                                 /// alpha: lattice units -> real (rotated, unit-normalized) space
    bool use_clamp = false;                             /// whether to clamp query points into the fully-contained region
    float clamp_radius = 0.0f;                          /// clamp radius (lattice units); guarantees decode lands in-set
    std::vector<float> coords;                          /// num_points * 8, already multiplied by `scale`
    HashMap<UInt64, UInt32> point_to_index;             /// e8Key(2*coord) -> codebook index (flat, cache-friendly)
};

/// Recursively enumerate all E8 lattice points (in 2*coordinate units, so all entries share parity `parity`) whose
/// squared norm (in 2*coordinate units) is <= `max_sumsq`. E8 membership reduces to: all entries same parity and the
/// sum of the 2*coordinates is divisible by 4.
void e8Collect(int depth, Int64 sumsq, Int64 max_sumsq, int parity, std::array<int, E8_SUBDIM> & cur, std::vector<std::pair<int, std::array<int, E8_SUBDIM>>> & out)
{
    if (depth == static_cast<int>(E8_SUBDIM))
    {
        Int64 s = 0;
        for (size_t i = 0; i < E8_SUBDIM; ++i)
            s += cur[i];
        if (((s % 4) + 4) % 4 != 0)
            return;
        out.emplace_back(static_cast<int>(sumsq), cur);
        return;
    }
    const Int64 remaining = max_sumsq - sumsq;
    const int maxc = static_cast<int>(std::floor(std::sqrt(static_cast<double>(remaining))));
    for (int v = -maxc; v <= maxc; ++v)
    {
        if ((((v % 2) + 2) % 2) != parity)
            continue;
        const Int64 ns = sumsq + static_cast<Int64>(v) * v;
        if (ns > max_sumsq)
            continue;
        cur[static_cast<size_t>(depth)] = v;
        e8Collect(depth + 1, ns, max_sumsq, parity, cur, out);
    }
}

/// Build the (data-independent) E8 codebook of 2^bits points for a given vector dimensionality. The lattice is truncated
/// to the 2^bits points closest to the origin (deterministic tie-break by squared norm then lexicographic coordinates)
/// and scaled so its RMS radius matches that of an 8-dim sub-vector of a unit vector (sqrt(8 / dimensions)).
std::shared_ptr<const E8Codebook> buildE8Codebook(size_t dimensions, size_t bits)
{
    const size_t num_points = static_cast<size_t>(1) << bits;

    std::vector<std::pair<int, std::array<int, E8_SUBDIM>>> pts;
    Int64 max_sumsq = 8;
    while (true)
    {
        pts.clear();
        std::array<int, E8_SUBDIM> cur{};
        e8Collect(0, 0, max_sumsq, 0, cur, pts); /// integer coset
        e8Collect(0, 0, max_sumsq, 1, cur, pts); /// half-integer coset
        if (pts.size() >= num_points)
            break;
        max_sumsq *= 2;
    }

    std::sort(pts.begin(), pts.end(),
        [](const auto & x, const auto & y)
        {
            if (x.first != y.first)
                return x.first < y.first;
            return x.second < y.second;
        });

    /// `complete_q` is the squared norm (in 2*coordinate units) of the largest shell that is ENTIRELY contained in the
    /// truncated set. Inside the corresponding radius the E8 decoder is guaranteed to land on a stored point, so we can
    /// avoid the (expensive) brute-force fallback entirely by clamping query points there. The set keeps the
    /// `num_points` points closest to the origin; the outermost shell may be only partially included.
    const int last_q = pts[num_points - 1].first;
    int complete_q = last_q;
    if (num_points < pts.size() && pts[num_points].first == last_q)
    {
        size_t i = num_points;
        while (i > 0 && pts[i - 1].first == last_q)
            --i;
        complete_q = (i > 0) ? pts[i - 1].first : 0;
    }
    const double r_complete = std::sqrt(static_cast<double>(complete_q) / 4.0);
    const double r_ref = (r_complete > 0.5) ? r_complete : std::sqrt(static_cast<double>(last_q) / 4.0);

    pts.resize(num_points);

    auto cb = std::make_shared<E8Codebook>();
    cb->bits = bits;
    cb->num_points = num_points;

    /// Scale so a typical sub-vector norm (sqrt(8/dimensions) for a unit rotated vector) maps to r_ref / 1.75. The
    /// sub-vector norm is concentrated, so r_ref then corresponds to roughly +3 sigma and almost every point decodes
    /// inside the fully-contained region.
    const double target_rms = std::sqrt(static_cast<double>(E8_SUBDIM) / static_cast<double>(dimensions));
    cb->scale = static_cast<float>(1.75 * target_rms / std::max(r_ref, 1e-6));
    const double typical_radius = static_cast<double>(target_rms) / std::max(static_cast<double>(cb->scale), 1e-12);

    /// Clamp query points to (r_complete - covering_radius) when that comfortably exceeds the typical data radius (the
    /// E8 covering radius is 1). The clamp then guarantees every decode lands on a stored point WITHOUT collapsing the
    /// (concentrated) data onto the origin - which is only possible for codebooks large enough to have inner room
    /// (otherwise the small codebook's brute-force fallback is cheap anyway).
    if (r_complete - 1.0 > typical_radius)
    {
        cb->use_clamp = true;
        cb->clamp_radius = static_cast<float>(r_complete - 1.0);
    }

    cb->coords.resize(num_points * E8_SUBDIM);
    cb->point_to_index.reserve(num_points * 2);
    for (size_t k = 0; k < num_points; ++k)
    {
        int c2[E8_SUBDIM];
        for (size_t i = 0; i < E8_SUBDIM; ++i)
        {
            c2[i] = pts[k].second[i];
            cb->coords[k * E8_SUBDIM + i] = cb->scale * (static_cast<float>(c2[i]) * 0.5f);
        }
        cb->point_to_index[e8Key(c2)] = static_cast<UInt32>(k);
    }
    return cb;
}

/// Process-wide cache of E8 codebooks keyed by (dimensions, bits). The codebook depends only on these two values (it is
/// not data-dependent), so build and query threads share one immutable instance. Both keys come from user DDL, so the
/// cache is bounded: once it is full, it is cleared before inserting a new entry (codebooks are cheap to rebuild, and
/// existing `shared_ptr` holders keep their instance alive until done). This caps the resident codebook memory.
std::shared_ptr<const E8Codebook> getE8Codebook(size_t dimensions, size_t bits)
{
    static constexpr size_t max_cached_codebooks = 32;
    static std::mutex mutex;
    static std::map<std::pair<size_t, size_t>, std::shared_ptr<const E8Codebook>> cache;
    std::lock_guard lock(mutex);
    const auto key = std::make_pair(dimensions, bits);
    auto it = cache.find(key);
    if (it != cache.end())
        return it->second;
    if (cache.size() >= max_cached_codebooks)
        cache.clear();
    auto cb = buildE8Codebook(dimensions, bits);
    cache.emplace(key, cb);
    return cb;
}

/// Encode one rotated, unit-normalized vector `rhat` (`dimensions` floats) into M = dimensions/8 sub-quantizer codes
/// written to `dst` (`code_bytes` each), followed by the 4-byte original L2 norm `norm`.
inline void encodeE8(const E8Codebook & cb, const float * rhat, size_t dimensions, size_t code_bytes, float norm, char * dst)
{
    const size_t M = dimensions / E8_SUBDIM;
    const double inv_scale = (cb.scale > 0.0f) ? 1.0 / static_cast<double>(cb.scale) : 0.0;
    for (size_t m = 0; m < M; ++m)
    {
        const float * sub = rhat + m * E8_SUBDIM;
        double y[E8_SUBDIM];
        double yn2 = 0.0;
        for (size_t i = 0; i < E8_SUBDIM; ++i)
        {
            y[i] = static_cast<double>(sub[i]) * inv_scale; /// to lattice units
            yn2 += y[i] * y[i];
        }

        /// Clamp into the fully-contained region so the decode is guaranteed to hit a stored point (no fallback scan).
        if (cb.use_clamp && yn2 > static_cast<double>(cb.clamp_radius) * static_cast<double>(cb.clamp_radius))
        {
            const double f = static_cast<double>(cb.clamp_radius) / std::sqrt(yn2);
            for (double & yi : y)
                yi *= f;
        }

        int c2[E8_SUBDIM];
        nearestE8(y, c2);

        UInt32 idx = 0;
        const auto * it = cb.point_to_index.find(e8Key(c2));
        if (it != nullptr)
        {
            idx = it->getMapped();
        }
        else
        {
            /// The nearest lattice point fell outside the truncated codebook (a rare large-norm sub-vector): fall back
            /// to a brute-force nearest search over the stored points.
            float best = std::numeric_limits<float>::max();
            for (size_t k = 0; k < cb.num_points; ++k)
            {
                const float * c = cb.coords.data() + k * E8_SUBDIM;
                float d = 0.0f;
                for (size_t i = 0; i < E8_SUBDIM; ++i)
                {
                    const float e = sub[i] - c[i];
                    d += e * e;
                }
                if (d < best)
                {
                    best = d;
                    idx = static_cast<UInt32>(k);
                }
            }
        }

        if (code_bytes == 1)
            dst[m] = static_cast<char>(static_cast<UInt8>(idx));
        else
        {
            const UInt16 v = static_cast<UInt16>(idx);
            std::memcpy(dst + m * 2, &v, sizeof(UInt16));
        }
    }
    std::memcpy(dst + M * code_bytes, &norm, sizeof(float));
}

/// The query side of the E8 estimator, prepared once per query: a per-subspace ADC table of inner products between the
/// (rotated, unit-normalized) query sub-vectors and every codebook point.
struct E8Query
{
    std::vector<float> lut;     /// M * num_points inner products, subspace m at offset m * num_points
    std::vector<Int8> lut_i8;   /// M * 16 int8-quantized LUT for the vpshufb fast scan (populated when num_points <= 16)
    float lut_inv_scale = 0.0f; /// converts an accumulated int sum back to a float inner product
    bool fast = false;          /// lut_i8 is populated -> the AVX-512 fast scan can be used
    size_t M = 0;
    size_t num_points = 0;
    size_t code_bytes = 0;
    bool is_l2 = false;
    float q_norm = 0.0f;        /// ||q|| of the original query (L2 only)
    float q_norm_sq = 0.0f;
};

E8Query buildE8Query(const E8Codebook & cb, const float * rqhat, size_t dimensions, size_t code_bytes, bool is_l2, float q_norm)
{
    E8Query q;
    q.M = dimensions / E8_SUBDIM;
    q.num_points = cb.num_points;
    q.code_bytes = code_bytes;
    q.is_l2 = is_l2;
    q.q_norm = q_norm;
    q.q_norm_sq = q_norm * q_norm;
    q.lut.resize(q.M * q.num_points);
    for (size_t m = 0; m < q.M; ++m)
    {
        const float * sub = rqhat + m * E8_SUBDIM;
        float * lut_m = q.lut.data() + m * q.num_points;
        for (size_t k = 0; k < q.num_points; ++k)
        {
            const float * c = cb.coords.data() + k * E8_SUBDIM;
            float d = 0.0f;
            for (size_t i = 0; i < E8_SUBDIM; ++i)
                d += sub[i] * c[i];
            lut_m[k] = d;
        }
    }

    /// For 4-bit codebooks (num_points <= 16) also build an int8-quantized LUT for the AVX-512 `vpshufb` fast scan.
    /// A single global scale maps inner products to the full int8 range; the fast scan accumulates the per-vector sum
    /// of M such values in int32, which cannot overflow within the validator's `M * 2^bits` cap.
    if (q.num_points <= 16)
    {
        float maxabs = 0.0f;
        for (float x : q.lut)
            maxabs = std::max(maxabs, std::abs(x));
        const float scale = (maxabs > 0.0f) ? (127.0f / maxabs) : 0.0f;
        q.lut_inv_scale = (scale > 0.0f) ? (1.0f / scale) : 0.0f;
        q.lut_i8.assign(q.M * 16, 0);
        for (size_t m = 0; m < q.M; ++m)
            for (size_t k = 0; k < q.num_points; ++k)
            {
                int v = static_cast<int>(std::lround(q.lut[m * q.num_points + k] * scale));
                q.lut_i8[m * 16 + k] = static_cast<Int8>(std::clamp(v, -127, 127));
            }
        q.fast = true;
    }
    return q;
}

/// ADC scan: sum the per-subspace inner products for this vector's codes, then map to the requested distance.
inline float e8Distance(const E8Query & q, const char * code)
{
    /// Codes are produced by the encoder against the same codebook, so they are always < num_points; the `min` only
    /// guards against a corrupt/foreign codes blob so a bad byte reads a valid LUT slot instead of past the end.
    const size_t last = q.num_points - 1;
    float ip = 0.0f;
    if (q.code_bytes == 1)
    {
        const UInt8 * c = reinterpret_cast<const UInt8 *>(code);
        for (size_t m = 0; m < q.M; ++m)
            ip += q.lut[m * q.num_points + std::min(static_cast<size_t>(c[m]), last)];
    }
    else
    {
        for (size_t m = 0; m < q.M; ++m)
        {
            UInt16 idx = 0;
            std::memcpy(&idx, code + m * 2, sizeof(UInt16));
            ip += q.lut[m * q.num_points + std::min(static_cast<size_t>(idx), last)];
        }
    }

    float norm_x = 0.0f;
    std::memcpy(&norm_x, code + q.M * q.code_bytes, sizeof(float));

    /// The codebook reconstruction is not unit-normalized, so the raw ADC inner product can fall outside [-1, 1].
    /// Clamp it to a valid cosine so cosineDistance stays in [0, 2] and L2Distance stays >= 0 (the optimized plan
    /// later passes the L2 value to sqrt; a negative value would produce NaN).
    ip = std::clamp(ip, -1.0f, 1.0f);

    if (q.is_l2)
        return q.q_norm_sq + norm_x * norm_x - 2.0f * q.q_norm * norm_x * ip;
    return 1.0f - ip;
}

}

namespace VectorQuantization
{

namespace
{
FlatQuantization methodToCodec(std::string_view method)
{
    if (method == "turboquant")
        return FlatQuantization::TurboQuant;
    if (method == "rabitq")
        return FlatQuantization::RaBitQ;
    if (method == "e8")
        return FlatQuantization::E8;
    if (method == "int8")
        return FlatQuantization::Int8;
    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown fastknn quantization method '{}'", method);
}
}

bool isSupportedMethod(std::string_view method)
{
    return method == "turboquant" || method == "rabitq" || method == "e8" || method == "int8";
}

size_t bytesPerVector(std::string_view method, size_t dimensions, size_t bits)
{
    return expectedFlatBytesPerVector(methodToCodec(method), dimensions, bits);
}

void encode(std::string_view method, const float * vec, size_t dimensions, size_t bits, char * dst)
{
    const FlatQuantization codec = methodToCodec(method);
    switch (codec)
    {
        case FlatQuantization::RaBitQ:
        {
            const std::vector<float> projection = generateRandomProjection(dimensions);
            const size_t padded = projection.size() / PROJECTION_ROUNDS;
            std::vector<float> work(padded);
            encodeRaBitQ(projection, vec, dimensions, work.data(), dst);
            return;
        }
        case FlatQuantization::TurboQuant:
        {
            const std::vector<float> p1 = generateRandomProjection(dimensions);
            const std::vector<float> p2 = generateRandomProjection(dimensions, RANDOM_PROJECTION_SEED_QJL);
            const size_t padded = p1.size() / PROJECTION_ROUNDS;
            std::vector<float> work1(padded);
            std::vector<float> work2(padded);
            encodeTurboQuant(p1, p2, vec, dimensions, work1.data(), work2.data(), dst);
            return;
        }
        case FlatQuantization::E8:
        {
            const std::vector<float> projection = generateRandomProjection(dimensions);
            const size_t padded = projection.size() / PROJECTION_ROUNDS;
            std::vector<float> work(padded);

            double norm_sq = 0.0;
            for (size_t i = 0; i < dimensions; ++i)
                norm_sq += static_cast<double>(vec[i]) * static_cast<double>(vec[i]);
            const float norm = static_cast<float>(std::sqrt(norm_sq));

            applyRandomProjection(projection, vec, dimensions, work.data());
            double rnorm_sq = 0.0;
            for (size_t i = 0; i < dimensions; ++i)
                rnorm_sq += static_cast<double>(work[i]) * static_cast<double>(work[i]);
            const float inv_rnorm = (rnorm_sq > 0.0) ? static_cast<float>(1.0 / std::sqrt(rnorm_sq)) : 0.0f;
            for (size_t i = 0; i < dimensions; ++i)
                work[i] *= inv_rnorm;

            auto codebook = getE8Codebook(dimensions, bits);
            const size_t code_bytes = (bits <= 8) ? 1 : 2;
            encodeE8(*codebook, work.data(), dimensions, code_bytes, norm, dst);
            return;
        }
        case FlatQuantization::Int8:
        {
            const std::vector<float> projection = generateRandomProjection(dimensions);
            const size_t padded = projection.size() / PROJECTION_ROUNDS;
            std::vector<float> work(padded);

            double norm_sq = 0.0;
            for (size_t i = 0; i < dimensions; ++i)
                norm_sq += static_cast<double>(vec[i]) * static_cast<double>(vec[i]);
            const float norm = static_cast<float>(std::sqrt(norm_sq));

            applyRandomProjection(projection, vec, dimensions, work.data());
            double rnorm_sq = 0.0;
            for (size_t i = 0; i < dimensions; ++i)
                rnorm_sq += static_cast<double>(work[i]) * static_cast<double>(work[i]);

            /// Scale the rotated vector so each kept coordinate is ~N(0, 1): unit-normalize the rotated vector (each of
            /// its `dimensions` coordinates then has variance 1/dimensions) and multiply by sqrt(dimensions). Lloyd-Max
            /// is the MSE-optimal scalar quantizer for that standard-normal coordinate distribution.
            const float scale = (rnorm_sq > 0.0)
                ? static_cast<float>(std::sqrt(static_cast<double>(dimensions) / rnorm_sq))
                : 0.0f;
            for (size_t i = 0; i < dimensions; ++i)
                dst[i] = static_cast<char>(LloydMax::quantize(work[i] * scale));
            std::memcpy(dst + dimensions, &norm, sizeof(float));
            return;
        }
    }
}

/// The query side of the 'int8' Lloyd-Max estimator: the unit-normalized rotated query direction (kept in full
/// precision - the estimator is asymmetric, only the data is quantized), plus the constants to turn the reconstructed
/// inner product into an L2 or cosine distance (mirrors `E8Query`).
struct Int8Query
{
    std::vector<float> q_dir;   /// unit-normalized rotated query, `dimensions` floats
    float inv_sqrt_d = 0.0f;    /// 1 / sqrt(dimensions): rescales the dequantized data direction back to unit norm
    bool is_l2 = false;
    float q_norm = 0.0f;        /// ||q|| of the original query (L2 only)
    float q_norm_sq = 0.0f;
    size_t dimensions = 0;
};

/// Prepared query state for computing approximate distances from codes.
struct Query
{
    FlatQuantization codec = FlatQuantization::RaBitQ;
    size_t dimensions = 0;
    size_t bits = 0;
    size_t code_bytes = 0;
    RaBitQQuery rabitq;
    TurboQuantQuery turbo;
    E8Query e8;
    std::shared_ptr<const E8Codebook> e8_codebook; /// keep the codebook alive for e8.coords
    Int8Query int8;
};

std::shared_ptr<const Query> prepareQuery(std::string_view method, const float * ref, size_t dimensions, size_t bits, bool is_l2)
{
    auto q = std::make_shared<Query>();
    q->codec = methodToCodec(method);
    q->dimensions = dimensions;
    q->bits = bits;

    double ref_norm_sq = 0.0;
    for (size_t i = 0; i < dimensions; ++i)
        ref_norm_sq += static_cast<double>(ref[i]) * static_cast<double>(ref[i]);
    const float ref_norm = static_cast<float>(std::sqrt(ref_norm_sq));

    switch (q->codec)
    {
        case FlatQuantization::RaBitQ:
        {
            q->code_bytes = dimensions / 8 + sizeof(float);
            const std::vector<float> projection = generateRandomProjection(dimensions);
            std::vector<float> work(projection.size() / PROJECTION_ROUNDS);
            applyRandomProjection(projection, ref, dimensions, work.data());
            q->rabitq = buildRaBitQQuery(work.data(), dimensions);
            break;
        }
        case FlatQuantization::TurboQuant:
        {
            q->code_bytes = dimensions / 4 + sizeof(float);
            const std::vector<float> p1 = generateRandomProjection(dimensions);
            const std::vector<float> p2 = generateRandomProjection(dimensions, RANDOM_PROJECTION_SEED_QJL);
            std::vector<Float64> ref64(ref, ref + dimensions);
            q->turbo = buildTurboQuantQuery(p1, p2, ref64.data(), dimensions);
            break;
        }
        case FlatQuantization::E8:
        {
            q->code_bytes = (bits <= 8) ? 1 : 2;
            const std::vector<float> projection = generateRandomProjection(dimensions);
            const size_t padded = projection.size() / PROJECTION_ROUNDS;
            std::vector<float> work(padded);
            applyRandomProjection(projection, ref, dimensions, work.data());
            double rnorm_sq = 0.0;
            for (size_t i = 0; i < dimensions; ++i)
                rnorm_sq += static_cast<double>(work[i]) * static_cast<double>(work[i]);
            const float inv_rnorm = (rnorm_sq > 0.0) ? static_cast<float>(1.0 / std::sqrt(rnorm_sq)) : 0.0f;
            for (size_t i = 0; i < dimensions; ++i)
                work[i] *= inv_rnorm;
            q->e8_codebook = getE8Codebook(dimensions, bits);
            q->e8 = buildE8Query(*q->e8_codebook, work.data(), dimensions, q->code_bytes, is_l2, ref_norm);
            break;
        }
        case FlatQuantization::Int8:
        {
            q->code_bytes = dimensions; /// Int8 codes; the 4-byte norm factor follows.
            const std::vector<float> projection = generateRandomProjection(dimensions);
            std::vector<float> work(projection.size() / PROJECTION_ROUNDS);
            applyRandomProjection(projection, ref, dimensions, work.data());
            double rnorm_sq = 0.0;
            for (size_t i = 0; i < dimensions; ++i)
                rnorm_sq += static_cast<double>(work[i]) * static_cast<double>(work[i]);
            const float inv_rnorm = (rnorm_sq > 0.0) ? static_cast<float>(1.0 / std::sqrt(rnorm_sq)) : 0.0f;
            q->int8.q_dir.resize(dimensions);
            for (size_t i = 0; i < dimensions; ++i)
                q->int8.q_dir[i] = work[i] * inv_rnorm;
            q->int8.inv_sqrt_d = (dimensions > 0) ? static_cast<float>(1.0 / std::sqrt(static_cast<double>(dimensions))) : 0.0f;
            q->int8.is_l2 = is_l2;
            q->int8.q_norm = ref_norm;
            q->int8.q_norm_sq = ref_norm * ref_norm;
            q->int8.dimensions = dimensions;
            break;
        }
    }
    return q;
}

float distance(const Query & q, const char * code)
{
    switch (q.codec)
    {
        case FlatQuantization::RaBitQ:
            return raBitQDistanceFast(q.rabitq, code);
        case FlatQuantization::TurboQuant:
            return turboQuantDistanceFast(q.turbo, code);
        case FlatQuantization::E8:
            return e8Distance(q.e8, code);
        case FlatQuantization::Int8:
        {
            const Int8Query & iq = q.int8;
            const Int8 * codes = reinterpret_cast<const Int8 *>(code);
            /// Reconstruct the data direction from the codes (LloydMax::dequantize(code_i) ~ sqrt(d) * data_dir_i) and
            /// take its inner product with the unit-normalized query direction; the rotation is orthogonal, so this
            /// inner product estimates cos(data, query).
            float dot = 0.0f;
            for (size_t i = 0; i < iq.dimensions; ++i)
                dot += LloydMax::dequantize(codes[i]) * iq.q_dir[i];
            float ip = std::clamp(dot * iq.inv_sqrt_d, -1.0f, 1.0f);

            float norm_x = 0.0f;
            std::memcpy(&norm_x, code + iq.dimensions, sizeof(float));

            if (iq.is_l2)
                return iq.q_norm_sq + norm_x * norm_x - 2.0f * iq.q_norm * norm_x * ip;
            return 1.0f - ip;
        }
    }
    return 0.0f;
}

}

}
