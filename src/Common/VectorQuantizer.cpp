#include <Common/VectorQuantizer.h>

#include <Common/Exception.h>
#include <Common/HadamardTransform.h>
#include <Common/HashTable/HashMap.h>
#include <Common/LloydMaxQuantizer.h>
#include <Common/TargetSpecific.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <numbers>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Quantization codecs (flat codecs, i.e. without codebook)
enum class QuantizationMethod : UInt8
{
    TurboQuant = 2, /// https://arxiv.org/abs/2504.19874
    RaBitQ = 3,     /// https://doi.org/10.1145/3654970
    Int8 = 5,
    /// Prefix: store only the leading dimensions (the prefix), quantized to int8 or bfloat16.
    /// For models with Matryoshka embeddings model, the leading bits carry most of the information, so the compression loses only little data
    PrefixInt8 = 6,
    PrefixBf16 = 7,
};

constexpr UInt64 RANDOM_PROJECTION_SEED = 0x9E3779B97F4A7C15ULL;

/// Second, independent seed for the 'turboquant' QJL (Quantized Johnson-Lindenstrauss) projection, applied to the
/// MSE-quantization residual. Must differ from RANDOM_PROJECTION_SEED so the two projections are independent.
constexpr UInt64 RANDOM_PROJECTION_SEED_QJL = 0xD1B54A32D192ED03ULL;

/// Number of (sign-flip + Hadamard) rounds. Three rounds of HD approximate a random rotation well, so the
/// resulting sign bits behave as random-hyperplane hashes (angular LSH).
constexpr int PROJECTION_ROUNDS = 3;

/// Smallest power of two >= n.
size_t projectionPaddedSize(size_t n)
{
    size_t p = 1;
    while (p < n)
        p <<= 1;
    return p;
}

/// The codec's structured projection uses only +-1 (sign-flip) Hadamard small factors. The order-9
/// Hartley factor that `HadamardTransform::kroneckerFactorFor` also supports is deliberately not used
/// here: stored codes for the affected dimensions (e.g. 1152) must stay stable across releases.
HadamardTransform::KroneckerFactor hadamardFactorFor(size_t dimensions)
{
    const auto factor = HadamardTransform::kroneckerFactorFor(dimensions);
    if (factor.kind != HadamardTransform::SmallFactorKind::Hadamard)
        return {};
    return factor;
}

/// Working dimension of the structured transform: the input dimension itself when it admits an exact Hadamard matrix
/// (`dimensions = 2^k * m`, m in {12, 20} - e.g. 384/768/1536/3072/2560), otherwise the input zero-padded to the next
/// power of two. Using the exact dimension avoids both the padding work and the geometry distortion of padding.
size_t projectionWorkingDim(size_t dimensions)
{
    return hadamardFactorFor(dimensions).m ? dimensions : projectionPaddedSize(dimensions);
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

    const auto kron = hadamardFactorFor(dimensions);
    const size_t kron_m = kron.m;
    const size_t kron_blocks = kron.blocks;
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
float raBitQNumeratorFromCounts(const RaBitQQuery & q, UInt64 pc, const UInt64 * plane_pc)
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
float raBitQNumerator(const RaBitQQuery & q, const char * code)
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

/// 'rabitq' estimator -> cosineDistance.
float raBitQDistance(const RaBitQQuery & q, const char * code)
{
    const float numerator = raBitQNumerator(q, code);
    float inv_factor = NAN;
    std::memcpy(&inv_factor, code + q.code_bytes, sizeof(float));
    /// Clamp the estimated cosine to a valid range; the unbiased RaBitQ estimator can fall outside [-1, 1] for some
    /// vectors, which would otherwise make the public `cosineDistance` estimate negative or exceed 2.
    const float cosine = std::clamp(numerator * q.inv_qnorm * inv_factor, -1.0f, 1.0f);
    return 1.0f - cosine;
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
float turboQuantDistance(const TurboQuantQuery & q, const char * code)
{
    const size_t code_bytes = q.mse.code_bytes; /// = dimensions / 8
    const float mse_dot = raBitQNumerator(q.mse, code);
    const float qjl_dot = raBitQNumerator(q.qjl, code + code_bytes);
    float gamma = NAN;
    std::memcpy(&gamma, code + 2 * code_bytes, sizeof(float));
    /// Clamp the estimated cosine so the public `cosineDistance` stays in [0, 2] (the estimator can fall outside [-1, 1]).
    const float cosine = std::clamp(q.c0 * mse_dot + q.k * gamma * qjl_dot, -1.0f, 1.0f);
    return 1.0f - cosine;
}


}

namespace
{
QuantizationMethod methodToCodec(std::string_view method)
{
    if (method == "turboquant")
        return QuantizationMethod::TurboQuant;
    if (method == "rabitq")
        return QuantizationMethod::RaBitQ;
    if (method == "int8")
        return QuantizationMethod::Int8;
    if (method == "prefix_int8")
        return QuantizationMethod::PrefixInt8;
    if (method == "prefix_bf16")
        return QuantizationMethod::PrefixBf16;
    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown quantization method '{}'", method);
}
}

bool VectorQuantizer::supportsL2(std::string_view method)
{
    /// `rabitq` and `turboquant` are sign/rotation cosine estimators. They do not retain the vector norm, so their
    /// approximate distance can only rank by ang.
    return method == "int8" || method == "prefix_int8" || method == "prefix_bf16";
}

std::string VectorQuantizer::validateParams(std::string_view method, size_t dimensions, size_t bits)
{
    const QuantizationMethod codec = methodToCodec(method);

    if (dimensions == 0)
        return "The number of dimensions must be greater than zero";

    /// Sanity-check against absurd values generated by fuzzers
    static constexpr size_t MAX_DIMENSIONS = 1u << 20; /// 1,048,576
    if (dimensions > MAX_DIMENSIONS)
        return fmt::format("the number of dimensions ({}) exceeds the maximum {}", dimensions, MAX_DIMENSIONS);

    /// 'turboquant' and 'rabitq' pack their codes 8 coordinates to the byte, They require that teh dimension is multiple of 8.
    if ((codec == QuantizationMethod::TurboQuant || codec == QuantizationMethod::RaBitQ)
        && dimensions % 8 != 0)
        return fmt::format("method '{}' requires the number of dimensions to be a multiple of 8, got {}", method, dimensions);

    /// For the prefix methods `bits` is the number of leading dimensions stored (the prefix); it must be in (0, dimensions].
    if ((codec == QuantizationMethod::PrefixInt8 || codec == QuantizationMethod::PrefixBf16) && (bits < 1 || bits > dimensions))
        return fmt::format("method '{}' requires the number of leading dimensions to be in [1, {}], got {}", method, dimensions, bits);

    return {};
}

size_t VectorQuantizer::bytesPerVector(std::string_view method, size_t dimensions, size_t bits)
{
    switch (methodToCodec(method))
    {
        case QuantizationMethod::TurboQuant:
            return dimensions / 4 + sizeof(float);
        case QuantizationMethod::RaBitQ:
            return dimensions / 8 + sizeof(float);
        case QuantizationMethod::Int8:
            /// One Int8 Lloyd-Max code per coordinate, followed by the 4-byte original L2 norm.
            return dimensions + sizeof(float);
        case QuantizationMethod::PrefixInt8:
            /// `bits` int8 codes of the leading dimensions, followed by the 4-byte per-vector scale.
            return bits + sizeof(float);
        case QuantizationMethod::PrefixBf16:
            /// `bits` leading dimensions as bfloat16 (2 bytes each).
            return bits * 2;
    }
    return 0;
}

struct VectorQuantizer::Encoder
{
    QuantizationMethod codec = QuantizationMethod::RaBitQ;
    size_t dimensions = 0;
    size_t bits = 0;
    std::vector<float> projection;   /// rabitq/int8: the (HD)^3 projection; turboquant: P1
    std::vector<float> projection2;  /// turboquant: P2 (the QJL projection)
    std::vector<float> work;         /// per-vector scratch for the rotated vector
    std::vector<float> work2;        /// turboquant: second per-vector scratch
};

void VectorQuantizer::EncoderDeleter::operator()(Encoder * ptr) const noexcept { delete ptr; }

VectorQuantizer::EncoderPtr VectorQuantizer::createEncoder(std::string_view method, size_t dimensions, size_t bits)
{
    EncoderPtr encoder(new Encoder());
    encoder->codec = methodToCodec(method);
    encoder->dimensions = dimensions;
    encoder->bits = bits;
    /// The projection is a deterministic function of (method, dimensions, seed) only, so it is generated once here and
    /// reused for every vector rather than regenerated per row - the data-independent analogue of the pq Encoder. The
    /// prefix methods need no projection.
    switch (encoder->codec)
    {
        case QuantizationMethod::RaBitQ:
        case QuantizationMethod::Int8:
            encoder->projection = generateRandomProjection(dimensions);
            encoder->work.resize(encoder->projection.size() / PROJECTION_ROUNDS);
            break;
        case QuantizationMethod::TurboQuant:
        {
            encoder->projection = generateRandomProjection(dimensions);
            encoder->projection2 = generateRandomProjection(dimensions, RANDOM_PROJECTION_SEED_QJL);
            const size_t padded = encoder->projection.size() / PROJECTION_ROUNDS;
            encoder->work.resize(padded);
            encoder->work2.resize(padded);
            break;
        }
        case QuantizationMethod::PrefixInt8:
        case QuantizationMethod::PrefixBf16:
            break;
    }
    return encoder;
}

void VectorQuantizer::encode(Encoder & encoder, const float * vec, char * dst)
{
    const size_t dimensions = encoder.dimensions;
    const size_t bits = encoder.bits;
    switch (encoder.codec)
    {
        case QuantizationMethod::RaBitQ:
            encodeRaBitQ(encoder.projection, vec, dimensions, encoder.work.data(), dst);
            return;
        case QuantizationMethod::TurboQuant:
            encodeTurboQuant(encoder.projection, encoder.projection2, vec, dimensions, encoder.work.data(), encoder.work2.data(), dst);
            return;
        case QuantizationMethod::PrefixInt8:
        {
            /// Store the leading `bits` dimensions as int8 with a per-vector symmetric scale (max|x| over the prefix),
            /// followed by the 4-byte scale. Robust to any vector magnitude. Non-finite prefix values are skipped in the
            /// scale and encoded as 0 (the full-precision vector is stored verbatim, so the exact rescore is unaffected);
            /// this keeps the encoding deterministic and avoids std::lround(NaN), whose result is implementation-defined.
            float scale = 0.0f;
            for (size_t i = 0; i < bits; ++i)
                if (std::isfinite(vec[i]))
                    scale = std::max(scale, std::abs(vec[i]));
            const float inv = (scale > 0.0f) ? 127.0f / scale : 0.0f;
            for (size_t i = 0; i < bits; ++i)
            {
                const float scaled = vec[i] * inv;
                const int q = std::isfinite(scaled) ? static_cast<int>(std::lround(scaled)) : 0;
                dst[i] = static_cast<char>(static_cast<Int8>(std::clamp(q, -127, 127)));
            }
            std::memcpy(dst + bits, &scale, sizeof(float));
            return;
        }
        case QuantizationMethod::PrefixBf16:
        {
            /// Store the leading `bits` dimensions as bfloat16 (the top 16 bits of the float32, round-to-nearest-even).
            for (size_t i = 0; i < bits; ++i)
            {
                UInt32 u = 0;
                std::memcpy(&u, &vec[i], sizeof(float));
                const UInt16 b = static_cast<UInt16>((u + 0x7FFFu + ((u >> 16) & 1u)) >> 16);
                std::memcpy(dst + i * 2, &b, sizeof(UInt16));
            }
            return;
        }
        case QuantizationMethod::Int8:
        {
            double norm_sq = 0.0;
            for (size_t i = 0; i < dimensions; ++i)
                norm_sq += static_cast<double>(vec[i]) * static_cast<double>(vec[i]);
            const float norm = static_cast<float>(std::sqrt(norm_sq));

            applyRandomProjection(encoder.projection, vec, dimensions, encoder.work.data());
            double rnorm_sq = 0.0;
            for (size_t i = 0; i < dimensions; ++i)
                rnorm_sq += static_cast<double>(encoder.work[i]) * static_cast<double>(encoder.work[i]);

            /// Scale the rotated vector so each kept coordinate is ~N(0, 1): unit-normalize the rotated vector (each of
            /// its `dimensions` coordinates then has variance 1/dimensions) and multiply by sqrt(dimensions). Lloyd-Max
            /// is the MSE-optimal scalar quantizer for that standard-normal coordinate distribution.
            const float scale = (rnorm_sq > 0.0)
                ? static_cast<float>(std::sqrt(static_cast<double>(dimensions) / rnorm_sq))
                : 0.0f;
            for (size_t i = 0; i < dimensions; ++i)
                dst[i] = static_cast<char>(LloydMax::quantize(encoder.work[i] * scale));
            std::memcpy(dst + dimensions, &norm, sizeof(float));
            return;
        }
    }
}

void VectorQuantizer::encode(std::string_view method, const float * vec, size_t dimensions, size_t bits, char * dst)
{
    auto encoder = createEncoder(method, dimensions, bits);
    encode(*encoder, vec, dst);
}

/// The query side of the 'int8' Lloyd-Max estimator: the unit-normalized rotated query direction (kept in full
/// precision - the estimator is asymmetric, only the data is quantized), plus the constants to turn the reconstructed
/// inner product into an L2 or cosine distance.
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
struct VectorQuantizer::Query
{
    QuantizationMethod codec = QuantizationMethod::RaBitQ;
    size_t dimensions = 0;
    size_t bits = 0;
    size_t code_bytes = 0;
    RaBitQQuery rabitq;
    TurboQuantQuery turbo;
    Int8Query int8;
    /// `prefix`: the leading `bits` dimensions of the query (full precision), its prefix norm, and the metric.
    std::vector<float> mrl_query;
    float mrl_query_norm = 0.0f;
    bool is_l2 = false;
};

void VectorQuantizer::QueryDeleter::operator()(const Query * ptr) const noexcept { delete ptr; }

VectorQuantizer::QueryPtr VectorQuantizer::prepareQuery(std::string_view method, const float * ref, size_t dimensions, size_t bits, bool is_l2)
{
    auto query = std::make_unique<Query>();
    query->codec = methodToCodec(method);
    query->dimensions = dimensions;
    query->bits = bits;

    double ref_norm_sq = 0.0;
    for (size_t i = 0; i < dimensions; ++i)
        ref_norm_sq += static_cast<double>(ref[i]) * static_cast<double>(ref[i]);
    const float ref_norm = static_cast<float>(std::sqrt(ref_norm_sq));

    switch (query->codec)
    {
        case QuantizationMethod::RaBitQ:
        {
            query->code_bytes = dimensions / 8 + sizeof(float);
            const std::vector<float> projection = generateRandomProjection(dimensions);
            std::vector<float> work(projection.size() / PROJECTION_ROUNDS);
            applyRandomProjection(projection, ref, dimensions, work.data());
            query->rabitq = buildRaBitQQuery(work.data(), dimensions);
            break;
        }
        case QuantizationMethod::TurboQuant:
        {
            query->code_bytes = dimensions / 4 + sizeof(float);
            const std::vector<float> p1 = generateRandomProjection(dimensions);
            const std::vector<float> p2 = generateRandomProjection(dimensions, RANDOM_PROJECTION_SEED_QJL);
            std::vector<Float64> ref64(ref, ref + dimensions);
            query->turbo = buildTurboQuantQuery(p1, p2, ref64.data(), dimensions);
            break;
        }
        case QuantizationMethod::PrefixInt8:
        case QuantizationMethod::PrefixBf16:
        {
            /// Distance is computed on the leading `bits` dimensions only (the stored prefix), against the query's prefix.
            query->code_bytes = (query->codec == QuantizationMethod::PrefixInt8) ? bits : bits * 2;
            query->mrl_query.assign(ref, ref + bits);
            double pn = 0.0;
            for (size_t i = 0; i < bits; ++i)
                pn += static_cast<double>(ref[i]) * static_cast<double>(ref[i]);
            query->mrl_query_norm = static_cast<float>(std::sqrt(pn));
            query->is_l2 = is_l2;
            break;
        }
        case QuantizationMethod::Int8:
        {
            query->code_bytes = dimensions; /// Int8 codes; the 4-byte norm factor follows.
            const std::vector<float> projection = generateRandomProjection(dimensions);
            std::vector<float> work(projection.size() / PROJECTION_ROUNDS);
            applyRandomProjection(projection, ref, dimensions, work.data());
            double rnorm_sq = 0.0;
            for (size_t i = 0; i < dimensions; ++i)
                rnorm_sq += static_cast<double>(work[i]) * static_cast<double>(work[i]);
            const float inv_rnorm = (rnorm_sq > 0.0) ? static_cast<float>(1.0 / std::sqrt(rnorm_sq)) : 0.0f;
            query->int8.q_dir.resize(dimensions);
            for (size_t i = 0; i < dimensions; ++i)
                query->int8.q_dir[i] = work[i] * inv_rnorm;
            query->int8.inv_sqrt_d = (dimensions > 0) ? static_cast<float>(1.0 / std::sqrt(static_cast<double>(dimensions))) : 0.0f;
            query->int8.is_l2 = is_l2;
            query->int8.q_norm = ref_norm;
            query->int8.q_norm_sq = ref_norm * ref_norm;
            query->int8.dimensions = dimensions;
            break;
        }
    }
    return QueryPtr(query.release());
}

float VectorQuantizer::distance(const Query & query, const char * code)
{
    switch (query.codec)
    {
        case QuantizationMethod::RaBitQ:
            return raBitQDistance(query.rabitq, code);
        case QuantizationMethod::TurboQuant:
            return turboQuantDistance(query.turbo, code);
        case QuantizationMethod::PrefixInt8:
        case QuantizationMethod::PrefixBf16:
        {
            /// Decode the stored prefix and compute L2 (squared, monotone for ranking) or cosine distance against the
            /// query's prefix.
            const size_t n = query.mrl_query.size();
            auto decoded = [&](size_t i) -> float
            {
                if (query.codec == QuantizationMethod::PrefixInt8)
                {
                    float scale = 0.0f;
                    std::memcpy(&scale, code + n, sizeof(float));
                    return static_cast<float>(static_cast<Int8>(code[i])) * scale / 127.0f;
                }
                UInt16 b = 0;
                std::memcpy(&b, code + i * 2, sizeof(UInt16));
                UInt32 u = static_cast<UInt32>(b) << 16;
                float f = 0.0f;
                std::memcpy(&f, &u, sizeof(float));
                return f;
            };

            if (query.is_l2)
            {
                float d = 0.0f;
                for (size_t i = 0; i < n; ++i)
                {
                    const float e = query.mrl_query[i] - decoded(i);
                    d += e * e;
                }
                return d; /// squared L2 over the prefix; monotone in the true prefix distance for ranking
            }

            float dot = 0.0f;
            float xn2 = 0.0f;
            for (size_t i = 0; i < n; ++i)
            {
                const float x = decoded(i);
                dot += query.mrl_query[i] * x;
                xn2 += x * x;
            }
            const float denom = query.mrl_query_norm * std::sqrt(xn2);
            const float cosine = (denom > 0.0f) ? std::clamp(dot / denom, -1.0f, 1.0f) : 0.0f;
            return 1.0f - cosine;
        }
        case QuantizationMethod::Int8:
        {
            const Int8Query & iq = query.int8;
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

/// Named (not anonymous) so the TargetSpecific::* namespaces the macro below generates cannot collide with
/// identically named kernels from another translation unit.
namespace RaBitQIndexImpl
{
namespace
{

/// Codes are interleaved LANES at a time, so one SIMD register holds the same 64-bit word of LANES
/// different codes and the popcount accumulator has one lane per code - no horizontal reduction per code.
/// 8 lanes fill a 512-bit register.
constexpr size_t LANES = 8;

/// `__builtin_elementwise_popcount` lowers to `vpopcntq` with AVX-512 VPOPCNTDQ (`x86_64_icelake` and up,
/// including Zen 4 and 5), a `vpshufb` nibble table on AVX2 and plain AVX-512, NEON `cnt` on ARM.
using LaneU64 = UInt64 __attribute__((vector_size(LANES * sizeof(UInt64))));
using LaneF32 = Float32 __attribute__((vector_size(LANES * sizeof(Float32))));

/// Groups per emitted minimum. Keeping the running minimum costs one vector `min` per group, but collapsing
/// it to a scalar is a shuffle chain: at 768 dimensions one scalar minimum per group costs 60% on top of the
/// scan, one per 16 groups costs 6%.
constexpr size_t MINIMA_WINDOW = 16;

/// `DECLARE_MULTITARGET_CODE` stops at `x86_64_v4`, which has no VPOPCNTDQ. That instruction is worth about
/// 2.6x here, so the Ice Lake tier gets its own copy of the body.
#define DECLARE_MULTITARGET_CODE_WITH_ICELAKE(...) \
DECLARE_MULTITARGET_CODE(__VA_ARGS__) \
DECLARE_X86_ICELAKE_SPECIFIC_CODE(__VA_ARGS__)

DECLARE_MULTITARGET_CODE_WITH_ICELAKE(

/// Score one query against `groups * LANES` codes, writing one estimated squared L2 distance per code.
///
/// Word `w` of the `LANES` codes in group `g` is at `codes[(g * words + w) * LANES]`. `planes` is the
/// bit-sliced query, `RABITQ_QUERY_BITS` planes of `words` words. `popcounts`, `sq_norms` and `factors` hold
/// `groups * LANES` entries, one per code.
///
/// Also writes the smallest estimate of each window of MINIMA_WINDOW groups to `window_minima`
/// (`ceil(groups / MINIMA_WINDOW)` entries), so the caller can reject `MINIMA_WINDOW * LANES` codes against
/// its running k-th best with one comparison. It has to happen here: float minimum reassociates no more than
/// float addition does, so a minimum taken in the caller stays a scalar chain.
///
/// The estimator is the one in `raBitQDistance`, rearranged so everything independent of the code is folded
/// by the caller into three scalars. With `weighted = sum_j 2^j * popcount(code AND plane_j)` and
/// `pc = popcount(code)`,
///     sum_i s_i q_i = 2 * (delta * weighted + q_min * pc) - q_total
///     ||x - c||^2 = ||x||^2 + ||c||^2 - 2 (x . c), (x . c) ~= cos(x, c) * ||x|| * ||c||
/// so with `factors[c] = ||c|| * inv_factor` and `qa`, `qb`, `qc` carrying delta, q_min and q_total (see
/// `prepareScanQuery`), the result is `||c||^2 - 2 (x . c)` - the squared distance less `||x||^2`, which is
/// constant across codes and so ranks the same. The estimated cosine is not clamped to [-1, 1] as
/// `raBitQDistance` clamps it: that would flatten the near-ties this ranking has to resolve.
void scanCodes(
    const UInt64 * __restrict codes, size_t groups, size_t words, const UInt64 * __restrict planes,
    const Float32 * __restrict popcounts, const Float32 * __restrict sq_norms, const Float32 * __restrict factors,
    Float32 qa, Float32 qb, Float32 qc, Float32 * __restrict scores, Float32 * __restrict window_minima)
{
    static_assert(RABITQ_QUERY_BITS == 4, "scanCodes is unrolled over exactly four query bit-planes");
    const UInt64 * __restrict plane0 = planes;
    const UInt64 * __restrict plane1 = planes + words;
    const UInt64 * __restrict plane2 = planes + 2 * words;
    const UInt64 * __restrict plane3 = planes + 3 * words;

    LaneF32 window_min = LaneF32{} + std::numeric_limits<Float32>::infinity();
    for (size_t group = 0; group < groups; ++group)
    {
        LaneU64 acc0{};
        LaneU64 acc1{};
        LaneU64 acc2{};
        LaneU64 acc3{};
        const UInt64 * __restrict group_codes = codes + group * words * LANES;
        for (size_t word = 0; word < words; ++word)
        {
            /// Copied, not dereferenced: the lane type wants an alignment the buffer does not promise.
            LaneU64 code;
            __builtin_memcpy(&code, group_codes + word * LANES, sizeof(code));
            acc0 += __builtin_elementwise_popcount(code & (plane0[word] - LaneU64{}));
            acc1 += __builtin_elementwise_popcount(code & (plane1[word] - LaneU64{}));
            acc2 += __builtin_elementwise_popcount(code & (plane2[word] - LaneU64{}));
            acc3 += __builtin_elementwise_popcount(code & (plane3[word] - LaneU64{}));
        }

        const LaneU64 weighted = acc0 + (acc1 << 1) + (acc2 << 2) + (acc3 << 3);
        LaneF32 lane_popcounts;
        LaneF32 lane_sq_norms;
        LaneF32 lane_factors;
        __builtin_memcpy(&lane_popcounts, popcounts + group * LANES, sizeof(lane_popcounts));
        __builtin_memcpy(&lane_sq_norms, sq_norms + group * LANES, sizeof(lane_sq_norms));
        __builtin_memcpy(&lane_factors, factors + group * LANES, sizeof(lane_factors));

        const LaneF32 estimate = lane_sq_norms
            - lane_factors * (qa * __builtin_convertvector(weighted, LaneF32) + qb * lane_popcounts + qc);
        __builtin_memcpy(scores + group * LANES, &estimate, sizeof(estimate));

        window_min = __builtin_elementwise_min(window_min, estimate);
        if ((group + 1) % MINIMA_WINDOW == 0 || group + 1 == groups)
        {
            /// Spelled out because `__builtin_reduce_min` lowers to far more than the three
            /// shuffle-and-minimum steps a power-of-two width needs.
            LaneF32 folded = __builtin_elementwise_min(window_min, __builtin_shufflevector(window_min, window_min, 4, 5, 6, 7, 0, 1, 2, 3));
            folded = __builtin_elementwise_min(folded, __builtin_shufflevector(folded, folded, 2, 3, 0, 1, 6, 7, 4, 5));
            folded = __builtin_elementwise_min(folded, __builtin_shufflevector(folded, folded, 1, 0, 3, 2, 5, 4, 7, 6));
            window_minima[group / MINIMA_WINDOW] = folded[0];
            window_min = LaneF32{} + std::numeric_limits<Float32>::infinity();
        }
    }
}

) // DECLARE_MULTITARGET_CODE_WITH_ICELAKE

#undef DECLARE_MULTITARGET_CODE_WITH_ICELAKE

/// Runtime dispatch to the widest ISA the CPU supports. Where multitarget code is off (ARM, or
/// ENABLE_MULTITARGET_CODE=OFF) only Default exists, which is why the body above is written with vector
/// extensions rather than intrinsics.
void scanCodes(
    const UInt64 * codes, size_t groups, size_t words, const UInt64 * planes,
    const Float32 * popcounts, const Float32 * sq_norms, const Float32 * factors,
    Float32 qa, Float32 qb, Float32 qc, Float32 * scores, Float32 * window_minima)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_icelake))
    {
        TargetSpecific::x86_64_icelake::scanCodes(codes, groups, words, planes, popcounts, sq_norms, factors, qa, qb, qc, scores, window_minima);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        TargetSpecific::x86_64_v4::scanCodes(codes, groups, words, planes, popcounts, sq_norms, factors, qa, qb, qc, scores, window_minima);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::scanCodes(codes, groups, words, planes, popcounts, sq_norms, factors, qa, qb, qc, scores, window_minima);
        return;
    }
#endif
    TargetSpecific::Default::scanCodes(codes, groups, words, planes, popcounts, sq_norms, factors, qa, qb, qc, scores, window_minima);
}

/// Bytes of codes to score before returning to the first query of the chunk, sized to keep a tile in L2
/// across the whole chunk. Without it every query re-reads the entire code array and a large index runs at
/// L3 or DRAM bandwidth.
constexpr size_t CODE_TILE_BYTES = 384 * 1024;

/// Queries in flight. Their planes and candidate buffers share L2 with the code tile, so this trades cache
/// footprint against how well the tile is amortized.
constexpr size_t QUERY_CHUNK = 128;

/// The `k` smallest estimates seen so far for one query.
///
/// Not a heap: almost every code is rejected by the single comparison against `threshold`, which both
/// structures do equally well, and for the few hundred that get past it a heap pays a sift per insertion.
/// Appending into a buffer of twice the size and partitioning when it fills costs a handful of
/// `nth_element` calls per query instead. A heap here spent a fifth of the scan in `sift_down`.
struct CandidateBuffer
{
    std::vector<std::pair<Float32, UInt32>> entries;
    size_t k = 0;
    Float32 limit = std::numeric_limits<Float32>::infinity();

    void reset(size_t k_)
    {
        k = k_;
        limit = std::numeric_limits<Float32>::infinity();
        entries.clear();
        entries.reserve(2 * k);
    }

    /// Only an estimate below this can still reach the final `k`; infinite until the buffer first fills.
    Float32 threshold() const { return limit; }

    void add(Float32 estimate, UInt32 position)
    {
        entries.emplace_back(estimate, position);
        if (entries.size() == 2 * k)
            shrinkToK();
    }

    /// Keep the `k` smallest and take the largest of them as the new threshold. Nothing at or above the old
    /// threshold was ever added, so these really are the `k` smallest seen.
    void shrinkToK()
    {
        /// Compare the estimate only; the default `std::pair` comparison drags in the position for no
        /// benefit, and it is not free.
        std::nth_element(entries.begin(), entries.begin() + (k - 1), entries.end(),
            [](const auto & a, const auto & b) { return a.first < b.first; });
        limit = entries[k - 1].first;
        entries.resize(k);
    }
};

/// Rotate a query the way the codes were rotated, bit-slice it into `planes` (RABITQ_QUERY_BITS planes of
/// `words` words) and fold the rest of the estimator into the `qa`, `qb`, `qc` that `scanCodes` takes.
/// `work` is scratch of at least the projection's working size.
void prepareScanQuery(
    const std::vector<float> & projection, const float * query, size_t dimensions, size_t words,
    std::vector<float> & work, UInt64 * planes, std::array<Float32, 3> & scalars)
{
    const size_t code_bytes = dimensions / 8;
    applyRandomProjection(projection, query, dimensions, work.data());
    const RaBitQQuery prepared = buildRaBitQQuery(work.data(), dimensions);

    /// Repack the plane bytes as words, in the same layout as the codes.
    for (int bit = 0; bit < RABITQ_QUERY_BITS; ++bit)
        for (size_t word = 0; word < words; ++word)
        {
            const size_t offset = word * sizeof(UInt64);
            UInt64 value = 0;
            std::memcpy(&value, prepared.planes.data() + static_cast<size_t>(bit) * code_bytes + offset,
                std::min(sizeof(UInt64), code_bytes - offset));
            planes[static_cast<size_t>(bit) * words + word] = value;
        }

    /// `scale` turns the estimated cosine into twice the estimated inner product, the form the distance
    /// expansion needs. A zero-length query leaves it zero and the estimate reduces to `||c||^2`, which is
    /// the right answer for the origin.
    double query_sq_norm = 0.0;
    for (size_t coord = 0; coord < dimensions; ++coord)
        query_sq_norm += static_cast<double>(query[coord]) * static_cast<double>(query[coord]);
    const Float32 scale = 2.0f * static_cast<Float32>(std::sqrt(query_sq_norm)) * prepared.inv_qnorm;

    scalars = {2.0f * scale * prepared.delta, 2.0f * scale * prepared.q_min, -scale * prepared.q_total};
}

}
}

bool RaBitQIndex::supportsDimensions(size_t dimensions)
{
    return dimensions > 0 && dimensions % 8 == 0;
}

struct RaBitQIndex::Data
{
    size_t count = 0;
    size_t dimensions = 0;
    size_t words = 0;   /// UInt64 words per code
    size_t groups = 0;  /// `count` rounded up to whole groups of LANES

    /// Shared by the codes and every query, which have to be rotated the same way.
    std::vector<float> projection;

    /// The interleaved codes, `groups * words * LANES` words. See `scanCodes`.
    std::vector<UInt64> codes;

    /// The three per-code scalars the estimator needs, `groups * LANES` entries each. Entries past `count`
    /// pad the last group; an infinite squared norm and a zero factor keep their estimate infinite.
    std::vector<Float32> popcounts;  /// popcount of the code, which does not depend on the query
    std::vector<Float32> sq_norms;   /// ||v||^2
    std::vector<Float32> factors;    /// ||v|| * inv_factor, the data half of the cosine estimator
};

RaBitQIndex::RaBitQIndex(const float * vectors, size_t count, size_t dimensions)
    : data(std::make_unique<Data>())
{
    using namespace RaBitQIndexImpl;

    if (!supportsDimensions(dimensions))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "RaBitQIndex: the dimension must be a non-zero multiple of 8, got {}", dimensions);
    if (count == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RaBitQIndex: cannot index an empty set of vectors");

    const size_t code_bytes = dimensions / 8;
    data->count = count;
    data->dimensions = dimensions;
    data->words = (code_bytes + sizeof(UInt64) - 1) / sizeof(UInt64);
    data->groups = (count + LANES - 1) / LANES;

    data->projection = generateRandomProjection(dimensions);
    data->codes.assign(data->groups * data->words * LANES, 0);
    data->popcounts.assign(data->groups * LANES, 0.0f);
    data->sq_norms.assign(data->groups * LANES, std::numeric_limits<Float32>::infinity());
    data->factors.assign(data->groups * LANES, 0.0f);

    std::vector<float> work(data->projection.size() / PROJECTION_ROUNDS);
    std::vector<char> code(code_bytes + sizeof(float));

    for (size_t i = 0; i < count; ++i)
    {
        const float * vector = vectors + i * dimensions;
        encodeRaBitQ(data->projection, vector, dimensions, work.data(), code.data());

        /// Scatter the packed sign bits into the interleaved layout. The last word is only partly covered
        /// when the dimension is not a multiple of 64; the rest stays zero and contributes no popcount, and
        /// neither does the matching padding in the query planes.
        UInt64 * slot = data->codes.data() + (i / LANES) * data->words * LANES + i % LANES;
        UInt64 popcount = 0;
        for (size_t word = 0; word < data->words; ++word)
        {
            const size_t offset = word * sizeof(UInt64);
            UInt64 value = 0;
            std::memcpy(&value, code.data() + offset, std::min(sizeof(UInt64), code_bytes - offset));
            slot[word * LANES] = value;
            popcount += static_cast<UInt64>(std::popcount(value));
        }

        float inv_factor = 0.0f;
        std::memcpy(&inv_factor, code.data() + code_bytes, sizeof(float));

        double sq_norm = 0.0;
        for (size_t coord = 0; coord < dimensions; ++coord)
            sq_norm += static_cast<double>(vector[coord]) * static_cast<double>(vector[coord]);

        data->popcounts[i] = static_cast<Float32>(popcount);
        data->sq_norms[i] = static_cast<Float32>(sq_norm);
        data->factors[i] = static_cast<Float32>(std::sqrt(sq_norm)) * inv_factor;
    }
}

RaBitQIndex::~RaBitQIndex() = default;

size_t RaBitQIndex::size() const
{
    return data->count;
}

void RaBitQIndex::nearestByL2(const float * queries, size_t num_queries, size_t k, UInt32 * out) const
{
    using namespace RaBitQIndexImpl;

    if (k == 0 || k > data->count)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "RaBitQIndex: asked for the {} nearest of {} indexed vectors", k, data->count);

    const size_t count = data->count;
    const size_t dimensions = data->dimensions;
    const size_t words = data->words;
    const size_t tile_groups = std::max<size_t>(1, CODE_TILE_BYTES / (words * LANES * sizeof(UInt64)));

    std::vector<float> work(data->projection.size() / PROJECTION_ROUNDS);
    /// Per query in the chunk: the bit-sliced planes and the three scalars `scanCodes` folds the rest into.
    std::vector<UInt64> planes(QUERY_CHUNK * RABITQ_QUERY_BITS * words);
    std::vector<std::array<Float32, 3>> scalars(QUERY_CHUNK);
    std::vector<CandidateBuffer> buffers(QUERY_CHUNK);
    std::vector<Float32> scores(tile_groups * LANES);
    std::vector<Float32> window_minima((tile_groups + MINIMA_WINDOW - 1) / MINIMA_WINDOW);

    for (size_t chunk_start = 0; chunk_start < num_queries; chunk_start += QUERY_CHUNK)
    {
        const size_t chunk = std::min(QUERY_CHUNK, num_queries - chunk_start);

        for (size_t q = 0; q < chunk; ++q)
        {
            prepareScanQuery(data->projection, queries + (chunk_start + q) * dimensions, dimensions, words,
                work, planes.data() + q * RABITQ_QUERY_BITS * words, scalars[q]);
            buffers[q].reset(k);
        }

        for (size_t tile_start = 0; tile_start < data->groups; tile_start += tile_groups)
        {
            const size_t tile = std::min(tile_groups, data->groups - tile_start);
            const size_t first = tile_start * LANES;

            for (size_t q = 0; q < chunk; ++q)
            {
                scanCodes(
                    data->codes.data() + tile_start * words * LANES, tile, words,
                    planes.data() + q * RABITQ_QUERY_BITS * words,
                    data->popcounts.data() + first, data->sq_norms.data() + first, data->factors.data() + first,
                    scalars[q][0], scalars[q][1], scalars[q][2], scores.data(), window_minima.data());

                /// One window of MINIMA_WINDOW * LANES codes at a time, so the common case - nothing in
                /// the window beats the running k-th best - costs one comparison instead of one per code.
                CandidateBuffer & buffer = buffers[q];
                for (size_t window = 0; window * MINIMA_WINDOW < tile; ++window)
                {
                    if (!(window_minima[window] < buffer.threshold()))
                        continue;
                    const size_t from = window * MINIMA_WINDOW * LANES;
                    const size_t to = std::min((window + 1) * MINIMA_WINDOW, tile) * LANES;
                    for (size_t i = from; i < to; ++i)
                        if (scores[i] < buffer.threshold() && first + i < count)
                            buffer.add(scores[i], static_cast<UInt32>(first + i));
                }
            }
        }

        for (size_t q = 0; q < chunk; ++q)
        {
            /// The buffer holds between `k` and `2k - 1` entries unless it happened to fill exactly.
            if (buffers[q].entries.size() > k)
                buffers[q].shrinkToK();
            UInt32 * row = out + (chunk_start + q) * k;
            for (size_t i = 0; i < k; ++i)
                row[i] = buffers[q].entries[i].second;
        }
    }
}

size_t RaBitQIndex::nearestByL2InRanges(
    const float * query, const PositionRange * ranges, size_t num_ranges, size_t k, UInt32 * out) const
{
    using namespace RaBitQIndexImpl;

    if (k == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RaBitQIndex: asked for the 0 nearest");

    const size_t count = data->count;
    const size_t words = data->words;

    std::vector<float> work(data->projection.size() / PROJECTION_ROUNDS);
    std::vector<UInt64> planes(static_cast<size_t>(RABITQ_QUERY_BITS) * words);
    std::array<Float32, 3> scalars{};
    prepareScanQuery(data->projection, query, data->dimensions, words, work, planes.data(), scalars);

    CandidateBuffer buffer;
    buffer.reset(k);

    /// The scan works whole groups, so a range is widened to the groups it touches and the positions that
    /// widening pulled in are dropped below. Ranges aligned to LANES lose nothing to this.
    std::vector<Float32> scores;
    std::vector<Float32> window_minima;
    for (size_t r = 0; r < num_ranges; ++r)
    {
        const size_t begin = ranges[r].begin;
        const size_t end = std::min<size_t>(ranges[r].end, count);
        if (begin >= end)
            continue;
        if (end > count)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "RaBitQIndex: range [{}, {}) is outside the {} indexed vectors", begin, ranges[r].end, count);

        const size_t first_group = begin / LANES;
        const size_t groups = (end + LANES - 1) / LANES - first_group;
        scores.resize(groups * LANES);
        window_minima.resize((groups + MINIMA_WINDOW - 1) / MINIMA_WINDOW);

        scanCodes(
            data->codes.data() + first_group * words * LANES, groups, words, planes.data(),
            data->popcounts.data() + first_group * LANES, data->sq_norms.data() + first_group * LANES,
            data->factors.data() + first_group * LANES,
            scalars[0], scalars[1], scalars[2], scores.data(), window_minima.data());

        const size_t first = first_group * LANES;
        for (size_t window = 0; window * MINIMA_WINDOW < groups; ++window)
        {
            if (!(window_minima[window] < buffer.threshold()))
                continue;
            const size_t from = window * MINIMA_WINDOW * LANES;
            const size_t to = std::min((window + 1) * MINIMA_WINDOW, groups) * LANES;
            for (size_t i = from; i < to; ++i)
                if (const size_t position = first + i; position >= begin && position < end
                    && scores[i] < buffer.threshold())
                    buffer.add(scores[i], static_cast<UInt32>(position));
        }
    }

    if (buffer.entries.size() > k)
        buffer.shrinkToK();
    const size_t written = std::min(k, buffer.entries.size());
    for (size_t i = 0; i < written; ++i)
        out[i] = buffer.entries[i].second;
    return written;
}

}
