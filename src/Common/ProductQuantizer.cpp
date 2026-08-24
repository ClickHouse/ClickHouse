#include <Common/ProductQuantizer.h>

#include <Common/Exception.h>

#include <DataTypes/DataTypeFixedString.h>
#include <base/defines.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <string>

#include <fmt/format.h>

namespace DB
{

namespace
{

/// Deterministic splitmix64 -> uniform size_t in [0, bound).
UInt64 splitmix64(UInt64 & state)
{
    UInt64 z = (state += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

size_t randIndex(UInt64 & state, size_t bound)
{
    return bound ? static_cast<size_t>(splitmix64(state) % bound) : 0;
}

/// Squared L2 distance between two d-dim float vectors.
double sqDist(const float * a, const float * b, size_t d)
{
    double s = 0.0;
    for (size_t i = 0; i < d; ++i)
    {
        const double e = static_cast<double>(a[i]) - static_cast<double>(b[i]);
        s += e * e;
    }
    return s;
}

/// Scalar double nearest-centroid argmin over row-major centroids. Used only by k-means training (which runs on a
/// bounded reservoir, not the hot per-row encode path): keeping it in double makes the learned codebook independent of
/// the SIMD float encode kernel, so encoding stays a near-lossless re-expression of the same argmin.
size_t nearestCentroidScalar(const float * centroids, size_t k, size_t d_sub, const float * sub)
{
    size_t best = 0;
    double best_d = std::numeric_limits<double>::max();
    for (size_t c = 0; c < k; ++c)
    {
        const double d = sqDist(sub, centroids + c * d_sub, d_sub);
        if (d < best_d)
        {
            best_d = d;
            best = c;
        }
    }
    return best;
}

/// Squared norm ||c||^2 of each of the `k` centroids (each `d_sub` floats); double accumulation for stability. Computed
/// once per codebook in `createEncoder`, so it stays off the hot per-vector path.
void centroidSquaredNorms(const float * centroids, size_t k, size_t d_sub, float * out)
{
    for (size_t c = 0; c < k; ++c)
    {
        const float * cen = centroids + c * d_sub;
        double n2 = 0.0;
        for (size_t i = 0; i < d_sub; ++i)
            n2 += static_cast<double>(cen[i]) * static_cast<double>(cen[i]);
        out[c] = static_cast<float>(n2);
    }
}

/// Transpose one subspace's `k` centroids from row-major (centroid c at `src + c * d_sub`) to the column-major layout
/// `dst[i * k + c]` (all centroids' coordinate i contiguous) that the nearest-centroid kernel scans across. Done once
/// per codebook in `createEncoder`, so it stays off the hot per-vector path.
void transposeCentroids(const float * src, size_t k, size_t d_sub, float * dst)
{
    for (size_t c = 0; c < k; ++c)
        for (size_t i = 0; i < d_sub; ++i)
            dst[i * k + c] = src[c * d_sub + i];
}

/// Index of the nearest of `k` centroids to `sub`, via the reformulated argmin
///   argmin_c ||x - c||^2 = argmin_c (||c||^2 - 2 x.c)
/// (||x||^2 is constant across centroids and dropped). With the centroids stored column-major (`centroids_t[i*k+c]`),
/// the dot products of `sub` against ALL k centroids accumulate in parallel into `acc[0..k)` with no per-centroid
/// horizontal reduction: each dimension i broadcasts `sub[i]` and does one fused-multiply-add across the k lanes. The
/// target-specific build maps this onto AVX-512/AVX2. Centroid norms are precomputed in `cb_sqnorm`. `acc` is caller
/// scratch of `k` floats. This is the hot per-row encode path.
size_t nearestCentroidImpl(
    const float * __restrict centroids_t,
    const float * __restrict cb_sqnorm,
    size_t k,
    size_t d_sub,
    const float * __restrict sub,
    float * __restrict acc)
{
    for (size_t c = 0; c < k; ++c)
        acc[c] = 0.0f;
    for (size_t i = 0; i < d_sub; ++i)
    {
        const float xi = sub[i];
        const float * __restrict col = centroids_t + i * k;
        for (size_t c = 0; c < k; ++c)
            acc[c] += xi * col[c];
    }

    size_t best = 0;
    float best_score = std::numeric_limits<float>::max();
    for (size_t c = 0; c < k; ++c)
    {
        const float score = cb_sqnorm[c] - 2.0f * acc[c];
        if (score < best_score)
        {
            best_score = score;
            best = c;
        }
    }
    return best;
}

/// Dispatch the nearest-centroid kernel to the widest supported ISA (decided per call; cheap relative to the k*d_sub work).
size_t nearestCentroid(
    const float * centroids_t, const float * cb_sqnorm, size_t k, size_t d_sub, const float * sub, float * acc)
{
    return nearestCentroidImpl(centroids_t, cb_sqnorm, k, d_sub, sub, acc);
}

constexpr int KMEANS_ITERATIONS = 20;

}

size_t ProductQuantizer::numCentroids(size_t nbits)
{
    return static_cast<size_t>(1) << nbits;
}

size_t ProductQuantizer::codebookFloats(size_t dimensions, size_t /*m*/, size_t nbits)
{
    /// m * k * d_sub = k * dimensions (m cancels).
    return numCentroids(nbits) * dimensions;
}

size_t ProductQuantizer::bytesPerVector(size_t /*dimensions*/, size_t m, size_t nbits)
{
    return m * (nbits <= 8 ? 1 : 2);
}

std::string ProductQuantizer::validateParams(size_t dimensions, size_t m, size_t nbits)
{
    if (dimensions == 0)
        return "the number of dimensions must be greater than zero";

    /// Sanity-check against absurd values generated by fuzzers
    static constexpr size_t MAX_DIMENSIONS = 1u << 20; /// 1,048,576
    if (dimensions > MAX_DIMENSIONS)
        return fmt::format("the number of dimensions ({}) exceeds the maximum {}", dimensions, MAX_DIMENSIONS);

    if (m == 0)
        return "the number of subspaces (m) must be greater than zero";

    if (dimensions % m != 0)
        return fmt::format("the number of dimensions ({}) must be a multiple of the number of subspaces m ({})", dimensions, m);

    if (nbits < 1 || nbits > 16)
        return fmt::format("nbits must be in the range [1, 16], got {}", nbits);

    /// The codebook is exposed as a `FixedString(codebookFloats * 4)`. Reject parameter combinations whose
    /// codebook would exceed the FixedString size limit, otherwise DDL passes this validator and only later fails with a generic
    /// "FixedString size is too large". The product cannot overflow here: dimensions <= 2^20 and 2^nbits <= 2^16
    /// so codebookFloats * 4 <= 2^38.
    const size_t codebook_bytes = codebookFloats(dimensions, m, nbits) * sizeof(float);
    if (codebook_bytes > MAX_FIXEDSTRING_SIZE)
        return fmt::format(
            "the codebook for these parameters would be {} bytes, exceeding the maximum {} (reduce nbits or dimensions)",
            codebook_bytes, MAX_FIXEDSTRING_SIZE);

    return {};
}

std::vector<float> ProductQuantizer::trainCodebook(const float * vectors, size_t n, size_t dimensions, size_t m, size_t nbits, UInt64 seed)
{
    const size_t d_sub = dimensions / m;
    const size_t k = numCentroids(nbits);

    std::vector<float> codebook(m * k * d_sub, 0.0f);
    if (n == 0)
        return codebook;

    UInt64 rng = seed + 0x123456789ABCDEFULL;

    /// Scratch for one subspace's k-means.
    std::vector<float> centroids(k * d_sub);
    std::vector<double> sums(k * d_sub);
    std::vector<size_t> counts(k);
    std::vector<size_t> assign(n);

    for (size_t mm = 0; mm < m; ++mm)
    {
        const size_t off = mm * d_sub;

        /// Initialize centroids from random sample sub-vectors (random init; k-means++ would help recall but adds cost).
        for (size_t c = 0; c < k; ++c)
        {
            const size_t r = randIndex(rng, n);
            std::memcpy(centroids.data() + c * d_sub, vectors + r * dimensions + off, d_sub * sizeof(float));
        }

        for (int iter = 0; iter < KMEANS_ITERATIONS; ++iter)
        {
            /// Assignment step in scalar double (off the hot path; keeps the codebook bit-faithful to the original).
            for (size_t i = 0; i < n; ++i)
                assign[i] = nearestCentroidScalar(centroids.data(), k, d_sub, vectors + i * dimensions + off);

            /// Update step: mean of assigned sub-vectors.
            std::fill(sums.begin(), sums.end(), 0.0);
            std::fill(counts.begin(), counts.end(), 0);
            for (size_t i = 0; i < n; ++i)
            {
                const size_t c = assign[i];
                const float * sub = vectors + i * dimensions + off;
                double * acc = sums.data() + c * d_sub;
                for (size_t j = 0; j < d_sub; ++j)
                    acc[j] += static_cast<double>(sub[j]);
                ++counts[c];
            }
            for (size_t c = 0; c < k; ++c)
            {
                if (counts[c] == 0)
                {
                    /// Empty cluster: re-seed from a random sample so it can pick up points next iteration.
                    const size_t r = randIndex(rng, n);
                    std::memcpy(centroids.data() + c * d_sub, vectors + r * dimensions + off, d_sub * sizeof(float));
                    continue;
                }
                const double inv = 1.0 / static_cast<double>(counts[c]);
                float * cen = centroids.data() + c * d_sub;
                const double * acc = sums.data() + c * d_sub;
                for (size_t j = 0; j < d_sub; ++j)
                    cen[j] = static_cast<float>(acc[j] * inv);
            }
        }

        std::memcpy(codebook.data() + mm * k * d_sub, centroids.data(), k * d_sub * sizeof(float));
    }

    return codebook;
}

struct ProductQuantizer::Encoder
{
    size_t m = 0;
    size_t d_sub = 0;
    size_t k = 0;
    bool two_bytes = false;
    std::vector<float> codebook_t; /// m*k*d_sub, per subspace column-major (centroid coordinate scanned across k)
    std::vector<float> cb_sqnorm;  /// m*k centroid squared norms for the reformulated argmin
    std::vector<float> acc;        /// k floats of per-vector kernel scratch (the encoder is used by one writer thread)
};

void ProductQuantizer::EncoderDeleter::operator()(Encoder * ptr) const noexcept { delete ptr; }

ProductQuantizer::EncoderPtr ProductQuantizer::createEncoder(const float * codebook, size_t dimensions, size_t m, size_t nbits)
{
    EncoderPtr encoder(new Encoder());
    encoder->m = m;
    encoder->d_sub = dimensions / m;
    encoder->k = numCentroids(nbits);
    encoder->two_bytes = nbits > 8;

    /// Per-codebook setup, reused across every row of a part: the column-major centroid layout and their squared norms.
    encoder->codebook_t.resize(encoder->m * encoder->k * encoder->d_sub);
    encoder->cb_sqnorm.resize(encoder->m * encoder->k);
    encoder->acc.resize(encoder->k);
    for (size_t mm = 0; mm < encoder->m; ++mm)
    {
        const float * centroids = codebook + mm * encoder->k * encoder->d_sub;
        transposeCentroids(centroids, encoder->k, encoder->d_sub, encoder->codebook_t.data() + mm * encoder->k * encoder->d_sub);
        centroidSquaredNorms(centroids, encoder->k, encoder->d_sub, encoder->cb_sqnorm.data() + mm * encoder->k);
    }
    return encoder;
}

void ProductQuantizer::encode(Encoder & encoder, const float * vec, char * dst)
{
    for (size_t mm = 0; mm < encoder.m; ++mm)
    {
        const float * centroids_t = encoder.codebook_t.data() + mm * encoder.k * encoder.d_sub;
        const float * sqn = encoder.cb_sqnorm.data() + mm * encoder.k;
        const size_t idx = nearestCentroid(centroids_t, sqn, encoder.k, encoder.d_sub, vec + mm * encoder.d_sub, encoder.acc.data());
        if (encoder.two_bytes)
        {
            const UInt16 v = static_cast<UInt16>(idx);
            std::memcpy(dst + mm * 2, &v, sizeof(UInt16));
        }
        else
            dst[mm] = static_cast<char>(static_cast<UInt8>(idx));
    }
}

void ProductQuantizer::encode(const float * codebook, size_t dimensions, size_t m, size_t nbits, const float * vec, char * dst)
{
    auto encoder = createEncoder(codebook, dimensions, m, nbits);
    encode(*encoder, vec, dst);
}

struct ProductQuantizer::Query
{
    size_t m = 0;
    size_t k = 0;
    bool two_bytes = false;
    bool is_l2 = false;
    std::vector<float> lut;       /// m*k: L2 -> partial squared distance; cosine -> partial inner product
    std::vector<float> cb_sqnorm; /// m*k centroid squared norms (cosine only)
    float q_norm = 0.0f;          /// ||query|| (cosine only)
};

/// Distance is asymmetric: the query is kept full-precision; a per-subspace lookup table of query-to-centroid
/// partial distances is precomputed once per (query, codebook), and each code is then `m` table lookups summed.
void ProductQuantizer::QueryDeleter::operator()(const Query * ptr) const noexcept { delete ptr; }

ProductQuantizer::QueryPtr
ProductQuantizer::prepareQuery(const float * codebook, size_t dimensions, size_t m, size_t nbits, const float * query, bool is_l2)
{
    const size_t d_sub = dimensions / m;
    const size_t k = numCentroids(nbits);

    auto q = std::make_unique<Query>();
    q->m = m;
    q->k = k;
    q->two_bytes = nbits > 8;
    q->is_l2 = is_l2;
    q->lut.assign(m * k, 0.0f);

    if (is_l2)
    {
        for (size_t mm = 0; mm < m; ++mm)
        {
            const float * qsub = query + mm * d_sub;
            const float * centroids = codebook + mm * k * d_sub;
            for (size_t c = 0; c < k; ++c)
                q->lut[mm * k + c] = static_cast<float>(sqDist(qsub, centroids + c * d_sub, d_sub));
        }
    }
    else
    {
        q->cb_sqnorm.assign(m * k, 0.0f);
        double qn2 = 0.0;
        for (size_t i = 0; i < dimensions; ++i)
            qn2 += static_cast<double>(query[i]) * static_cast<double>(query[i]);
        q->q_norm = static_cast<float>(std::sqrt(qn2));
        for (size_t mm = 0; mm < m; ++mm)
        {
            const float * qsub = query + mm * d_sub;
            const float * centroids = codebook + mm * k * d_sub;
            for (size_t c = 0; c < k; ++c)
            {
                const float * cen = centroids + c * d_sub;
                double dot = 0.0;
                double n2 = 0.0;
                for (size_t i = 0; i < d_sub; ++i)
                {
                    dot += static_cast<double>(qsub[i]) * static_cast<double>(cen[i]);
                    n2 += static_cast<double>(cen[i]) * static_cast<double>(cen[i]);
                }
                q->lut[mm * k + c] = static_cast<float>(dot);
                q->cb_sqnorm[mm * k + c] = static_cast<float>(n2);
            }
        }
    }
    return QueryPtr(q.release());
}

float ProductQuantizer::distance(const Query & query, const char * code)
{
    const size_t last = query.k - 1;
    auto codeAt = [&](size_t mm) -> size_t
    {
        if (query.two_bytes)
        {
            UInt16 idx = 0;
            std::memcpy(&idx, code + mm * 2, sizeof(UInt16));
            return std::min(static_cast<size_t>(idx), last);
        }
        return std::min(static_cast<size_t>(static_cast<UInt8>(code[mm])), last);
    };

    if (query.is_l2)
    {
        float d = 0.0f;
        for (size_t mm = 0; mm < query.m; ++mm)
            d += query.lut[mm * query.k + codeAt(mm)];
        return d; /// squared L2 of (query, reconstructed vector); monotone in true distance for ranking
    }

    float dot = 0.0f;
    float xn2 = 0.0f;
    for (size_t mm = 0; mm < query.m; ++mm)
    {
        const size_t c = codeAt(mm);
        dot += query.lut[mm * query.k + c];
        xn2 += query.cb_sqnorm[mm * query.k + c];
    }
    const float denom = query.q_norm * std::sqrt(xn2);
    const float cosine = (denom > 0.0f) ? std::clamp(dot / denom, -1.0f, 1.0f) : 0.0f;
    return 1.0f - cosine;
}

}
