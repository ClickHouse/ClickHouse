#include <Common/ProductQuantization.h>

#include <Common/Exception.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <string>

#include <fmt/format.h>

namespace DB
{

namespace ProductQuantization
{

namespace
{

/// Deterministic splitmix64 -> uniform size_t in [0, bound).
inline UInt64 splitmix64(UInt64 & state)
{
    UInt64 z = (state += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

inline size_t randIndex(UInt64 & state, size_t bound)
{
    return bound ? static_cast<size_t>(splitmix64(state) % bound) : 0;
}

/// Squared L2 distance between two d-dim float vectors.
inline double sqDist(const float * a, const float * b, size_t d)
{
    double s = 0.0;
    for (size_t i = 0; i < d; ++i)
    {
        const double e = static_cast<double>(a[i]) - static_cast<double>(b[i]);
        s += e * e;
    }
    return s;
}

/// Index of the nearest centroid (of `k` centroids, each `d_sub` floats, contiguous in `centroids`) to `sub`.
inline size_t nearestCentroid(const float * centroids, size_t k, size_t d_sub, const float * sub)
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

constexpr int KMEANS_ITERATIONS = 20;

}

size_t numCentroids(size_t nbits)
{
    return static_cast<size_t>(1) << nbits;
}

size_t codebookFloats(size_t dimensions, size_t /*m*/, size_t nbits)
{
    /// m * k * d_sub = k * dimensions (m cancels).
    return numCentroids(nbits) * dimensions;
}

size_t bytesPerVector(size_t /*dimensions*/, size_t m, size_t nbits)
{
    return m * (nbits <= 8 ? 1 : 2);
}

std::string validateParams(size_t dimensions, size_t m, size_t nbits)
{
    if (dimensions == 0)
        return "the number of dimensions must be greater than zero";
    /// Bound the dimensions before any size arithmetic: `codebookFloats = 2^nbits * dimensions` and the per-vector code
    /// size are derived from it, so an absurd value (e.g. a fuzzer passing 2^63) would overflow size_t and then trip a
    /// std::length_error/bad_alloc deep inside a container. Cap well above any real embedding so the derived sizes stay
    /// in range and the error is a clean exception here instead.
    static constexpr size_t MAX_DIMENSIONS = 1u << 20; /// 1,048,576
    if (dimensions > MAX_DIMENSIONS)
        return fmt::format("the number of dimensions ({}) exceeds the maximum {}", dimensions, MAX_DIMENSIONS);
    if (m == 0)
        return "the number of subspaces (m) must be greater than zero";
    if (dimensions % m != 0)
        return fmt::format("the number of dimensions ({}) must be a multiple of the number of subspaces m ({})", dimensions, m);
    if (nbits < 1 || nbits > 16)
        return fmt::format("nbits must be in the range [1, 16], got {}", nbits);
    return {};
}

std::vector<float> trainCodebook(const float * vectors, size_t n, size_t dimensions, size_t m, size_t nbits, UInt64 seed)
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
            /// Assignment step.
            for (size_t i = 0; i < n; ++i)
                assign[i] = nearestCentroid(centroids.data(), k, d_sub, vectors + i * dimensions + off);

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

void encode(const float * codebook, size_t dimensions, size_t m, size_t nbits, const float * vec, char * dst)
{
    const size_t d_sub = dimensions / m;
    const size_t k = numCentroids(nbits);
    const bool two_bytes = nbits > 8;
    for (size_t mm = 0; mm < m; ++mm)
    {
        const float * centroids = codebook + mm * k * d_sub;
        const size_t idx = nearestCentroid(centroids, k, d_sub, vec + mm * d_sub);
        if (two_bytes)
        {
            const UInt16 v = static_cast<UInt16>(idx);
            std::memcpy(dst + mm * 2, &v, sizeof(UInt16));
        }
        else
            dst[mm] = static_cast<char>(static_cast<UInt8>(idx));
    }
}

struct Query
{
    size_t m = 0;
    size_t k = 0;
    bool two_bytes = false;
    bool is_l2 = false;
    std::vector<float> lut;       /// m*k: L2 -> partial squared distance; cosine -> partial inner product
    std::vector<float> cb_sqnorm; /// m*k centroid squared norms (cosine only)
    float q_norm = 0.0f;          /// ||query|| (cosine only)
};

std::shared_ptr<const Query>
prepareQuery(const float * codebook, size_t dimensions, size_t m, size_t nbits, const float * query, bool is_l2)
{
    const size_t d_sub = dimensions / m;
    const size_t k = numCentroids(nbits);

    auto q = std::make_shared<Query>();
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
    return q;
}

float distance(const Query & q, const char * code)
{
    const size_t last = q.k - 1;
    auto codeAt = [&](size_t mm) -> size_t
    {
        if (q.two_bytes)
        {
            UInt16 idx = 0;
            std::memcpy(&idx, code + mm * 2, sizeof(UInt16));
            return std::min(static_cast<size_t>(idx), last);
        }
        return std::min(static_cast<size_t>(static_cast<UInt8>(code[mm])), last);
    };

    if (q.is_l2)
    {
        float d = 0.0f;
        for (size_t mm = 0; mm < q.m; ++mm)
            d += q.lut[mm * q.k + codeAt(mm)];
        return d; /// squared L2 of (query, reconstructed vector); monotone in true distance for ranking
    }

    float dot = 0.0f;
    float xn2 = 0.0f;
    for (size_t mm = 0; mm < q.m; ++mm)
    {
        const size_t c = codeAt(mm);
        dot += q.lut[mm * q.k + c];
        xn2 += q.cb_sqnorm[mm * q.k + c];
    }
    const float denom = q.q_norm * std::sqrt(xn2);
    const float cosine = (denom > 0.0f) ? std::clamp(dot / denom, -1.0f, 1.0f) : 0.0f;
    return 1.0f - cosine;
}

}

}
