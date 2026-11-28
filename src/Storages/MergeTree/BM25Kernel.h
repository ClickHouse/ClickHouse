#pragma once

#include <Storages/MergeTree/SmallFloat.h>
#include <base/defines.h>
#include <base/types.h>

#include <algorithm>
#include <cmath>
#include <array>

namespace DB
{

struct BM25Params
{
    Float64 k1 = 1.2;
    Float64 b = 0.75;
};

/// Okapi BM25 smoothed IDF: idf(t) = ln((N − df + 0.5) / (df + 0.5) + 1).
inline Float64 calculateIDF(UInt64 num_docs, UInt64 doc_freq)
{
    const Float64 n = static_cast<Float64>(num_docs);
    const Float64 df = static_cast<Float64>(doc_freq);
    return std::max(0.0, std::log((n - df + 0.5) / (df + 0.5) + 1.0));
}

/// Length-normalization cache keyed by the SmallFloat doc-length byte.
struct BM25LengthNormCache
{
    std::array<Float32, 256> norm{};

    BM25LengthNormCache(Float64 avgdl, const BM25Params & params)
    {
        const Float64 inv_avgdl = avgdl > 0.0 ? 1.0 / avgdl : 0.0;

        for (size_t i = 0; i < 256; ++i)
        {
            const Float64 dl = static_cast<Float64>(SmallFloat::fromInt4Byte(static_cast<UInt8>(i)));
            norm[i] = static_cast<Float32>(params.k1 * (1.0 - params.b + params.b * dl * inv_avgdl));
        }
    }
};

struct BM25Weight
{
    /// idf · (k1 + 1) — per-term UB
    Float32 weight = 0;
    /// Shared, built once from avgdl.
    const BM25LengthNormCache * length_norm_cache = nullptr;

    BM25Weight(Float64 idf, const BM25Params & params, const BM25LengthNormCache * length_norm_cache_)
        : weight(static_cast<Float32>(idf * (params.k1 + 1.0)))
        , length_norm_cache(length_norm_cache_)
    {
    }

    /// BM25 contribution of one (term, doc) occurrence.
    ALWAYS_INLINE Float32 contribution(UInt32 tf, UInt8 dl_byte) const
    {
        return weight * static_cast<Float32>(tf) / (static_cast<Float32>(tf) + length_norm_cache->norm[dl_byte]);
    }
};

struct BM25ScoringToken
{
    String token;
    BM25Weight weight;
};

}
