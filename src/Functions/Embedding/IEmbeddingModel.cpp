#include <Functions/Embedding/IEmbeddingModel.h>
#include <Common/Exception.h>

#if USE_SIMSIMD
#    include <immintrin.h>
#    include <simsimd/simsimd.h>
/// simsimd_wsum_f16 is a SIMSIMD_DYNAMIC symbol defined in SimSIMD's lib.c via
/// SIMSIMD_DECLARATION_WSUM(wsum, f16). With SIMSIMD_DYNAMIC_DISPATCH=1 it is NOT
/// declared in the header (only the #else branch of simsimd.h has the declaration),
/// so we forward-declare it explicitly.
extern "C" SIMSIMD_DYNAMIC void simsimd_wsum_f16(
    simsimd_f16_t const * a, simsimd_f16_t const * b, simsimd_size_t n,
    simsimd_distance_t alpha, simsimd_distance_t beta, simsimd_f16_t * result);
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

std::vector<float> IEmbeddingModel::embed(const std::vector<int32_t> & /*token_ids*/, size_t /*dims*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "embed(token_ids) not supported by this model backend");
}

void IEmbeddingModel::embedInto(const int32_t * /*token_ids*/, size_t /*n_tokens*/, float * /*output*/, size_t /*dims*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "embedInto not supported by this model backend");
}

float IEmbeddingModel::cosineSimilarity(std::span<const float> a, std::span<const float> b)
{
    size_t n = std::min(a.size(), b.size());

    #if USE_SIMSIMD
        /// Embeddings are L2-normalized, so cosine similarity == dot product.
        /// SimSIMD provides dynamic dispatch (AVX-512 / AVX2 / NEON) with no manual branching.
        simsimd_distance_t result = 0;
        simsimd_dot_f32(a.data(), b.data(), n, &result);
        return static_cast<float>(result);
    #else
        float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;
        for (size_t i = 0; i < n; ++i)
        {
            dot += a[i] * b[i];
            norm_a += a[i] * a[i];
            norm_b += b[i] * b[i];
        }
        if (norm_a == 0.0f || norm_b == 0.0f)
            return 0.0f;
        return dot / (std::sqrt(norm_a) * std::sqrt(norm_b));
    #endif
}

}
