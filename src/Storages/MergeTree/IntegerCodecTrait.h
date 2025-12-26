#pragma once
#if defined(__x86_64__) || defined(_M_X64)
#if defined(__SSE4_1__)
    #define USE_SIMDCOMP 1
#endif
#endif

#if !defined(USE_SIMDCOMP)
    #define USE_STREAMVBYTE 1
#endif

extern "C"
{
#if defined(USE_SIMDCOMP)
#include <simdcomp.h>
#else
#include <streamvbyte.h>
#include <streamvbytedelta.h>
#endif
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Generic codec traits.
/// Specializations provide a uniform encode/decode interface for different integer types.
template <typename T>
struct CodecTraits;

/// Specialization of CodecTraits for uint32_t.
/// If the current platform is Intel/AMD x86 and the compiler has enabled
/// AVX512F / AVX2 / SSE4.1 (or later) SIMD instruction sets, use simdcomp
/// for integer compression encoding.
/// On all other platforms (e.g. ARM, Apple Silicon, or x86 without these
/// instruction sets), fall back to streamvbyte for integer compression
/// to ensure portability and compatibility.
template <>
struct CodecTraits<uint32_t>
{
    static constexpr size_t kBlockSize =  128;

    ALWAYS_INLINE static std::pair<size_t, size_t> evaluateSizeAndMaxBits([[maybe_unused]] const uint32_t * data, size_t size)
    {
        /// When using streamvbyte for compression, we don’t need to know how many bits
        /// are required to store the maximum value in the array. Therefore, the second
        /// return value is simply set to 0 here.
#if defined(USE_SIMDCOMP)
        {
            auto bits = maxbits_length(data, size);
            auto bytes = simdpack_compressedbytes(size, bits);
            return { bytes, bits };
        }
#else
        return { streamvbyte_max_compressedbytes(size), 0 };
#endif
    }

    ALWAYS_INLINE static uint32_t encode(const uint32_t * p, std::size_t n, [[maybe_unused]] uint32_t bits, unsigned char *out)
    {
#if defined(USE_SIMDCOMP)
        __m128i * m128_out = reinterpret_cast<__m128i*>(out);
        __m128i * m128_out_end = simdpack_length(p, n, m128_out, bits);

        return static_cast<size_t>(m128_out_end - m128_out) * sizeof(__m128i);
#else
        return streamvbyte_delta_encode(p, n, out, 0);
#endif
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char * p, std::size_t n, [[maybe_unused]] uint32_t bits, uint32_t *out)
    {
#if defined(USE_SIMDCOMP)
        auto * m128i_p = reinterpret_cast<__m128i*>(p);
        const auto * m128i_p_end = simdunpack_length(m128i_p, n, out, bits);
        return static_cast<size_t>(m128i_p_end - m128i_p) * sizeof(__m128*);
#else
        return streamvbyte_delta_decode(p, out, n, 0);
#endif
    }
};
}
