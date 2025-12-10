#pragma once
#if defined(__x86_64__) || defined(_M_X64)
#if defined(__AVX512F__)
    #define USE_SIMDCOMP_AVX512 1
#elif defined(__AVX2__)
    #define USE_SIMDCOMP_AVX2 1
#elif defined(__SSE4_1__)
    #define USE_SIMDCOMP_SSE41 1
#else
    #define USE_STREAMVBYTE 1
#endif
#endif

#if defined(USE_SIMDCOMP_AVX512) || defined(USE_SIMDCOMP_AVX2) || defined(USE_SIMDCOMP_SSE41)
    #define USE_SIMDCOMP 1
#endif

#if defined(__x86_64__) || defined(_M_X64)
    #if defined(__AVX512F__)
        #define USE_SIMDCOMP_AVX512 1
    #elif defined(__AVX2__)
        #define USE_SIMDCOMP_AVX2 1
    #elif defined(__SSE4_1__)
        #define USE_SIMDCOMP_SSE41 1
    #else
        #define USE_STREAMVBYTE 1
    #endif
#else
    #define USE_STREAMVBYTE 1
#endif

#if defined(USE_SIMDCOMP_AVX512) || defined(USE_SIMDCOMP_AVX2) || defined(USE_SIMDCOMP_SSE41)
    #define USE_SIMDCOMP 1
#endif

extern "C"
{
#if defined(USE_SIMDCOMP)
#include <simdcomp.h>
#endif
#include <streamvbyte.h>
#include <streamvbytedelta.h>
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
    ALWAYS_INLINE static std::pair<size_t, size_t> evaluateSizeAndMaxBits([[maybe_unused]] const uint32_t * data, size_t size)
    {
#if defined(USE_SIMDCOMP)
        /// When using streamvbyte for compression, we don’t need to know how many bits
        /// are required to store the maximum value in the array. Therefore, the second
        /// return value is simply set to 0 here.
        {
            auto bits = maxbits_length(data, size);
            auto bytes = simdpack_compressedbytes(size, bits);
            return { bytes, bits };
        }
#endif
        return { streamvbyte_max_compressedbytes(size), 0 };
    }

    ALWAYS_INLINE static uint32_t encode(const uint32_t * p, std::size_t n, [[maybe_unused]] uint32_t bits, unsigned char *out)
    {
#if defined(USE_SIMDCOMP_AVX512)
        auot * m512i_out = reinterpret_cast<__m512i*>(out);
        avx512pack(p, m512i_out, bits);
        return static_cast<uint32_t>(m512i_out - reinterpret_cast<__m512i*>(out));
#endif

#if defined(USE_SIMDCOMP_AVX2)
        auto * m256i_out = reinterpret_cast<__m256i*>(out);
        avxpack(p, m256i_out, bits);
        return static_cast<uint32_t>(m256i_out - reinterpret_cast<__m256i*>(out));
#endif

#if defined(USE_SIMDCOMP_SSE41)
        auto * m128_out = reinterpret_cast<__m128i*>(out);
        auto end = simdpack_length(p, n, m128_out, bits);
        return static_cast<uint32_t>(end - m128_out);
#endif
        return streamvbyte_delta_encode(p, n, out, 0);
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char * p, std::size_t n, [[maybe_unused]] uint32_t bits, uint32_t *out)
    {
#if defined(USE_SIMDCOMP_AVX512)
        auot * m512i_out = reinterpret_cast<__m512i*>(out);
        avx512unpack(p, m512i_out, bits);
        return static_cast<uint32_t>(m512i_out - reinterpret_cast<__m512i*>(out));
#endif

#if defined(USE_SIMDCOMP_AVX2)
        auto * m256i_out = reinterpret_cast<__m256i*>(out);
        avxunpack(p, m256i_out, bits);
        return static_cast<uint32_t>(m256i_out - reinterpret_cast<__m256i*>(out));
#endif

#if defined(USE_SIMDCOMP_SSE41)
        auto * m128i_p = reinterpret_cast<__m128i*>(p);
        const auto * end = simdunpack_length(m128i_p, n, out, bits);
        return static_cast<size_t>(end - m128i_p);
#endif
        return streamvbyte_delta_decode(p, out, n, 0);
    }
};
}
