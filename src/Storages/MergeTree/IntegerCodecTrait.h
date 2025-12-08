#pragma once

extern "C"
{
#if defined(__x86_64__) || defined(_M_X64)
#include <simdcomp.h>
#else
#include <streamvbyte.h>
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
///
/// This implementation uses StreamVByte delta coding
/// (streamvbyte_delta_encode / streamvbyte_delta_decode)
/// to compress and decompress arrays of 32-bit unsigned integers.
template <>
struct CodecTraits<uint32_t>
{
    ALWAYS_INLINE static std::pair<size_t, size_t> evaluateSizeAndMaxBits(const std::vector<uint32_t> & data)
    {
#if defined(__x86_64__) || defined(_M_X64)
        auto bits = maxbits_length(data.data(), data.size());
        return { bits * data.size(), bits };
#else
        return { streamvbyte_max_compressedbytes(data.size()), 0 };
#endif
    }

    ALWAYS_INLINE static uint32_t encode(uint32_t * p, std::size_t n, [[maybe_unused]] uint32_t bits, unsigned char *out)
    {
#if defined(__x86_64__) || defined(_M_X64)
#if defined(__AVX512F__)
        const auto * end = avx512pack(p, static_cast<__m512i*>(out), bits);
        return static_cast<uint32_t>(end - out);
#endif
#if defined(__AVX2__)
        const auto * end = avxpack(p, static_cast<__m256i*>(out), bits);
        return static_cast<uint32_t>(end - out);
#endif
#if defined(__SSE4_1__)
        return simdpack(p, static_cast<__m128i*>(out), bits);
        return static_cast<uint32_t>(end - out);
#endif
#endif
        return streamvbyte_delta_encode(p, n, out, 0);
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char * p, std::size_t n, [[maybe_unused]] uint32_t bits, uint32_t *out)
    {
#if defined(__x86_64__) || defined(_M_X64)
#if defined(__AVX512F__)
        avx512unpack(p, static_cast<__m512i*>(out), bits);
        return n * bits;
#endif
#if defined(__AVX2__)
        avxunpack(p, static_cast<__m256i*>(out), bits);
        return n * bits;
#endif
#if defined(__SSE4_1__)
        simdunpack(p, static_cast<__m128i*>(out), bits);
        return n * bits;
#endif
#endif
        return streamvbyte_delta_decode(p, out, n, 0);
    }
};

/// Specialization of CodecTraits for uint64_t.
template <>
struct CodecTraits<uint64_t>
{
    ALWAYS_INLINE static std::pair<size_t, size_t> evaluateSizeAndMaxBits(const std::vector<uint64_t> &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CodecTraits<uint64_t>::bound");
    }

    ALWAYS_INLINE static uint64_t encode(uint64_t *, std::size_t, size_t, unsigned char *)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CodecTraits<uint64_t>::encode");
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char *, std::size_t, size_t, uint64_t *)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CodecTraits<uint64_t>::decode");
    }
};
}
