#pragma once

extern "C"
{
#if defined(__x86_64__) || defined(_M_X64)
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
        auto bytes = simdpack_compressedbytes(data.size(), bits);
        return { bytes, bits };
#else
        return { streamvbyte_max_compressedbytes(data.size()), 0 };
#endif
    }

    ALWAYS_INLINE static uint32_t encode(uint32_t * p, std::size_t n, [[maybe_unused]] uint32_t bits, unsigned char *out)
    {
#if defined(__x86_64__) || defined(_M_X64)
#if defined(__AVX512F__)
        auot * m512i_out = reinterpret_cast<__m512i*>(out);
        avx512pack(p, m512i_out, bits);
        return static_cast<uint32_t>(m512i_out - reinterpret_cast<__m512i*>(out));
#endif
#if defined(__AVX2__)
        auto * m256i_out = reinterpret_cast<__m256i*>(out);
        avxpack(p, m256i_out, bits);
        return static_cast<uint32_t>(m256i_out - reinterpret_cast<__m256i*>(out));
#endif
#if defined(__SSE4_1__)
        auto * m128_out = reinterpret_cast<__m128i*>(out);
        auto end = simdpack_length(p, n, m128_out, bits);
        return static_cast<uint32_t>(end - m128_out);
#endif
#endif
        return streamvbyte_delta_encode(p, n, out, 0);
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char * p, std::size_t n, [[maybe_unused]] uint32_t bits, uint32_t *out)
    {
#if defined(__x86_64__) || defined(_M_X64)
#if defined(__AVX512F__)
        auot * m512i_out = reinterpret_cast<__m512i*>(out);
        avx512unpack(p, m512i_out, bits);
        return static_cast<uint32_t>(m512i_out - reinterpret_cast<__m512i*>(out));
#endif
#if defined(__AVX2__)
        auto * m256i_out = reinterpret_cast<__m256i*>(out);
        avxunpack(p, m256i_out, bits);
        return static_cast<uint32_t>(m256i_out - reinterpret_cast<__m256i*>(out));
#endif
#if defined(__SSE4_1__)
        auto * m128i_p = reinterpret_cast<__m128i*>(p);
        const auto * end = simdunpack_length(m128i_p, n, out, bits);
        return static_cast<size_t>(end - m128i_p);
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
