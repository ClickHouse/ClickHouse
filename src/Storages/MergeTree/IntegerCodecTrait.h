#pragma once
extern "C" {
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
    ALWAYS_INLINE static uint32_t bound(std::size_t n)
    {
        return streamvbyte_max_compressedbytes(n);
    }

    ALWAYS_INLINE static uint32_t encode(uint32_t * p, std::size_t n, unsigned char *out)
    {
        return streamvbyte_delta_encode(p, n, out, 0);
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char * p, std::size_t n, uint32_t *out)
    {
        return streamvbyte_delta_decode(p, out, n, 0);
    }
};

/// Specialization of CodecTraits for uint64_t.
template <>
struct CodecTraits<uint64_t>
{
    ALWAYS_INLINE static uint32_t bound(std::size_t)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CodecTraits<uint64_t>::bound");
    }

    ALWAYS_INLINE static uint64_t encode(uint64_t *, std::size_t, unsigned char *)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CodecTraits<uint64_t>::encode");
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char *, std::size_t, uint64_t *)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CodecTraits<uint64_t>::decode");
    }
};
}
