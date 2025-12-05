#pragma once

extern "C" {
#include <ic.h>
}

extern "C" {
#include <streamvbyte.h>
#include <streamvbytedelta.h>
}
#pragma clang optimize off
namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

/// A compact, block-based container for storing monotonically increasing
/// postings (document IDs or row offsets) with TurboPFor compression.
///
/// This templated class supports both 32-bit and 64-bit integer types.
/// Postings are accumulated in memory and compressed in fixed-size blocks
/// (`kBlockSize = 128`) using TurboPFor’s PForDelta codec
/// (`p4nd1enc32` / `p4nd1enc64`). Each compressed block is prefixed by a
/// small variable-length header that stores the number of elements and the
/// compressed byte length, both encoded as VarUInt values.
///
/// Data Layout
///
/// The serialized byte stream layout is as follows:
///
///   +--------------------------------------------------------------------+
///   | VarUInt(block_count) | VarUInt(total_elems) | VarUInt(bytes_total) |
///   | [BlockHeader][CompressedBlock] ... [BlockHeader][CompressedBlock]  |
///   +--------------------------------------------------------------------+
///
/// Each `[BlockHeader]` is encoded using VarUInt values:
///   - VarUInt(n)      : number of elements in the block
///   - VarUInt(bytes)  : number of compressed bytes that follow
///
/// The `[CompressedBlock]` contains TurboPFor-encoded integer deltas.
///
/// Compression
///
/// When `kBlockSize` values have been collected, the block is delta-encoded
/// (using the “delta − 1” scheme for strictly increasing sequences) and
/// compressed with the TurboPFor codec. Remaining values in `current` are
/// automatically flushed during serialization.
template <typename T>
struct CodecTraits;
#if 0
template <>
struct CodecTraits<uint32_t>
{
    ALWAYS_INLINE static uint32_t bound(std::size_t n)
    {
        return p4nbound256v32(static_cast<uint32_t>(n));
    }

    ALWAYS_INLINE static uint32_t encode(uint32_t * p, std::size_t n, unsigned char *out)
    {
        return p4nd1enc32(p, n, out);
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char * p, std::size_t n, uint32_t *out)
    {
        return p4nd1dec32(p, n, out);
    }
};

template <>
struct CodecTraits<uint64_t>
{
    ALWAYS_INLINE static uint32_t bound(std::size_t n)
    {
        return p4nbound256v32(static_cast<uint32_t>(n));
    }

    ALWAYS_INLINE static uint64_t encode(uint64_t * p, std::size_t n, unsigned char *out)
    {
        return p4nd1enc64(p, n, out);
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char * p, std::size_t n, uint64_t *out)
    {
        return p4nd1dec64(p, n, out);
    }
};
#endif
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
