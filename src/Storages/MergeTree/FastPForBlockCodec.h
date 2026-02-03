#pragma once

#include <config.h>
#include <cstring>
#include <cstdint>
#include <span>
#include <Common/Exception.h>

#if USE_FASTPFOR
#include <fastpfor.h>
#include <codecfactory.h>
#include <compositecodec.h>
#include <variablebyte.h>

// Forward declarations for SIMD functions from FastPFor library
// These are needed because of header include ordering issues
// NOLINTBEGIN(readability-avoid-const-params-in-decls)
namespace FastPForLib
{
extern void SIMD_fastpack_32(const uint32_t *__restrict__ in, __m128i *__restrict__ out, const uint32_t bit);
extern void SIMD_fastunpack_32(const __m128i *__restrict__ in, uint32_t *__restrict__ out, const uint32_t bit);
extern void SIMD_fastpackwithoutmask_32(const uint32_t *__restrict__ in, __m128i *__restrict__ out, const uint32_t bit);
}
// NOLINTEND(readability-avoid-const-params-in-decls)

#include <simdbitpacking.h>
#include <simdfastpfor.h>
#include <simdoptpfor.h>
#include <simdbinarypacking.h>
#include <streamvariablebyte.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
}

/// FastPFor block codec wrappers for posting list compression.
///
/// This file provides static interface wrappers around FastPFor library codecs,
/// enabling them to be used as BlockCodec template parameters in PostingListCodecImpl.
///
/// All codecs here are SIMD-accelerated (where available) and designed for
/// compressing sorted integer sequences (posting lists).
///
/// Interface semantics match BitpackingBlockCodec:
///   - encode()/decode() take span references and advance them past consumed/written data
///   - calculateNeededBytes() returns estimated bytes needed for compression


namespace impl
{

/// Static interface wrapper for FastPFor codecs.
///
/// Adapts FastPFor library codecs to the static BlockCodec interface required
/// by PostingListCodecBitpackingImpl. Uses thread-local instances internally because
/// FastPFor codecs maintain internal state.
///
/// Interface matches BitpackingBlockCodec:
///   - encode(span<uint32_t>&, span<char>&) -> bytes written, advances both spans
///   - decode(span<const std::byte>&, count, span<uint32_t>&) -> bytes consumed, advances both spans
///   - calculateNeededBytes(span<uint32_t>&) -> estimated bytes needed
///
/// Note: For FastPFor codecs, max_bits parameter is ignored in encode/decode since
/// FastPFor internally determines and stores bit widths. We keep the parameter for
/// interface compatibility.
///
/// Template parameters:
///   - CodecType: The FastPFor codec type (e.g., SIMDFastPFor, SIMDBinaryPacking)
///   - Derived: CRTP derived class that provides static name() method
template <typename CodecType, typename Derived>
struct FastPForCodecBase
{
    /// Block size for compression (128 elements).
    /// This matches the BlockSize of SIMDFastPFor<4>, SIMDBinaryPacking, and SIMDOPTPFor<4>.
    /// StreamVByte has no block size requirement but uses 128 for consistency.
    static constexpr size_t BLOCK_SIZE = 128;

    /// Returns the codec name for identification.
    static constexpr const char * name() noexcept { return Derived::NAME; }

    /// Calculates needed bytes for encoding.
    /// For FastPFor, we don't need max_bits since the codec handles its own metadata.
    /// @param data  Input integers (span reference, not modified)
    /// @return      estimated_bytes needed for compression
    static size_t calculateNeededBytes(const std::span<uint32_t> & data) noexcept
    {
        if (data.empty())
            return 0;

        /// 4 bytes length prefix + worst-case uncompressed size + overhead for codec metadata.
        /// FastPFor may expand slightly in worst case, but typically compresses well.
        return sizeof(uint32_t) + data.size() * sizeof(uint32_t) + 256;
    }

    /// Encodes integers from `in` to compressed bytes in `out`.
    /// Advances both `in` and `out` spans past the consumed/written data.
    ///
    /// Format: [4-byte length prefix][compressed data]
    /// The length prefix stores the size of compressed data in bytes, allowing
    /// correct decoding when multiple blocks are stored sequentially.
    ///
    /// @param in       Input integers (span reference, advanced to end after encoding)
    /// @param out      Output buffer (span reference, advanced past written bytes)
    /// @return         Number of bytes written to `out`
    static size_t encode(std::span<uint32_t> & in, std::span<char> & out)
    {
        if (in.empty())
            return 0;

        thread_local CodecType codec;

        /// Reserve space for length prefix
        constexpr size_t prefix_size = sizeof(uint32_t);
        if (out.size() < prefix_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "{} encode: output buffer too small for length prefix", Derived::NAME);

        /// Encode data after the length prefix
        char * data_start = out.data() + prefix_size;
        size_t out_capacity = (out.size() - prefix_size) / sizeof(uint32_t);
        size_t out_count = out_capacity;

        codec.encodeArray(
            in.data(), in.size(),
            reinterpret_cast<uint32_t*>(data_start), out_count);

        size_t compressed_bytes = out_count * sizeof(uint32_t);

        /// Write length prefix (little-endian)
        uint32_t length_prefix = static_cast<uint32_t>(compressed_bytes);
        std::memcpy(out.data(), &length_prefix, sizeof(length_prefix));

        size_t total_written = prefix_size + compressed_bytes;

        /// Advance spans
        in = in.subspan(in.size());
        out = out.subspan(total_written);

        return total_written;
    }

    /// Decodes compressed bytes from `in` to integers in `out`.
    /// Advances both `in` and `out` spans past the consumed/written data.
    ///
    /// Format: [4-byte length prefix][compressed data]
    /// The length prefix tells us exactly how many bytes belong to this block.
    ///
    /// @param in       Compressed input (span reference, advanced past consumed bytes)
    /// @param count    Number of integers to decode
    /// @param out      Output buffer (span reference, advanced past decoded integers)
    /// @return         Number of bytes consumed from `in`
    static size_t decode(std::span<const std::byte> & in, size_t count, std::span<uint32_t> & out)
    {
        if (count == 0)
            return 0;

        if (in.empty())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "{} decode: input buffer is empty but need to decode {} integers", Derived::NAME, count);

        if (out.size() < count)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "{} decode: output buffer too small, need {} but got {}", Derived::NAME, count, out.size());

        /// Read length prefix
        constexpr size_t prefix_size = sizeof(uint32_t);
        if (in.size() < prefix_size)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "{} decode: input buffer too small for length prefix", Derived::NAME);

        uint32_t compressed_bytes;
        std::memcpy(&compressed_bytes, in.data(), sizeof(compressed_bytes));

        size_t total_block_size = prefix_size + compressed_bytes;
        if (in.size() < total_block_size)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "{} decode: input buffer too small, need {} but got {}",
                Derived::NAME, total_block_size, in.size());

        thread_local CodecType codec;

        /// Only pass the exact compressed data size to the codec
        const std::byte * data_start = in.data() + prefix_size;
        size_t in_count = compressed_bytes / sizeof(uint32_t);
        size_t out_count = count;

        try
        {
            codec.decodeArray(
                reinterpret_cast<const uint32_t*>(data_start), in_count,
                out.data(), out_count);

            /// Advance spans
            in = in.subspan(total_block_size);
            out = out.subspan(out_count);

            return total_block_size;
        }
        catch (...)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "{} decode failed: compressed_bytes={}, in_count={}, count={}",
                Derived::NAME, compressed_bytes, in_count, count);
        }
    }
};

}

/// SIMD-FastPFor: Patched Frame-of-Reference with SIMD acceleration.
/// High compression ratio, very fast decode. Best for large posting lists with outliers.
/// Uses CompositeCodec with VariableByte for remainder handling.
struct SIMDFastPForBlockCodec : impl::FastPForCodecBase<
    FastPForLib::CompositeCodec<FastPForLib::SIMDFastPFor<4>, FastPForLib::VariableByte>,
    SIMDFastPForBlockCodec>
{
    static constexpr const char * NAME = "fastpfor";
};

/// SIMD-BinaryPacking: Fixed-width bit packing with SIMD.
/// Medium compression, fastest decode speed. Ideal for speed-critical query scenarios.
/// Uses CompositeCodec with VariableByte for remainder handling.
struct SIMDBinaryPackingBlockCodec : impl::FastPForCodecBase<
    FastPForLib::CompositeCodec<FastPForLib::SIMDBinaryPacking, FastPForLib::VariableByte>,
    SIMDBinaryPackingBlockCodec>
{
    static constexpr const char * NAME = "binarypacking";
};

/// SIMD-OptPFor: Optimized Patched Frame-of-Reference with SIMD.
/// Highest compression ratio among all variants. Slightly slower than FastPFor.
/// Uses CompositeCodec with VariableByte for remainder handling.
struct SIMDOptPForBlockCodec : impl::FastPForCodecBase<
    FastPForLib::CompositeCodec<FastPForLib::SIMDOPTPFor<4>, FastPForLib::VariableByte>,
    SIMDOptPForBlockCodec>
{
    static constexpr const char * NAME = "optpfor";
};

#endif // USE_FASTPFOR

}
