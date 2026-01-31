#pragma once

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
///   - calculateNeededBytesAndMaxBits() returns {bytes, max_bits} pair
///   - bitpackingCompressedBytes() calculates compressed size for given count and bits

#include <config.h>
#include <span>
#include <cstdint>
#include <Common/Exception.h>

#if USE_FASTPFOR

#include <codecfactory.h>
#include <compositecodec.h>
#include <variablebyte.h>
#include <simdfastpfor.h>
#include <simdoptpfor.h>
#include <simdbinarypacking.h>
#include <streamvariablebyte.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace impl
{

/// Static interface wrapper for FastPFor codecs.
///
/// Adapts FastPFor library codecs to the static BlockCodec interface required
/// by PostingListCodecBitpackingImpl. Uses thread-local instances internally because
/// FastPFor codecs maintain internal state.
///
/// Interface matches BitpackingBlockCodec:
///   - encode(span<uint32_t>&, max_bits, span<char>&) -> bytes written, advances both spans
///   - decode(span<const std::byte>&, count, max_bits, span<uint32_t>&) -> bytes consumed, advances both spans
///   - calculateNeededBytesAndMaxBits(span<uint32_t>&) -> {bytes, max_bits}
///   - bitpackingCompressedBytes(count, bits) -> compressed size
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
    /// Returns the codec name for identification.
    static constexpr const char * name() noexcept { return Derived::NAME; }

    /// Calculates compressed byte size for `count` integers with `bits` bit-width.
    /// For FastPFor, this returns a worst-case estimate since actual compression
    /// depends on data distribution.
    static size_t bitpackingCompressedBytes(size_t count, uint32_t /*bits*/) noexcept
    {
        // FastPFor worst case: slightly larger than input + overhead for metadata
        return count * sizeof(uint32_t) + 1024;
    }

    /// Calculates needed bytes and max bits for encoding.
    /// For FastPFor, max_bits is computed but actual compression may vary.
    /// @param data  Input integers (span reference, not modified)
    /// @return      {estimated_bytes, max_bits} pair
    static std::pair<size_t, uint32_t> calculateNeededBytesAndMaxBits(std::span<uint32_t> & data) noexcept
    {
        if (data.empty())
            return {0, 0};

        // Calculate max bits by OR-reducing all values
        uint32_t or_result = 0;
        for (uint32_t v : data)
            or_result |= v;

        uint32_t max_bits = (or_result == 0) ? 0 : (32 - static_cast<uint32_t>(__builtin_clz(or_result)));

        // Worst-case estimate for FastPFor
        size_t needed_bytes = data.size() * sizeof(uint32_t) + 1024;

        return {needed_bytes, max_bits};
    }

    /// Encodes integers from `in` to compressed bytes in `out`.
    /// Advances both `in` and `out` spans past the consumed/written data.
    ///
    /// @param in       Input integers (span reference, advanced to end after encoding)
    /// @param out      Output buffer (span reference, advanced past written bytes)
    /// @return         Number of bytes written to `out`
    /// @note           The second parameter (max_bits) is ignored for FastPFor but kept for interface compatibility.
    static size_t encode(std::span<uint32_t> & in, int32_t /*max_bits*/, std::span<char> & out)
    {
        if (in.empty())
            return 0;

        thread_local CodecType codec;

        size_t out_capacity = out.size() / sizeof(uint32_t);
        size_t out_count = out_capacity;

        codec.encodeArray(
            in.data(), in.size(),
            reinterpret_cast<uint32_t*>(out.data()), out_count);

        size_t written_bytes = out_count * sizeof(uint32_t);

        // Advance spans
        in = in.subspan(in.size());  // All input consumed
        out = out.subspan(written_bytes);

        return written_bytes;
    }

    /// Decodes compressed bytes from `in` to integers in `out`.
    /// Advances both `in` and `out` spans past the consumed/written data.
    ///
    /// @param in       Compressed input (span reference, advanced past consumed bytes)
    /// @param count    Number of integers to decode
    /// @param out      Output buffer (span reference, advanced past decoded integers)
    /// @return         Number of bytes consumed from `in`
    /// @note           The third parameter (max_bits) is ignored for FastPFor but kept for interface compatibility.
    static size_t decode(std::span<const std::byte> & in, size_t count, uint32_t /*max_bits*/, std::span<uint32_t> & out)
    {
        if (count == 0 || in.empty())
            return 0;

        if (out.size() < count)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "FastPFor decode: output buffer too small, need {} but got {}", count, out.size());

        thread_local CodecType codec;

        size_t in_count = in.size() / sizeof(uint32_t);
        size_t out_count = count;

        const uint32_t * consumed_end = codec.decodeArray(
            reinterpret_cast<const uint32_t*>(in.data()), in_count,
            out.data(), out_count);

        size_t consumed_bytes = reinterpret_cast<const char*>(consumed_end)
                              - reinterpret_cast<const char*>(in.data());

        // Advance spans
        in = in.subspan(consumed_bytes);
        out = out.subspan(count);

        return consumed_bytes;
    }
};

} // namespace impl

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

/// StreamVByte: Byte-aligned variable-byte encoding with SIMD.
/// Lower compression but very fast streaming decode with good random access.
struct StreamVByteBlockCodec : impl::FastPForCodecBase<
    FastPForLib::StreamVByte,
    StreamVByteBlockCodec>
{
    static constexpr const char * NAME = "streamvbyte";
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
