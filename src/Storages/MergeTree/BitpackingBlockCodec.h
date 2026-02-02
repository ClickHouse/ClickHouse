#pragma once

#include <span>
#include <config.h>
#include <Common/Exception.h>

#if USE_SIMDCOMP
extern "C"
{
#include <simdcomp.h>
}
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
}

namespace impl
{

template <bool has_simdcomp>
struct BitpackingBlockCodecImpl;

#if USE_SIMDCOMP

static constexpr bool has_simdcomp = true;

template<>
struct BitpackingBlockCodecImpl<true>
{
    static constexpr const char * NAME = "bitpacking";
    /// Block size for bitpacking compression. SIMDComp works best with 128-element blocks.
    static constexpr size_t BLOCK_SIZE = 128;

    /// Returns the codec name for identification.
    static constexpr const char * name() noexcept { return NAME; }

    /// Returns the estimated compressed bytes needed for encoding the data.
    /// The actual max_bits calculation and serialization is handled by encode().
    static size_t calculateNeededBytes(const std::span<uint32_t> & data) noexcept
    {
        if (data.empty())
            return 0;

        uint32_t n = static_cast<uint32_t>(data.size());
        auto bits = maxbits_length(data.data(), n);
        auto bytes = simdpack_compressedbytes(n, bits);
        return 1 + bytes;  // 1 byte header + payload
    }

    static size_t encode(std::span<uint32_t> & in, std::span<char> & out)
    {
        if (in.empty())
            return 0;

        /// Calculate max bits
        uint32_t n = static_cast<uint32_t>(in.size());
        auto max_bits = maxbits_length(in.data(), n);

        if (max_bits > 32)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid bit width {} bits must be in [0, 32].", max_bits);

        /// Write max_bits header (1 byte)
        if (out.size() < 1)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Output buffer too small for header");

        out[0] = static_cast<char>(max_bits);
        auto payload_out = out.subspan(1);

        /// simdcomp expects __m128i* output pointer; we compute consumed bytes
        /// from the returned end pointer (in units of 16-byte vectors).
        __m128i * m128_out = reinterpret_cast<__m128i *>(payload_out.data());
        __m128i * m128_out_end = simdpack_length(in.data(), in.size(), m128_out, max_bits);
        size_t payload_bytes = static_cast<size_t>(m128_out_end - m128_out) * sizeof(__m128i);
        size_t total_bytes = 1 + payload_bytes;

        in = in.subspan(in.size());
        out = out.subspan(total_bytes);
        return total_bytes;
    }

    static size_t decode(std::span<const std::byte> & in, size_t n, std::span<uint32_t> & out)
    {
        if (n == 0)
            return 0;

        if (in.empty())
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Input buffer is empty but need to decode {} integers", n);

        /// Read max_bits header (1 byte)
        uint32_t max_bits = static_cast<uint32_t>(in[0]);
        auto payload_in = in.subspan(1);

        if (max_bits > 32)
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Invalid bit width {}, must be in [0, 32]", max_bits);

        /// Special case: if max_bits == 0, all values are 0
        if (max_bits == 0)
        {
            std::memset(out.data(), 0, n * sizeof(uint32_t));
            in = in.subspan(1);
            out = out.subspan(n);
            return 1;
        }

        /// simdcomp expects __m128i* input pointer; we compute consumed bytes
        /// from the returned end pointer (in units of 16-byte vectors).
        const __m128i * m128i_in = reinterpret_cast<const __m128i *>(payload_in.data());
        const __m128i * m128i_in_end = simdunpack_length(m128i_in, n, out.data(), max_bits);
        size_t payload_bytes = static_cast<size_t>(m128i_in_end - m128i_in) * sizeof(__m128i);
        size_t total_bytes = 1 + payload_bytes;

        in = in.subspan(total_bytes);
        out = out.subspan(n);
        return total_bytes;
    }
};
#else

static constexpr bool has_simdcomp = false;

#endif

/// Generic implementation for non-x86/SSE platforms (simdcomp sadly requires SSE2 and provides no fallback on its own).
/// It aims to produce a 100% binary-compatible output as simdcomp.
template<>
struct BitpackingBlockCodecImpl<false>
{
    static constexpr const char * NAME = "bitpacking";
    /// Block size for bitpacking compression. Must match the SIMD version (128 elements).
    static constexpr size_t BLOCK_SIZE = 128;

    /// Returns the codec name for identification.
    static constexpr const char * name() noexcept { return NAME; }

    /// Returns the estimated compressed bytes needed for encoding the data.
    /// The actual max_bits calculation and serialization is handled by encode().
    static size_t calculateNeededBytes(const std::span<uint32_t> & data) noexcept
    {
        if (data.empty())
            return 0;

        size_t n = data.size();
        uint32_t bits = maxbitsLength(data);
        chassert(bits <= 32);
        size_t bytes = bitpackingCompressedBytes(n, bits);
        return 1 + bytes;  // 1 byte header + payload
    }

    /// Encodes (packs) a sequence of 32-bit integers into the SIMDComp-compatible bitpacked byte stream.
    /// - `in`: input values to compress.
    /// - `out`: destination buffer; the function writes the packed stream into it
    ///          and advances `out` to point past the written bytes.
    /// Returns: number of bytes written into `out` (including 1-byte header + packed payload).
    static size_t encode(std::span<uint32_t> & in, std::span<char> & out)
    {
        if (in.empty())
            return 0;

        /// Calculate max bits
        uint32_t max_bits = maxbitsLength(in);

        if (max_bits > 32)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid bit width {} bits must be in [0, 32].", max_bits);

        /// Write max_bits header (1 byte)
        if (out.size() < 1)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Output buffer too small for header");

        out[0] = static_cast<char>(max_bits);
        auto payload_out = out.subspan(1);

        char * data_out = payload_out.data();
        char * data_out_end = packingLength(in.data(), in.size(), data_out, max_bits);
        size_t payload_bytes = static_cast<size_t>(data_out_end - data_out);
        size_t total_bytes = 1 + payload_bytes;

        in = in.subspan(in.size());
        out = out.subspan(total_bytes);
        return total_bytes;
    }

    /// Decodes (unpacks) a SIMDComp-compatible bitpacked byte stream back into 32-bit integers.
    /// - `in`: source byte stream; the function consumes exactly the bytes needed
    ///         to decode `n` integers and advances `in` past the consumed bytes.
    /// - `n`: number of integers to decode.
    /// - `out`: destination span for decoded integers; must have at least `n` slots.
    /// Returns: number of bytes consumed from `in` (including 1-byte header + packed payload).
    static size_t decode(std::span<const std::byte> & in, size_t n, std::span<uint32_t> & out)
    {
        if (n == 0)
            return 0;

        if (in.empty())
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Input buffer is empty but need to decode {} integers", n);

        /// Read max_bits header (1 byte)
        uint32_t max_bits = static_cast<uint32_t>(in[0]);
        auto payload_in = in.subspan(1);

        if (max_bits > 32)
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Invalid bit width {}, must be in [0, 32]", max_bits);

        /// Special case: if max_bits == 0, all values are 0
        if (max_bits == 0)
        {
            std::memset(out.data(), 0, n * sizeof(uint32_t));
            in = in.subspan(1);
            out = out.subspan(n);
            return 1;
        }

        const char * data_in = reinterpret_cast<const char *>(payload_in.data());
        const char * data_in_end = unpackingLength(data_in, n, out.data(), max_bits);
        size_t payload_bytes = static_cast<size_t>(data_in_end - data_in);
        size_t total_bytes = 1 + payload_bytes;

        in = in.subspan(total_bytes);
        out = out.subspan(n);
        return total_bytes;
    }

private:
    /// Non-SSE version: equivalent to SIMDComp maxbits_length.
    /// It OR-reduces all values to compute required bit width.
    static uint32_t maxbitsLength(const std::span<uint32_t> & in) noexcept
    {
        size_t n = in.size();
        uint32_t xored_in = 0;
        // Process in chunks of 4 (mirrors the SIMD path grouping), but without SSE.
        size_t offset = (n / 4) * 4;

        for (size_t k = 0; k < offset; k += 4)
        {
            xored_in |= in[k + 0];
            xored_in |= in[k + 1];
            xored_in |= in[k + 2];
            xored_in |= in[k + 3];
        }

        // Tail
        for (size_t k = offset; k < n; ++k)
            xored_in |= in[k];

        // Bits(xored_in): 0 -> 0, else 32 - clz(xored_in)
        if (xored_in == 0)
            return 0u;
        else
            return 32u - static_cast<uint32_t>(__builtin_clz(xored_in));
    }

    static size_t bitpackingCompressedBytes(size_t count, uint32_t bits) noexcept
    {
        if (bits == 0)
            return 0; // no payload for bits==0
        if (bits == 32)
            return count * sizeof(uint32_t); // raw data (payload only)

        size_t groups = (count + 3) / 4;
        size_t words32 = (groups * static_cast<size_t>(bits) + 31) / 32;

        return words32 * 16; // payload only, no header
    }

    static uint32_t maskForBits(size_t bits) noexcept
    {
        return (bits == 32) ? 0xFFFFFFFFu :
            (bits == 0)  ? 0u :
            static_cast<uint32_t>((static_cast<uint64_t>(1) << bits) - 1u);
    }

    /// Pack `groups` groups of 4x32-bit integers (total = groups*4 values) into the
    /// SIMDComp-compatible "horizontal 4-lane" bitpacked layout.
    /// SIMDComp’s on-wire/on-disk format is defined in terms of 128-bit chunks
    /// (conceptually one __m128i = four 32-bit lanes). In this fallback path we do
    /// NOT require the output buffer to be 16-byte aligned: we write 16 bytes per
    /// packed word via memcpy into a byte buffer.
    ///
    /// Special cases:
    /// - Bits == 0  : nothing is written.
    /// - Bits == 32 : values are copied as raw uint32_t (no bitpacking), still grouped
    ///               as 4 words per 16-byte chunk.
    ///
    /// Parameters:
    /// - in     : pointer to input uint32_t values (must have at least groups*4 elements).
    /// - groups : number of 4-value groups to pack.
    /// - out    : pointer to the output byte buffer. The buffer must have enough space
    ///            for the produced payload (multiple of 16 bytes for Bits in [1..31];
    ///            exactly groups*16 bytes for Bits==32).
    ///
    /// Returns:
    /// - Pointer to the first byte *after* the written output.
    template<uint32_t Bits>
    static char * packFixed(const uint32_t * in, size_t groups, char * out) noexcept
    {
        static_assert(Bits <= 32, "Bits must be 0..32");

        if constexpr (Bits == 0)
            return out;

        if (groups == 0)
            return out;

        /// Bits==32: store raw 32-Bits words (4 per m128i). This matches SIMDComp's
        /// special-case behavior for full blocks.
        if constexpr (Bits == 32)
        {
            std::memcpy(out, in, groups * 4 * sizeof(uint32_t));
            return out + groups * 16;
        }

        /// Mask to keep only the lowest Bits bits (Bits in 1..31 here).
        const uint32_t mask = maskForBits(Bits);
        /// Per-lane bit accumulators. We use 64-bit so we can append bits and flush
        /// 32-bit words without losing leftover bits.
        uint64_t acc0 = 0;
        uint64_t acc1 = 0;
        uint64_t acc2 = 0;
        uint64_t acc3 = 0;
        /// How many valid bits currently in each accumulator.
        uint32_t acc_bits = 0;

        char * p = out;

        for (size_t i = 0; i < groups; ++i)
        {
            /// Load one group (4 lanes) and keep only Bits bits.
            const uint32_t v0 = in[4 * i + 0] & mask;
            const uint32_t v1 = in[4 * i + 1] & mask;
            const uint32_t v2 = in[4 * i + 2] & mask;
            const uint32_t v3 = in[4 * i + 3] & mask;

            /// Append this group's bits at the current bit offset.
            acc0 |= (static_cast<uint64_t>(v0) << acc_bits);
            acc1 |= (static_cast<uint64_t>(v1) << acc_bits);
            acc2 |= (static_cast<uint64_t>(v2) << acc_bits);
            acc3 |= (static_cast<uint64_t>(v3) << acc_bits);
            acc_bits += Bits;

            /// When we've accumulated at least 32 bits, flush one 32-bit word per lane
            /// into one output m128i.
            if (acc_bits >= 32)
            {
                uint32_t tmp[4] = { static_cast<uint32_t>(acc0), static_cast<uint32_t>(acc1), static_cast<uint32_t>(acc2), static_cast<uint32_t>(acc3), };
                std::memcpy(p, tmp, 16);
                p += 16;

                /// Keep leftover bits (at most 31 bits remain because Bits<=31).
                acc0 >>= 32;
                acc1 >>= 32;
                acc2 >>= 32;
                acc3 >>= 32;
                acc_bits -= 32;
            }
        }

        /// Flush any remaining partial word at the end.
        if (acc_bits != 0)
        {
            uint32_t tmp[4] = { static_cast<uint32_t>(acc0), static_cast<uint32_t>(acc1), static_cast<uint32_t>(acc2), static_cast<uint32_t>(acc3), };
            std::memcpy(p, tmp, 16);
            p += 16;
        }

        return p;
    }


    /// Unpack `groups` groups of 4 integers (total = groups*4 values) from a
    /// SIMDComp-compatible "horizontal 4-lane" bitpacked byte stream.
    ///
    /// The on-wire/on-disk format is conceptually organized in 16-byte chunks
    /// (equivalent to one __m128i viewed as four 32-bit lanes). This fallback path
    /// does NOT require 16-byte alignment: it refills the lane reservoirs by
    /// memcpy-ing 16 bytes from the byte stream into four uint32_t words.
    ///
    /// Behavior by Bits:
    /// - Bits == 0  : no payload is stored/consumed. (Caller may treat decoded values as zeros.)
    /// - Bits == 32 : values are stored as raw uint32_t; copy groups*4 words and advance by groups*16 bytes.
    /// - Bits 1..31 : use four 64-bit lane accumulators (acc0..acc3) as bit reservoirs.
    ///               Refill by reading one 16-byte chunk (4×uint32_t, 32 bits per lane)
    ///               whenever fewer than Bits bits remain, then extract one 4-value group
    ///               in LSB-first order.
    ///
    /// Parameters:
    /// - in     : pointer to the compressed input byte stream.
    /// - groups : number of 4-value groups to decode.
    /// - out    : destination for decoded integers (must have at least groups*4 elements).
    ///
    /// Returns:
    /// - Pointer to the first byte *after* the consumed input.
    template<uint32_t Bits>
    static const char * unpackFixed(const char * in, size_t groups, uint32_t * out) noexcept
    {
        static_assert(Bits <= 32, "Bits must be 0..32");
        if (groups == 0) return in;

        /// Bits==0: no payload in the stream;
        if constexpr (Bits == 0)
            return in;

        /// Bits==32: stream stores raw uint32_t values (4 per group / per m128i).
        if constexpr (Bits == 32)
        {
            size_t copy_size = groups * 4 * sizeof(uint32_t);
            std::memcpy(out, in, copy_size);
            return in + copy_size;
        }

        /// Bits=1..31: mask to keep only the lowest Bits bits from each extracted value.
        const uint32_t mask = maskForBits(Bits);
        /// Per-lane bit reservoirs (64-bit to hold leftover bits across word boundaries).
        uint64_t acc0 = 0;
        uint64_t acc1 = 0;
        uint64_t acc2 = 0;
        uint64_t acc3 = 0;

        uint32_t acc_bits = 0;

        const char * p = in;
        size_t produced_groups = 0;

        while (produced_groups < groups)
        {
            /// Refill reservoirs if there aren't enough bits to extract one Bits-bit value.
            /// Reading one m128i adds 32 bits per lane.
            if (acc_bits < Bits)
            {
                uint32_t tmp[4];
                std::memcpy(tmp, p, 16);
                p += 16;

                acc0 |= (static_cast<uint64_t>(tmp[0]) << acc_bits);
                acc1 |= (static_cast<uint64_t>(tmp[1]) << acc_bits);
                acc2 |= (static_cast<uint64_t>(tmp[2]) << acc_bits);
                acc3 |= (static_cast<uint64_t>(tmp[3]) << acc_bits);
                acc_bits += 32;
                continue;
            }

            /// Extract one value per lane (LSB-first), forming one 4-integer group.
            /// SIMDComp’s fastpack1_32 is a classic example of LSB-first.
            out[4 * produced_groups + 0] = static_cast<uint32_t>(acc0) & mask;
            out[4 * produced_groups + 1] = static_cast<uint32_t>(acc1) & mask;
            out[4 * produced_groups + 2] = static_cast<uint32_t>(acc2) & mask;
            out[4 * produced_groups + 3] = static_cast<uint32_t>(acc3) & mask;

            /// Consume Bits bits from each lane reservoir.
            acc0 >>= Bits;
            acc1 >>= Bits;
            acc2 >>= Bits;
            acc3 >>= Bits;
            acc_bits -= Bits;

            ++produced_groups;
        }

        return p;
    }

    /// Pack a tail segment (0 < tail < COMPRESSED_BLOCK_SIZE) into the SIMDComp-compatible
    /// horizontal 4-lane bitpacked byte stream.
    ///
    /// This is used for the final partial block when the total number of input integers is
    /// not a multiple of the full block size (e.g. 128 integers). The packed layout is
    /// compatible with SIMDComp’s 4×32-bit lane organization (conceptually 16-byte chunks),
    /// but this fallback implementation operates on a byte buffer and does NOT require
    /// 16-byte alignment (we emit 16-byte chunks via memcpy).
    ///
    /// Behavior by Bits:
    /// - Bits == 0  : no payload is written.
    /// - Bits == 32 : write raw uint32_t values tightly (tail * 4 bytes), no bitpacking.
    /// - Bits 1..31 : pack values LSB-first into four lane reservoirs and flush 32-bit
    ///               words per lane as 16-byte chunks.
    ///
    /// Parameters:
    /// - in   : pointer to the input integers (must have at least `tail` elements).
    /// - tail : number of integers to pack (typically 1..127; 0 is allowed and writes nothing).
    /// - out  : destination pointer to the compressed byte stream. The caller must ensure
    ///          sufficient capacity for the produced payload.
    ///
    /// Returns:
    /// - Pointer to the first byte after the written output.
    template<uint32_t Bits>
    static char * packTail(const uint32_t * in, size_t tail, char * out) noexcept
    {
        static_assert(Bits <= 32, "Bits must be 0..32");
        if (tail == 0) return out;

        /// Bits==0: no payload is written;
        if constexpr (Bits == 0)
            return out;

        /// Bits==32: store raw 32-bit words tightly (SIMDComp's special-case behavior).
        if constexpr (Bits == 32)
        {
            size_t copy_size = tail * sizeof(uint32_t);
            std::memcpy(out, in, copy_size);
            return out + copy_size;
        }

        /// Bits=1..31: mask keeps only the lowest Bits bits of each input value.
        const uint32_t mask = maskForBits(Bits);

        /// Per-lane bit reservoirs. We maintain four independent accumulators
        /// (one per lane) and flush 32-bit words when enough bits are available.
        uint64_t acc0 = 0;
        uint64_t acc1 = 0;
        uint64_t acc2 = 0;
        uint64_t acc3 = 0;
        uint32_t acc_bits = 0;

        char * p = out;

        const size_t full_groups = tail / 4;
        const size_t rem = tail % 4;

        /// Process complete 4-value groups first.
        for (size_t i = 0; i < full_groups; ++i)
        {
            /// Load one group (4 lanes), keep only Bits bits.
            const uint32_t v0 = in[4 * i + 0] & mask;
            const uint32_t v1 = in[4 * i + 1] & mask;
            const uint32_t v2 = in[4 * i + 2] & mask;
            const uint32_t v3 = in[4 * i + 3] & mask;

            /// Append this group's Bits bits to each lane's accumulator at the current offset.
            acc0 |= (static_cast<uint64_t>(v0) << acc_bits);
            acc1 |= (static_cast<uint64_t>(v1) << acc_bits);
            acc2 |= (static_cast<uint64_t>(v2) << acc_bits);
            acc3 |= (static_cast<uint64_t>(v3) << acc_bits);
            acc_bits += Bits;

            /// Flush one 32-bit word per lane into one output m128i when possible.
            if (acc_bits >= 32)
            {
                uint32_t tmp[4] = { static_cast<uint32_t>(acc0), static_cast<uint32_t>(acc1), static_cast<uint32_t>(acc2), static_cast<uint32_t>(acc3), };
                std::memcpy(p, tmp, 16);
                p += 16;

                acc0 >>= 32;
                acc1 >>= 32;
                acc2 >>= 32;
                acc3 >>= 32;
                acc_bits -= 32;
            }
        }

        /// If there is a partial last group (1..3 values), pad missing lanes with 0
        /// and treat it as a complete group for packing purposes.
        if (rem != 0)
        {
            const size_t base = full_groups * 4;
            const uint32_t v0 = in[base + 0] & mask;
            const uint32_t v1 = (rem > 1 ? in[base + 1] : 0u) & mask;
            const uint32_t v2 = (rem > 2 ? in[base + 2] : 0u) & mask;
            const uint32_t v3 = 0u;

            acc0 |= (static_cast<uint64_t>(v0) << acc_bits);
            acc1 |= (static_cast<uint64_t>(v1) << acc_bits);
            acc2 |= (static_cast<uint64_t>(v2) << acc_bits);
            acc3 |= (static_cast<uint64_t>(v3) << acc_bits);
            acc_bits += Bits;

            if (acc_bits >= 32)
            {
                uint32_t tmp[4] = { static_cast<uint32_t>(acc0), static_cast<uint32_t>(acc1), static_cast<uint32_t>(acc2), static_cast<uint32_t>(acc3), };
                std::memcpy(p, tmp, 16);
                p += 16;

                acc0 >>= 32;
                acc1 >>= 32;
                acc2 >>= 32;
                acc3 >>= 32;
                acc_bits -= 32;
            }
        }

        /// Flush the final partial output word if any bits remain.
        if (acc_bits != 0)
        {
            uint32_t tmp[4] = { static_cast<uint32_t>(acc0), static_cast<uint32_t>(acc1), static_cast<uint32_t>(acc2), static_cast<uint32_t>(acc3), };
            std::memcpy(p, tmp, 16);
            p += 16;
        }

        return p;
    }

    /// Unpack (decode) a tail segment (0 < tail < COMPRESSED_BLOCK_SIZE) from a
    /// SIMDComp-compatible horizontal 4-lane bitpacked *byte stream*.
    ///
    /// This is the counterpart of packTail<Bits>(). It decodes the last partial block
    /// when the total number of integers is not a multiple of the full block size
    /// (e.g. 128 integers).
    ///
    /// Special cases:
    /// - Bits == 0  : no payload is stored/consumed. (Caller may treat decoded values as zeros.)
    /// - Bits == 32 : values are stored as raw uint32_t, tightly packed (tail * 4 bytes),
    ///               with no 16-byte chunk padding.
    ///
    /// Parameters:
    /// - in   : pointer to the compressed input byte stream.
    /// - tail : number of integers to decode (typically 1..127; 0 is allowed and consumes nothing).
    /// - out  : destination buffer (must have at least `tail` elements).
    ///
    /// Returns:
    /// - Pointer to the first byte after the consumed input.
    template<uint32_t Bits>
    static const char * unpackTail(const char * in, size_t tail, uint32_t * out) noexcept
    {
        static_assert(Bits <= 32, "Bits must be 0..32");
        if (tail == 0) return in;

        /// Bits==0: no stored bits;
        if constexpr (Bits == 0)
            return in;

        /// Bits==32: raw uint32_t values are stored tightly (SIMDComp's special-case behavior).
        if constexpr (Bits == 32)
        {
            size_t copy_size = tail * sizeof(uint32_t);
            std::memcpy(out, in, copy_size);
            return in + copy_size;
        }

        // Bits = 1..31
        const uint32_t mask = maskForBits(Bits);

        /// Number of 4-lane groups that were packed for this tail:
        /// packing pads the last incomplete group (if any) with zeros, so we must decode
        /// ceil(tail/4) groups from the stream.
        const size_t groups = (tail + 3) / 4;
        const size_t full_groups = tail / 4;
        const size_t rem = tail % 4;

        /// Per-lane bit reservoirs (accumulators). We refill by reading one m128i (32 bits per lane)
        /// whenever we don't have enough bits to extract one Bits-bit value.
        uint64_t acc0 = 0;
        uint64_t acc1 = 0;
        uint64_t acc2 = 0;
        uint64_t acc3 = 0;
        uint32_t acc_bits = 0;

        const char * p = in;

        for (size_t g = 0; g < groups; ++g)
        {
            /// Refill accumulators if fewer than Bits bits remain.
            /// Reading one m128i contributes 32 bits to each lane.
            if (acc_bits < Bits)
            {
                uint32_t tmp[4];
                std::memcpy(tmp, p, 16);
                p += 16;
                acc0 |= (static_cast<uint64_t>(tmp[0]) << acc_bits);
                acc1 |= (static_cast<uint64_t>(tmp[1]) << acc_bits);
                acc2 |= (static_cast<uint64_t>(tmp[2]) << acc_bits);
                acc3 |= (static_cast<uint64_t>(tmp[3]) << acc_bits);
                acc_bits += 32;
            }

            /// Extract one value per lane from the low Bits bits (LSB-first).
            const uint32_t v0 = static_cast<uint32_t>(acc0) & mask;
            const uint32_t v1 = static_cast<uint32_t>(acc1) & mask;
            const uint32_t v2 = static_cast<uint32_t>(acc2) & mask;
            const uint32_t v3 = static_cast<uint32_t>(acc3) & mask;

            /// Consume Bits bits from each lane accumulator.
            acc0 >>= Bits;
            acc1 >>= Bits;
            acc2 >>= Bits;
            acc3 >>= Bits;
            acc_bits -= Bits;

            /// For full groups, write all 4 output values.
            if (g < full_groups)
            {
                out[4 * g + 0] = v0;
                out[4 * g + 1] = v1;
                out[4 * g + 2] = v2;
                out[4 * g + 3] = v3;
            }
            /// For the final partial group, only write the existing `rem` values
            /// (the missing lanes were zero-padded during packing).
            else if (rem != 0)
            {
                const size_t base = 4 * g;
                out[base + 0] = v0;
                if (rem > 1) out[base + 1] = v1;
                if (rem > 2) out[base + 2] = v2;
                /// lane3 is omitted when rem < 4
            }
        }

        return p;
    }

    using packing_func = char * (*)(const uint32_t *, size_t, char *) noexcept;

/// Macro to generate all 33 function pointer entries (0..32) for a template function
#define BITPACKING_FUNC_TABLE_33(func) \
    &func<0>,  &func<1>,  &func<2>,  &func<3>,  &func<4>,  &func<5>,  &func<6>,  &func<7>,  \
    &func<8>,  &func<9>,  &func<10>, &func<11>, &func<12>, &func<13>, &func<14>, &func<15>, \
    &func<16>, &func<17>, &func<18>, &func<19>, &func<20>, &func<21>, &func<22>, &func<23>, \
    &func<24>, &func<25>, &func<26>, &func<27>, &func<28>, &func<29>, &func<30>, &func<31>, \
    &func<32>

    static packing_func getPackFixedFunc(uint32_t bits)
    {
        chassert(bits <= 32);
        static const packing_func table[33] = { BITPACKING_FUNC_TABLE_33(packFixed) };
        return table[bits];
    }

    static packing_func getPackTailFunc(uint32_t bits)
    {
        chassert(bits <= 32);
        static const packing_func table[33] = { BITPACKING_FUNC_TABLE_33(packTail) };
        return table[bits];
    }

    using unpack_func = const char * (*)(const char *, size_t, uint32_t *) noexcept;

    static unpack_func getUnpackFixedFunc(uint32_t bit)
    {
        chassert(bit <= 32);
        static const unpack_func table[33] = { BITPACKING_FUNC_TABLE_33(unpackFixed) };
        return table[bit];
    }

    static unpack_func getUnpackTailFunc(uint32_t bit)
    {
        chassert(bit <= 32);
        static const unpack_func table[33] = { BITPACKING_FUNC_TABLE_33(unpackTail) };
        return table[bit];
    }

#undef BITPACKING_FUNC_TABLE_33

    /// Pack (encode) `length` 32-bit integers into the SIMDComp-compatible bitpacked stream.
    /// The input is processed in two parts:
    /// 1) Full blocks of BLOCK_SIZE integers (SIMDComp block is typically 128 ints).
    ///    Each full block contains exactly 32 "groups" of 4 integers (4 lanes), so we call
    ///    the fixed-block packer with groups=32. For BIT=1..31 this produces exactly BIT
    ///    output m128i words per block (format guarantee). For BIT=32 it copies raw words.
    /// 2) A remaining tail (length % BLOCK_SIZE), which is packed using the tail packer.
    ///    The tail packer must preserve SIMDComp's exact short-length behavior, including:
    ///    - zero-padding to complete the last 4-lane group when tail%4 != 0 (for BIT<32),
    ///    - tight uint32_t copy for BIT==32 (no 16-byte padding).
    ///
    /// Parameters:
    /// - in     : pointer to the input integers (at least `length` elements).
    /// - length : number of integers to pack.
    /// - out    : destination pointer to the compressed stream.
    /// - bit    : bit-width per value (0..32).
    ///
    /// Returns:
    /// - Pointer to the first output position after the written compressed data.
    static char * packingLength(const uint32_t * in, size_t length, char * out, uint32_t bit) noexcept
    {
        /// Select the fixed-block packer for this bit width.
        auto func = getPackFixedFunc(bit);

        /// Process all complete blocks. Each block has 32 groups (128 ints / 4).
        size_t blocks = length / BLOCK_SIZE;
        for (size_t i = 0; i < blocks; ++i)
        {
            out = func(in, 32, out);
            in += BLOCK_SIZE;
        }

        /// Pack the remaining tail (0..BLOCK_SIZE-1 ints), if any.
        const size_t tail = length % BLOCK_SIZE;
        if (tail == 0)
            return out;

        /// Select the tail packer (short-length path) for this bit width.
        func = getPackTailFunc(bit);
        return func(in, tail, out);
    }

    /// Unpack (decode) `length` 32-bit integers from a SIMDComp-compatible bitpacked stream.
    ///
    /// The input stream is consumed in two stages, mirroring packingLength():
    /// 1) Full blocks of BLOCK_SIZE integers (typically 128). Each full block corresponds
    ///    to exactly 32 groups of 4 integers (4 lanes). We therefore call the fixed-block
    ///    unpacker with groups=32. The fixed-block unpacker advances the input pointer by
    ///    the exact number of bytes consumed for this bit width (BIT × 16 bytes for 1..31,
    ///    128 × 4 bytes for BIT==32, and 0 for BIT==0).
    /// 2) A remaining tail (length % BLOCK_SIZE). The tail unpacker handles short-length
    ///    behavior and must preserve SIMDComp semantics, including:
    ///    - padding-aware decoding for BIT<32 (the encoder may have zero-padded the last group),
    ///    - tight uint32_t copy for BIT==32 (no 16-byte padding), advancing the input pointer
    ///      by `tail` uint32_t words (possibly resulting in a non-16B aligned pointer).
    ///
    /// Parameters:
    /// - in     : pointer to the compressed input stream (m128i words).
    /// - length : number of integers to decode.
    /// - out    : destination buffer for decoded integers (must have at least `length` slots).
    /// - bit    : bit-width used during encoding (0..32).
    ///
    /// Returns:
    /// - Pointer to the first input position after the consumed compressed data.
    static const char * unpackingLength(const char * in, size_t length, uint32_t * out, uint32_t bit) noexcept
    {
        /// Select the fixed-block decoder for this bit width.
        auto func = getUnpackFixedFunc(bit);

        /// Decode all complete blocks. Each block has 32 groups (128 ints / 4).
        const size_t blocks = length / BLOCK_SIZE;
        for (size_t i = 0; i < blocks; ++i)
        {
            in = func(in, 32, out);
            out += BLOCK_SIZE;
        }

        /// Decode the remaining tail (0..BLOCK_SIZE-1 ints), if any.
        size_t tail = length % BLOCK_SIZE;
        if (tail == 0)
            return in;

        /// Select the tail (short-length) decoder for this bit width.
        func = getUnpackTailFunc(bit);
        return func(in, tail, out);
    }
};

}

using BitpackingBlockCodec = impl::BitpackingBlockCodecImpl<impl::has_simdcomp>;

}

