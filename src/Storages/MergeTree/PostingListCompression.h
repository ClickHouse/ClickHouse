#pragma once

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
}

static constexpr size_t BLOCK_SIZE = 128;

namespace impl
{

template <bool has_simdcomp>
struct BlockCodecImpl;

#if USE_SIMDCOMP

static constexpr bool has_simdcomp = true;

template<>
struct BlockCodecImpl<true>
{

    /// Returns {compressed_bytes, bits} where bits is the max bit-width required
    /// to represent all values in [0..n).
    static std::pair<size_t, size_t> calculateNeededBytesAndMaxBits(std::span<uint32_t> & data) noexcept
    {
        size_t n = data.size();
        auto bits = maxbits_length(data.data(), n);
        auto bytes = simdpack_compressedbytes(n, bits);
        return {bytes, bits};
    }

    static uint32_t encode(std::span<uint32_t> & in, uint32_t max_bits, std::span<char> & out)
    {
        if (max_bits > 32)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid bit width {} bits must be in [0, 32].", max_bits);
        /// simdcomp expects __m128i* output pointer; we compute consumed bytes
        /// from the returned end pointer (in units of 16-byte vectors).
        auto * m128_out = reinterpret_cast<__m128i *>(out.data());
        auto * m128_out_end = simdpack_length(in.data(), in.size(), m128_out, max_bits);
        auto used = static_cast<size_t>(m128_out_end - m128_out) * sizeof(__m128i);
        out = out.subspan(used);
        return used;
    }

    static size_t decode(std::span<const std::byte> & in, size_t n, uint32_t max_bits, std::span<uint32_t> & out)
    {
        if (max_bits > 32)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid bit width {} bits must be in [0, 32].", max_bits);
        /// simdcomp expects __m128i* input pointer; we compute consumed bytes
        /// from the returned end pointer (in units of 16-byte vectors).
        auto * m128i_in = reinterpret_cast<const __m128i *>(in.data());
        auto * m128i_in_end = simdunpack_length(m128i_in, n, out.data(), max_bits);
        auto used = static_cast<size_t>(m128i_in_end - m128i_in) * sizeof(__m128);
        in = in.subspan(used);
        return used;
    }
};
#else

static constexpr bool has_simdcomp = false;

#endif

/// Generic implementation on non-x86/SSE platforms (simdcomp sadly requires SSE2 and provides no fallback on its own).
/// It aims to be 100% bit-compatible to simdcomp's output.
template<>
struct BlockCodecImpl<false>
{
    /// A portable replacement for SSE's __m128i.
    /// We only rely on its *layout* (16 bytes total, 4 lanes of 32-bit words),
    /// because SIMDComp stores packed data as an array of 128-bit "words".
    /// This type makes the on-disk / on-wire format identical on non-SSE platforms.
    struct alignas(16) m128i { uint32_t u32[4]; };

    /// Ensure the type is exactly 128 bits. If this ever changes, the packed format
    /// (pointer arithmetic, byte counts, and decode correctness) would break.
    static_assert(sizeof(m128i) == 16, "m128i must be 16 bytes");

    /// Non-SSE version: equivalent to SIMDComp maxbits_length.
    /// It OR-reduces all values to compute required bit width.
    [[maybe_unused]] static uint32_t maxbitsLength(const std::span<uint32_t> & in) noexcept
    {
        size_t n = in.size();
        uint32_t xored_in = 0;
        // Process in chunks of 4 (mirrors the SIMD path grouping), but without SSE.
        const uint32_t offset = (n / 4) * 4;

        for (uint32_t k = 0; k < offset; k += 4)
        {
            xored_in |= in[k + 0];
            xored_in |= in[k + 1];
            xored_in |= in[k + 2];
            xored_in |= in[k + 3];
        }

        // Tail
            for (uint32_t k = offset; k < n; ++k)
                xored_in |= in[k];

            // Bits(xored_in): 0 -> 0, else 32 - clz(xored_in)
            if (xored_in == 0)
                return 0u;
            else
                return 32u - static_cast<uint32_t>(__builtin_clz(xored_in));
        }

        [[maybe_unused]] static size_t bitpackingCompressedBytes(int length, uint32_t bit) noexcept
        {
            if (bit == 0)
                return 0;
            if (bit == 32)
                return length * sizeof(uint32_t);

            size_t groups = (length + 3) / 4;
            size_t words32 = (groups * static_cast<size_t>(bit) + 31) / 32;

            return words32 * sizeof(m128i);
        }

        /// Returns {compressed_bytes, bits} where bits is the max bit-width required
        /// to represent all values in [0..n).
        [[maybe_unused]] static std::pair<size_t, size_t> calculateNeededBytesAndMaxBits(const std::span<uint32_t> & data) noexcept
        {
            size_t n = data.size();
            size_t bits = maxbitsLength(data);
            size_t bytes = bitpackingCompressedBytes(n, bits);
            return {bytes, bits};
        }

        /// Encodes (packs) a sequence of 32-bit integers into the SIMDComp-compatible bitpacked byte stream.
        /// - `in`: input values to compress.
        /// - `max_bits`: bit-width used for each value (0..32). Must match the decoder.
        /// - `out`: destination buffer; the function writes the packed stream into it
        ///          and advances `out` to point past the written bytes.
        /// Returns: number of bytes written into `out` (the packed payload size).
        [[maybe_unused]] static uint32_t encode(std::span<uint32_t> & in, uint32_t max_bits, std::span<char> & out)
        {
            if (max_bits > 32)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid bit width {} bits must be in [0, 32].", max_bits);
            auto * m128_out = reinterpret_cast<m128i *>(out.data());
            auto * m128_out_end = packingLength(in.data(), in.size(), m128_out, max_bits);
            auto used = static_cast<size_t>(m128_out_end - m128_out) * sizeof(m128i);
            out = out.subspan(used);
            return used;
        }

        /// Decodes (unpacks) a SIMDComp-compatible bitpacked byte stream back into 32-bit integers.
        /// - `in`: source byte stream; the function consumes exactly the bytes needed
        ///         to decode `n` integers and advances `in` past the consumed bytes.
        /// - `n`: number of integers to decode.
        /// - `max_bits`: bit-width that was used during encoding (0..32). Must match
        ///               the encoder's `max_bits`.
        /// - `out`: destination span for decoded integers; must have at least `n` slots.
        /// Returns: number of bytes consumed from `in` (the packed payload size).
        [[maybe_unused]] static size_t decode(std::span<const std::byte> & in, size_t n, uint32_t max_bits, std::span<uint32_t> & out)
        {
            if (max_bits > 32)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid bit width {} bits must be in [0, 32].", max_bits);
            auto * m128i_in = reinterpret_cast<const m128i *>(in.data());
            auto * m128i_in_end = unpackingLength(m128i_in, n, out.data(), max_bits);
            auto used = static_cast<size_t>(m128i_in_end - m128i_in) * sizeof(m128i);
            in = in.subspan(used);
            return used;
        }

        /// Pack `groups` groups of 4x32-bit integers (total = groups*4 values) into the
        /// SIMDComp-compatible "horizontal 4-lane" bitpacked layout.
        /// SIMDComp uses “groups of 4×32-bit integers” because its on-wire/on-disk format is
        /// built around a 128-bit word (__m128i) viewed as four 32-bit lanes. To be fully format-compatible,
        /// we must preserve that exact layout.
        ///
        /// Special cases:
        /// - Bits == 0  : nothing is written.
        /// - Bits == 32 : values are copied as raw uint32_t (no bitpacking), still grouped as 4 words per m128i.
        ///
        /// Parameters:
        /// - in     : pointer to input uint32_t values (must have at least groups*4 elements).
        /// - groups : number of 4-value groups to pack.
        /// - out    : output pointer to m128i words.
        ///
        /// Returns:
        /// - Pointer to the first m128i *after* the written output.
        template<uint32_t Bits>
        [[maybe_unused]] static m128i * packFixed(const uint32_t * in, size_t groups, m128i * out) noexcept
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
                return out + groups;
            }

            /// Mask to keep only the lowest Bits bits (Bits in 1..31 here).
            const uint32_t mask = (Bits == 31) ? 0x7FFFFFFFu : ((static_cast<uint32_t>(1) << Bits) - 1u);

            /// Per-lane bit accumulators. We use 64-bit so we can append bits and flush
            /// 32-bit words without losing leftover bits.
            uint64_t acc0 = 0;
            uint64_t acc1 = 0;
            uint64_t acc2 = 0;
            uint64_t acc3 = 0;
            /// How many valid bits currently in each accumulator.
            uint32_t acc_bits = 0;

            m128i * p = out;

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
                    p->u32[0] = static_cast<uint32_t>(acc0);
                    p->u32[1] = static_cast<uint32_t>(acc1);
                    p->u32[2] = static_cast<uint32_t>(acc2);
                    p->u32[3] = static_cast<uint32_t>(acc3);
                    ++p;

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
                p->u32[0] = static_cast<uint32_t>(acc0);
                p->u32[1] = static_cast<uint32_t>(acc1);
                p->u32[2] = static_cast<uint32_t>(acc2);
                p->u32[3] = static_cast<uint32_t>(acc3);
                ++p;
            }

            return p;
        }


        /// Unpack `groups` groups of 4 integers (total = groups*4 values) from a
        /// SIMDComp-compatible "horizontal 4-lane" bitpacked stream.
        ///
        /// Behavior by Bits:
        /// - Bits == 0  : no bits were stored; values are implicit zeros (no input consumed).
        /// - Bits == 32 : values are stored as raw uint32_t; copy directly and advance by groups*4 words.
        /// - Bits 1..31 : use four 64-bit lane accumulators (acc0..acc3) as bit reservoirs.
        ///               We refill by reading one m128i (32 bits per lane) whenever the
        ///               reservoir has fewer than BIT bits, then extract one group.
        ///
        /// Parameters:
        /// - in     : pointer to the compressed input (m128i words).
        /// - groups : number of 4-value groups to decode.
        /// - out    : destination for decoded integers (must have at least groups*4 elements).
        ///
        /// Returns:
        /// - Pointer to the first m128i *after* the consumed input words.
        template<uint32_t Bits>
        [[maybe_unused]] static const m128i * unpackFixed(const m128i *in, size_t groups, uint32_t *out) noexcept
        {
            static_assert(Bits <= 32, "Bits must be 0..32");
            if (groups == 0) return in;

            /// Bits==0: no payload in the stream;
            if constexpr (Bits == 0)
                return in;

            /// Bits==32: stream stores raw uint32_t values (4 per group / per m128i).
            if constexpr (Bits == 32)
            {
                std::memcpy(out, in, groups * 4 * sizeof(uint32_t));
                return reinterpret_cast<const m128i *>(reinterpret_cast<const uint32_t *>(in) + groups * 4);
            }

            /// Bits=1..31: mask to keep only the lowest Bits bits from each extracted value.
            const uint32_t mask = (Bits == 31) ? 0x7FFFFFFFu : ((static_cast<uint32_t>(1) << Bits) - 1u);

            /// Per-lane bit reservoirs (64-bit to hold leftover bits across word boundaries).
            uint64_t acc0 = 0;
            uint64_t acc1 = 0;
            uint64_t acc2 = 0;
            uint64_t acc3 = 0;

            uint32_t acc_bits = 0;

            const m128i * p = in;
            size_t produced_groups = 0;

            while (produced_groups < groups)
            {
                /// Refill reservoirs if there aren't enough bits to extract one Bits-bit value.
                /// Reading one m128i adds 32 bits per lane.
                if (acc_bits < Bits)
                {
                    const uint32_t w0 = p->u32[0];
                    const uint32_t w1 = p->u32[1];
                    const uint32_t w2 = p->u32[2];
                    const uint32_t w3 = p->u32[3];
                    ++p;

                    acc0 |= (static_cast<uint64_t>(w0) << acc_bits);
                    acc1 |= (static_cast<uint64_t>(w1) << acc_bits);
                    acc2 |= (static_cast<uint64_t>(w2) << acc_bits);
                    acc3 |= (static_cast<uint64_t>(w3) << acc_bits);
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

        /// Pack a tail segment (0 < tail < BLOCK_SIZE) into the SIMDComp-compatible
        /// horizontal 4-lane bitpacked format.
        /// This function is used for the last partial block when the total length is
        /// not a multiple of 128 integers. It must preserve SIMDComp's exact byte layout.
        ///
        /// Parameters:
        /// - in   : pointer to the input integers (at least `tail` elements).
        /// - tail : number of integers to pack (0..127 typically).
        /// - out  : destination pointer to the compressed stream.
        ///
        /// Returns:
        /// - Pointer to the first output position after the written data.
        template<uint32_t Bits>
        [[maybe_unused]] static m128i * packTail(const uint32_t * in, size_t tail, m128i * out) noexcept
        {
            static_assert(Bits <= 32, "Bits must be 0..32");
            if (tail == 0) return out;

            /// Bits==0: no payload is written;
            if constexpr (Bits == 0)
                return out;

            /// Bits==32: store raw 32-bit words tightly (SIMDComp's special-case behavior).
            if constexpr (Bits == 32)
            {
                auto *out32 = reinterpret_cast<uint32_t *>(out);
                std::memcpy(out32, in, tail * sizeof(uint32_t));
                out32 += tail;
                return reinterpret_cast<m128i *>(out32);
            }

            /// Bits=1..31: mask keeps only the lowest Bits bits of each input value.
            const uint32_t mask = (Bits == 31) ? 0x7FFFFFFFu : ((static_cast<uint32_t>(1) << Bits) - 1u);

            /// Per-lane bit reservoirs. We maintain four independent accumulators
            /// (one per lane) and flush 32-bit words when enough bits are available.
            uint64_t acc0 = 0;
            uint64_t acc1 = 0;
            uint64_t acc2 = 0;
            uint64_t acc3 = 0;
            uint32_t acc_bits = 0;

            m128i * p = out;

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
                    p->u32[0] = static_cast<uint32_t>(acc0);
                    p->u32[1] = static_cast<uint32_t>(acc1);
                    p->u32[2] = static_cast<uint32_t>(acc2);
                    p->u32[3] = static_cast<uint32_t>(acc3);
                    ++p;

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
                const uint32_t v0 = (rem > 0 ? in[base + 0] : 0u) & mask;
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
                    p->u32[0] = uint32_t(acc0);
                    p->u32[1] = uint32_t(acc1);
                    p->u32[2] = uint32_t(acc2);
                    p->u32[3] = uint32_t(acc3);
                    ++p;

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
                p->u32[0] = static_cast<uint32_t>(acc0);
                p->u32[1] = static_cast<uint32_t>(acc1);
                p->u32[2] = static_cast<uint32_t>(acc2);
                p->u32[3] = static_cast<uint32_t>(acc3);
                ++p;
            }

            return p;
        }

        /// Unpack (decode) a tail segment (0 < tail < BLOCK_SIZE) from the SIMDComp-compatible
        /// horizontal 4-lane bitpacked stream.
        /// This is the counterpart of packTail<Bits>(). It decodes the last partial block
        /// when the total number of integers is not a multiple of 128.
        ///
        /// Special cases:
        /// - Bits == 0  : no payload;
        /// - Bits == 32 : values are stored as raw uint32_t, tightly packed (no 16-byte padding).
        ///
        /// Parameters:
        /// - in   : pointer to the compressed input stream (m128i words).
        /// - tail : number of integers to decode (0..127 typically).
        /// - out  : destination buffer (must have at least `tail` elements).
        ///
        /// Returns:
        /// - Pointer to the first m128i *after* the consumed input words.
        template<uint32_t Bits>
        [[maybe_unused]] static const m128i * unpackTail(const m128i * in, size_t tail, uint32_t * out) noexcept
        {
            static_assert(Bits <= 32, "Bits must be 0..32");
            if (tail == 0) return in;

            /// Bits==0: no stored bits;
            if constexpr (Bits == 0)
                return in;

            /// Bits==32: raw uint32_t values are stored tightly (SIMDComp's special-case behavior).
            if constexpr (Bits == 32)
            {
                std::memcpy(out, in, tail * sizeof(uint32_t));
                return reinterpret_cast<const m128i *>(reinterpret_cast<const uint32_t *>(in) + tail);
            }

            // Bits = 1..31
            const uint32_t mask = (Bits == 31) ? 0x7FFFFFFFu : ((uint32_t(1) << Bits) - 1u);

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

        const m128i * p = in;

        for (size_t g = 0; g < groups; ++g)
        {
            /// Refill accumulators if fewer than Bits bits remain.
            /// Reading one m128i contributes 32 bits to each lane.
            if (acc_bits < Bits)
            {
                acc0 |= (static_cast<uint64_t>(p->u32[0]) << acc_bits);
                acc1 |= (static_cast<uint64_t>(p->u32[1]) << acc_bits);
                acc2 |= (static_cast<uint64_t>(p->u32[2]) << acc_bits);
                acc3 |= (static_cast<uint64_t>(p->u32[3]) << acc_bits);
                ++p;
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
                if (rem > 0) out[base + 0] = v0;
                if (rem > 1) out[base + 1] = v1;
                if (rem > 2) out[base + 2] = v2;
                /// lane3 is omitted when rem < 4
            }
        }

        return p;
    }

    using packing_func = m128i * (*)(const uint32_t *, size_t, m128i *) noexcept;

    [[maybe_unused]] static packing_func getPackFixedFunc(uint32_t bits)
    {
        chassert(bits <= 32);

        static const packing_func table[33] = {
            &packFixed<0>,
            &packFixed<1>,
            &packFixed<2>,
            &packFixed<3>,
            &packFixed<4>,
            &packFixed<5>,
            &packFixed<6>,
            &packFixed<7>,
            &packFixed<8>,
            &packFixed<9>,
            &packFixed<10>,
            &packFixed<11>,
            &packFixed<12>,
            &packFixed<13>,
            &packFixed<14>,
            &packFixed<15>,
            &packFixed<16>,
            &packFixed<17>,
            &packFixed<18>,
            &packFixed<19>,
            &packFixed<20>,
            &packFixed<21>,
            &packFixed<22>,
            &packFixed<23>,
            &packFixed<24>,
            &packFixed<25>,
            &packFixed<26>,
            &packFixed<27>,
            &packFixed<28>,
            &packFixed<29>,
            &packFixed<30>,
            &packFixed<31>,
            &packFixed<32>,
        };
        return table[bits];
    }

    [[maybe_unused]] static packing_func getPackTailFunc(uint32_t bits)
    {
        chassert(bits <= 32);

        static const packing_func table[33] = {
            &packTail<0>,
            &packTail<1>,
            &packTail<2>,
            &packTail<3>,
            &packTail<4>,
            &packTail<5>,
            &packTail<6>,
            &packTail<7>,
            &packTail<8>,
            &packTail<9>,
            &packTail<10>,
            &packTail<11>,
            &packTail<12>,
            &packTail<13>,
            &packTail<14>,
            &packTail<15>,
            &packTail<16>,
            &packTail<17>,
            &packTail<18>,
            &packTail<19>,
            &packTail<20>,
            &packTail<21>,
            &packTail<22>,
            &packTail<23>,
            &packTail<24>,
            &packTail<25>,
            &packTail<26>,
            &packTail<27>,
            &packTail<28>,
            &packTail<29>,
            &packTail<30>,
            &packTail<31>,
            &packTail<32>,
        };
        return table[bits];
    }

    using unpack_func = const m128i* (*)(const m128i*, size_t, uint32_t*) noexcept;
    [[maybe_unused]] static unpack_func getUnpackFixedFunc(uint32_t bit)
    {
        chassert(bit <= 32);

        static const unpack_func table[33] = {
            &unpackFixed<0>,
            &unpackFixed<1>,
            &unpackFixed<2>,
            &unpackFixed<3>,
            &unpackFixed<4>,
            &unpackFixed<5>,
            &unpackFixed<6>,
            &unpackFixed<7>,
            &unpackFixed<8>,
            &unpackFixed<9>,
            &unpackFixed<10>,
            &unpackFixed<11>,
            &unpackFixed<12>,
            &unpackFixed<13>,
            &unpackFixed<14>,
            &unpackFixed<15>,
            &unpackFixed<16>,
            &unpackFixed<17>,
            &unpackFixed<18>,
            &unpackFixed<19>,
            &unpackFixed<20>,
            &unpackFixed<21>,
            &unpackFixed<22>,
            &unpackFixed<23>,
            &unpackFixed<24>,
            &unpackFixed<25>,
            &unpackFixed<26>,
            &unpackFixed<27>,
            &unpackFixed<28>,
            &unpackFixed<29>,
            &unpackFixed<30>,
            &unpackFixed<31>,
            &unpackFixed<32>,
        };
        return table[bit];
    }

    [[maybe_unused]] static unpack_func getUnpackTailFunc(uint32_t bit)
    {
        chassert(bit <= 32);

        static const unpack_func table[33] = {
            &unpackTail<0>,
            &unpackTail<1>,
            &unpackTail<2>,
            &unpackTail<3>,
            &unpackTail<4>,
            &unpackTail<5>,
            &unpackTail<6>,
            &unpackTail<7>,
            &unpackTail<8>,
            &unpackTail<9>,
            &unpackTail<10>,
            &unpackTail<11>,
            &unpackTail<12>,
            &unpackTail<13>,
            &unpackTail<14>,
            &unpackTail<15>,
            &unpackTail<16>,
            &unpackTail<17>,
            &unpackTail<18>,
            &unpackTail<19>,
            &unpackTail<20>,
            &unpackTail<21>,
            &unpackTail<22>,
            &unpackTail<23>,
            &unpackTail<24>,
            &unpackTail<25>,
            &unpackTail<26>,
            &unpackTail<27>,
            &unpackTail<28>,
            &unpackTail<29>,
            &unpackTail<30>,
            &unpackTail<31>,
            &unpackTail<32>,
        };
        return table[bit];
    }

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
    /// - out    : destination pointer to the compressed stream (m128i words).
    /// - bit    : bit-width per value (0..32).
    ///
    /// Returns:
    /// - Pointer to the first output position after the written compressed data.
    [[maybe_unused]] static m128i * packingLength(const uint32_t * in, size_t length, m128i * out, uint32_t bit) noexcept
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
    ///    the exact number of m128i words consumed for this bit width (BIT for 1..31,
    ///    32 for BIT==32, and 0 for BIT==0).
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
    [[maybe_unused]] static const m128i * unpackingLength(const m128i * in, size_t length, uint32_t * out, uint32_t bit) noexcept
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

using BlockCodec = impl::BlockCodecImpl<impl::has_simdcomp>;

}

