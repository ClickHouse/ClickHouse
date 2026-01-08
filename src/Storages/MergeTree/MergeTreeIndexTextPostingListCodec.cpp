#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <roaring/roaring.hh>

#include <config.h>

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
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

static constexpr size_t BLOCK_SIZE = 128;

namespace impl
{

template <bool has_simdcomp>
struct BlockCodecImpl;

#if USE_SIMDCOMP
static constexpr bool has_simdcomp = true;

tempate<>
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

    static std::size_t decode(std::span<const std::byte> & in, std::size_t n, uint32_t max_bits, std::span<uint32_t> & out)
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

/// On SSE-compatible platforms, we want to keep this class primarily for running unit tests.
/// These tests ensure that the fallback implementation is fully compatible with simdcomp.
/// On non-SSE platforms, this is the fallback path.
template<>
struct BlockCodecImpl<false>
{
    /// A portable stand-in for SSE's __m128i.
    /// We only rely on its *layout* (16 bytes total, 4 lanes of 32-bit words),
    /// because SIMDComp stores packed data as an array of 128-bit "words".
    /// This type makes the on-disk / on-wire format identical on non-SSE platforms.
    struct alignas(16) m128i { std::uint32_t u32[4]; };

    /// Ensure the type is exactly 128 bits. If this ever changes, the packed format
    /// (pointer arithmetic, byte counts, and decode correctness) would break.
    static_assert(sizeof(m128i) == 16, "m128i must be 16 bytes");

    /// Non-SSE version: equivalent to SIMDComp maxbits_length.
    /// It OR-reduces all values to compute required bit width.
    [[maybe_unused]] static uint32_t maxbitsLength(const std::span<uint32_t> & in) noexcept
    {
        size_t n = in.size();
        uint32_t bigxor = 0;
        // Process in chunks of 4 (mirrors the SIMD path grouping), but without SSE.
        const uint32_t offset = (n / 4) * 4;

        for (uint32_t k = 0; k < offset; k += 4)
        {
            bigxor |= in[k + 0];
            bigxor |= in[k + 1];
            bigxor |= in[k + 2];
            bigxor |= in[k + 3];
        }

        // Tail.
        for (uint32_t k = offset; k < n; ++k)
        {
            bigxor |= in[k];
        }

        // bits(bigxor): 0 -> 0, else 32 - clz(bigxor)
        if (bigxor == 0) return 0u;
        return 32u - static_cast<uint32_t>(__builtin_clz(bigxor));
    }

    [[maybe_unused]] static size_t bitpackingCompressedBytes(int length, uint32_t bit) noexcept
    {
        if (bit == 0) return 0;
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
    [[maybe_unused]] static std::size_t decode(std::span<const std::byte> & in, std::size_t n, uint32_t max_bits, std::span<uint32_t> & out)
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
    /// - BIT == 0  : nothing is written.
    /// - BIT == 32 : values are copied as raw uint32_t (no bitpacking), still grouped as 4 words per m128i.
    ///
    /// Parameters:
    /// - in     : pointer to input uint32_t values (must have at least groups*4 elements).
    /// - groups : number of 4-value groups to pack.
    /// - out    : output pointer to m128i words.
    ///
    /// Returns:
    /// - Pointer to the first m128i *after* the written output.
    template<uint32_t BIT>
    [[maybe_unused]] static m128i * packingFixed(const uint32_t * in, size_t groups, m128i * out) noexcept
    {
        static_assert(BIT <= 32, "BIT must be 0..32");
        if (groups == 0) return out;

        if constexpr (BIT == 0)
            return out;

        /// BIT==32: store raw 32-bit words (4 per m128i). This matches SIMDComp's
        /// special-case behavior for full blocks.
        if constexpr (BIT == 32)
        {
            std::memcpy(out, in, groups * 4 * sizeof(uint32_t));
            return out + groups;
        }

        /// Mask to keep only the lowest BIT bits (BIT in 1..31 here).
        const uint32_t mask = (BIT == 31) ? 0x7FFFFFFFu : ((static_cast<uint32_t>(1) << BIT) - 1u);

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
            /// Load one group (4 lanes) and keep only BIT bits.
            const uint32_t v0 = in[4 * i + 0] & mask;
            const uint32_t v1 = in[4 * i + 1] & mask;
            const uint32_t v2 = in[4 * i + 2] & mask;
            const uint32_t v3 = in[4 * i + 3] & mask;

            /// Append this group's bits at the current bit offset.
            acc0 |= (static_cast<uint64_t>(v0) << acc_bits);
            acc1 |= (static_cast<uint64_t>(v1) << acc_bits);
            acc2 |= (static_cast<uint64_t>(v2) << acc_bits);
            acc3 |= (static_cast<uint64_t>(v3) << acc_bits);
            acc_bits += BIT;

            /// When we've accumulated at least 32 bits, flush one 32-bit word per lane
            /// into one output m128i.
            if (acc_bits >= 32)
            {
                p->u32[0] = static_cast<uint32_t>(acc0);
                p->u32[1] = static_cast<uint32_t>(acc1);
                p->u32[2] = static_cast<uint32_t>(acc2);
                p->u32[3] = static_cast<uint32_t>(acc3);
                ++p;

                /// Keep leftover bits (at most 31 bits remain because BIT<=31).
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
    /// Behavior by BIT:
    /// - BIT == 0  : no bits were stored; values are implicit zeros (no input consumed).
    /// - BIT == 32 : values are stored as raw uint32_t; copy directly and advance by groups*4 words.
    /// - BIT 1..31 : use four 64-bit lane accumulators (acc0..acc3) as bit reservoirs.
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
    template<uint32_t BIT>
    [[maybe_unused]] static const m128i * unpackingFixed(const m128i *in, size_t groups, uint32_t *out) noexcept
    {
        static_assert(BIT <= 32, "BIT must be 0..32");
        if (groups == 0) return in;

        /// BIT==0: no payload in the stream;
        if constexpr (BIT == 0)
            return in;

        /// BIT==32: stream stores raw uint32_t values (4 per group / per m128i).
        if constexpr (BIT == 32)
        {
            std::memcpy(out, in, groups * 4 * sizeof(uint32_t));
            return reinterpret_cast<const m128i *>(reinterpret_cast<const uint32_t *>(in) + groups * 4);
        }

        /// BIT=1..31: mask to keep only the lowest BIT bits from each extracted value.
        const uint32_t mask = (BIT == 31) ? 0x7FFFFFFFu : ((static_cast<uint32_t>(1) << BIT) - 1u);

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
            /// Refill reservoirs if there aren't enough bits to extract one BIT-bit value.
            /// Reading one m128i adds 32 bits per lane.
            if (acc_bits < BIT)
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

            /// Consume BIT bits from each lane reservoir.
            acc0 >>= BIT;
            acc1 >>= BIT;
            acc2 >>= BIT;
            acc3 >>= BIT;
            acc_bits -= BIT;

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
    template<uint32_t BIT>
    [[maybe_unused]] static m128i * packingTail(const uint32_t * in, size_t tail, m128i * out) noexcept
    {
        static_assert(BIT <= 32, "BIT must be 0..32");
        if (tail == 0) return out;

        /// BIT==0: no payload is written;
        if constexpr (BIT == 0)
            return out;

        /// BIT==32: store raw 32-bit words tightly (SIMDComp's special-case behavior).
        if constexpr (BIT == 32)
        {
            auto *out32 = reinterpret_cast<uint32_t *>(out);
            std::memcpy(out32, in, tail * sizeof(uint32_t));
            out32 += tail;
            return reinterpret_cast<m128i *>(out32);
        }

        /// BIT=1..31: mask keeps only the lowest BIT bits of each input value.
        const uint32_t mask = (BIT == 31) ? 0x7FFFFFFFu : ((static_cast<uint32_t>(1) << BIT) - 1u);

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
            /// Load one group (4 lanes), keep only BIT bits.
            const uint32_t v0 = in[4 * i + 0] & mask;
            const uint32_t v1 = in[4 * i + 1] & mask;
            const uint32_t v2 = in[4 * i + 2] & mask;
            const uint32_t v3 = in[4 * i + 3] & mask;

            /// Append this group's BIT bits to each lane's accumulator at the current offset.
            acc0 |= (static_cast<uint64_t>(v0) << acc_bits);
            acc1 |= (static_cast<uint64_t>(v1) << acc_bits);
            acc2 |= (static_cast<uint64_t>(v2) << acc_bits);
            acc3 |= (static_cast<uint64_t>(v3) << acc_bits);
            acc_bits += BIT;

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
            acc_bits += BIT;

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
    /// This is the counterpart of packingTail<BIT>(). It decodes the last partial block
    /// when the total number of integers is not a multiple of 128.
    ///
    /// Special cases:
    /// - BIT == 0  : no payload;
    /// - BIT == 32 : values are stored as raw uint32_t, tightly packed (no 16-byte padding).
    ///
    /// Parameters:
    /// - in   : pointer to the compressed input stream (m128i words).
    /// - tail : number of integers to decode (0..127 typically).
    /// - out  : destination buffer (must have at least `tail` elements).
    ///
    /// Returns:
    /// - Pointer to the first m128i *after* the consumed input words.
    template<uint32_t BIT>
    [[maybe_unused]] static const m128i * unpackingTail(const m128i * in, size_t tail, uint32_t * out) noexcept
    {
        static_assert(BIT <= 32, "BIT must be 0..32");
        if (tail == 0) return in;

        /// BIT==0: no stored bits;
        if constexpr (BIT == 0)
            return in;

        /// BIT==32: raw uint32_t values are stored tightly (SIMDComp's special-case behavior).
        if constexpr (BIT == 32)
        {
            std::memcpy(out, in, tail * sizeof(uint32_t));
            return reinterpret_cast<const m128i *>(reinterpret_cast<const uint32_t *>(in) + tail);
        }

        // BIT = 1..31
        const uint32_t mask = (BIT == 31) ? 0x7FFFFFFFu : ((uint32_t(1) << BIT) - 1u);

        /// Number of 4-lane groups that were packed for this tail:
        /// packing pads the last incomplete group (if any) with zeros, so we must decode
        /// ceil(tail/4) groups from the stream.
        const size_t groups = (tail + 3) / 4;
        const size_t full_groups = tail / 4;
        const size_t rem = tail % 4;

        /// Per-lane bit reservoirs (accumulators). We refill by reading one m128i (32 bits per lane)
        /// whenever we don't have enough bits to extract one BIT-bit value.
        uint64_t acc0 = 0;
        uint64_t acc1 = 0;
        uint64_t acc2 = 0;
        uint64_t acc3 = 0;
        uint32_t acc_bits = 0;

        const m128i * p = in;

        for (size_t g = 0; g < groups; ++g)
        {
            /// Refill accumulators if fewer than BIT bits remain.
            /// Reading one m128i contributes 32 bits to each lane.
            if (acc_bits < BIT)
            {
                acc0 |= (static_cast<uint64_t>(p->u32[0]) << acc_bits);
                acc1 |= (static_cast<uint64_t>(p->u32[1]) << acc_bits);
                acc2 |= (static_cast<uint64_t>(p->u32[2]) << acc_bits);
                acc3 |= (static_cast<uint64_t>(p->u32[3]) << acc_bits);
                ++p;
                acc_bits += 32;
            }

            /// Extract one value per lane from the low BIT bits (LSB-first).
            const uint32_t v0 = static_cast<uint32_t>(acc0) & mask;
            const uint32_t v1 = static_cast<uint32_t>(acc1) & mask;
            const uint32_t v2 = static_cast<uint32_t>(acc2) & mask;
            const uint32_t v3 = static_cast<uint32_t>(acc3) & mask;

            /// Consume BIT bits from each lane accumulator.
            acc0 >>= BIT;
            acc1 >>= BIT;
            acc2 >>= BIT;
            acc3 >>= BIT;
            acc_bits -= BIT;

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

    using packing_func = m128i* (*)(const uint32_t*, size_t, m128i*) noexcept;
    [[maybe_unused]] static packing_func packingFixedFunctions(uint32_t bit)
    {
        /// The caller guarantees `bit <= 32`.
        chassert(bit <= 32);
        static const packing_func table[33] = {
            /*0 */ &packingFixed<0>,
            /*1 */ &packingFixed<1>,
            /*2 */ &packingFixed<2>,
            /*3 */ &packingFixed<3>,
            /*4 */ &packingFixed<4>,
            /*5 */ &packingFixed<5>,
            /*6 */ &packingFixed<6>,
            /*7 */ &packingFixed<7>,
            /*8 */ &packingFixed<8>,
            /*9 */ &packingFixed<9>,
            /*10*/ &packingFixed<10>,
            /*11*/ &packingFixed<11>,
            /*12*/ &packingFixed<12>,
            /*13*/ &packingFixed<13>,
            /*14*/ &packingFixed<14>,
            /*15*/ &packingFixed<15>,
            /*16*/ &packingFixed<16>,
            /*17*/ &packingFixed<17>,
            /*18*/ &packingFixed<18>,
            /*19*/ &packingFixed<19>,
            /*20*/ &packingFixed<20>,
            /*21*/ &packingFixed<21>,
            /*22*/ &packingFixed<22>,
            /*23*/ &packingFixed<23>,
            /*24*/ &packingFixed<24>,
            /*25*/ &packingFixed<25>,
            /*26*/ &packingFixed<26>,
            /*27*/ &packingFixed<27>,
            /*28*/ &packingFixed<28>,
            /*29*/ &packingFixed<29>,
            /*30*/ &packingFixed<30>,
            /*31*/ &packingFixed<31>,
            /*32*/ &packingFixed<32>,
        };
        return table[bit];
    }

    [[maybe_unused]] static packing_func packingTailFunctions(uint32_t bit)
    {
        /// The caller guarantees `bit <= 32`.
        chassert(bit <= 32);
        static const packing_func table[33] = {
            /*0 */ &packingTail<0>,
            /*1 */ &packingTail<1>,
            /*2 */ &packingTail<2>,
            /*3 */ &packingTail<3>,
            /*4 */ &packingTail<4>,
            /*5 */ &packingTail<5>,
            /*6 */ &packingTail<6>,
            /*7 */ &packingTail<7>,
            /*8 */ &packingTail<8>,
            /*9 */ &packingTail<9>,
            /*10*/ &packingTail<10>,
            /*11*/ &packingTail<11>,
            /*12*/ &packingTail<12>,
            /*13*/ &packingTail<13>,
            /*14*/ &packingTail<14>,
            /*15*/ &packingTail<15>,
            /*16*/ &packingTail<16>,
            /*17*/ &packingTail<17>,
            /*18*/ &packingTail<18>,
            /*19*/ &packingTail<19>,
            /*20*/ &packingTail<20>,
            /*21*/ &packingTail<21>,
            /*22*/ &packingTail<22>,
            /*23*/ &packingTail<23>,
            /*24*/ &packingTail<24>,
            /*25*/ &packingTail<25>,
            /*26*/ &packingTail<26>,
            /*27*/ &packingTail<27>,
            /*28*/ &packingTail<28>,
            /*29*/ &packingTail<29>,
            /*30*/ &packingTail<30>,
            /*31*/ &packingTail<31>,
            /*32*/ &packingTail<32>,
        };
        return table[bit];
    }

    using unpacking_func = const m128i* (*)(const m128i*, size_t, uint32_t*) noexcept;
    [[maybe_unused]] static unpacking_func unpackingFixedFunctions(uint32_t bit)
    {
        /// The caller guarantees `bit <= 32`.
        chassert(bit <= 32);
        static const unpacking_func table[33] = {
            /*0 */ &unpackingFixed<0>,
            /*1 */ &unpackingFixed<1>,
            /*2 */ &unpackingFixed<2>,
            /*3 */ &unpackingFixed<3>,
            /*4 */ &unpackingFixed<4>,
            /*5 */ &unpackingFixed<5>,
            /*6 */ &unpackingFixed<6>,
            /*7 */ &unpackingFixed<7>,
            /*8 */ &unpackingFixed<8>,
            /*9 */ &unpackingFixed<9>,
            /*10*/ &unpackingFixed<10>,
            /*11*/ &unpackingFixed<11>,
            /*12*/ &unpackingFixed<12>,
            /*13*/ &unpackingFixed<13>,
            /*14*/ &unpackingFixed<14>,
            /*15*/ &unpackingFixed<15>,
            /*16*/ &unpackingFixed<16>,
            /*17*/ &unpackingFixed<17>,
            /*18*/ &unpackingFixed<18>,
            /*19*/ &unpackingFixed<19>,
            /*20*/ &unpackingFixed<20>,
            /*21*/ &unpackingFixed<21>,
            /*22*/ &unpackingFixed<22>,
            /*23*/ &unpackingFixed<23>,
            /*24*/ &unpackingFixed<24>,
            /*25*/ &unpackingFixed<25>,
            /*26*/ &unpackingFixed<26>,
            /*27*/ &unpackingFixed<27>,
            /*28*/ &unpackingFixed<28>,
            /*29*/ &unpackingFixed<29>,
            /*30*/ &unpackingFixed<30>,
            /*31*/ &unpackingFixed<31>,
            /*32*/ &unpackingFixed<32>,
        };
        return table[bit];
    }

    [[maybe_unused]] static unpacking_func unpackingTailFunctions(uint32_t bit)
    {
        /// The caller guarantees `bit <= 32`.
        chassert(bit <= 32);
        static const unpacking_func table[33] = {
            /*0 */ &unpackingTail<0>,
            /*1 */ &unpackingTail<1>,
            /*2 */ &unpackingTail<2>,
            /*3 */ &unpackingTail<3>,
            /*4 */ &unpackingTail<4>,
            /*5 */ &unpackingTail<5>,
            /*6 */ &unpackingTail<6>,
            /*7 */ &unpackingTail<7>,
            /*8 */ &unpackingTail<8>,
            /*9 */ &unpackingTail<9>,
            /*10*/ &unpackingTail<10>,
            /*11*/ &unpackingTail<11>,
            /*12*/ &unpackingTail<12>,
            /*13*/ &unpackingTail<13>,
            /*14*/ &unpackingTail<14>,
            /*15*/ &unpackingTail<15>,
            /*16*/ &unpackingTail<16>,
            /*17*/ &unpackingTail<17>,
            /*18*/ &unpackingTail<18>,
            /*19*/ &unpackingTail<19>,
            /*20*/ &unpackingTail<20>,
            /*21*/ &unpackingTail<21>,
            /*22*/ &unpackingTail<22>,
            /*23*/ &unpackingTail<23>,
            /*24*/ &unpackingTail<24>,
            /*25*/ &unpackingTail<25>,
            /*26*/ &unpackingTail<26>,
            /*27*/ &unpackingTail<27>,
            /*28*/ &unpackingTail<28>,
            /*29*/ &unpackingTail<29>,
            /*30*/ &unpackingTail<30>,
            /*31*/ &unpackingTail<31>,
            /*32*/ &unpackingTail<32>,
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
    [[maybe_unused]] static m128i * packingLength(const std::uint32_t * in, std::size_t length, m128i * out, std::uint32_t bit) noexcept
    {
        /// Select the fixed-block packer for this bit width.
        auto func = packingFixedFunctions(bit);

        /// Process all complete blocks. Each block has 32 groups (128 ints / 4).
        size_t blocks = length / BLOCK_SIZE;
        for (std::size_t i = 0; i < blocks; ++i)
        {
            out = func(in, 32, out);
            in += BLOCK_SIZE;
        }

        /// Pack the remaining tail (0..BLOCK_SIZE-1 ints), if any.
        const size_t tail = length % BLOCK_SIZE;
        if (tail == 0)
            return out;

        /// Select the tail packer (short-length path) for this bit width.
        func = packingTailFunctions(bit);
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
        auto func = unpackingFixedFunctions(bit);

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
        func = unpackingTailFunctions(bit);
        return func(in, tail, out);
    }
};

using BlockCodec = BlockCodecImpl<has_simdcomp>;

}


/// Normalize the requested block size to a multiple of BLOCK_SIZE.
/// We encode/decode posting lists in fixed-size blocks, and the SIMD bit-packing
/// implementation expects block-aligned sizes for efficient processing.
PostingListCodecImpl::PostingListCodecImpl(size_t postings_list_block_size)
    : posting_list_block_size((postings_list_block_size + BLOCK_SIZE - 1) & ~(BLOCK_SIZE - 1))
{
    compressed_data.reserve(BLOCK_SIZE);
    current_segment.reserve(BLOCK_SIZE);
}

void PostingListCodecImpl::insert(uint32_t row)
{
    if (rows_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_begin = row;
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        prev_row = row;
        current_segment.emplace_back(row - prev_row);
        ++rows_in_current_segment;
        ++total_rows;
        return;
    }

    current_segment.emplace_back(row - prev_row);
    prev_row = row;
    ++rows_in_current_segment;
    ++total_rows;

    if (current_segment.size() == BLOCK_SIZE)
        compressBlock(current_segment);

    if (rows_in_current_segment == posting_list_block_size)
        flushCurrentSegment();
}

void PostingListCodecImpl::insert(std::span<uint32_t> rows)
{
    chassert(rows.size() == BLOCK_SIZE && rows_in_current_segment % BLOCK_SIZE == 0);

    if (rows_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_begin = rows.front();
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        prev_row = rows.front();
        rows_in_current_segment += BLOCK_SIZE;
        total_rows += BLOCK_SIZE;
    }

    auto last_row = rows.back();
    std::adjacent_difference(rows.begin(), rows.end(), rows.begin());
    rows[0] -= prev_row;
    prev_row = last_row;

    compressBlock(rows);

    if (rows_in_current_segment == posting_list_block_size)
        flushCurrentSegment();
}

void PostingListCodecImpl::decode(ReadBuffer & in, PostingList & postings)
{
    Header header;
    header.read(in);
    if (header.codec_type != static_cast<uint8_t>(codec_type))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected codec type {}, but got {}", codec_type, header.codec_type);

    prev_row = header.first_row_id;

    uint32_t tail_block_size = header.cardinality % BLOCK_SIZE;
    uint32_t full_block_count = header.cardinality / BLOCK_SIZE;

    current_segment.reserve(BLOCK_SIZE);
    if (header.payload_bytes > (compressed_data.capacity() - compressed_data.size()))
        compressed_data.reserve(compressed_data.size() + header.payload_bytes);
    compressed_data.resize(header.payload_bytes);
    in.readStrict(compressed_data.data(), header.payload_bytes);

    //auto * p = reinterpret_cast<unsigned char *> (compressed_data.data());
    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte*>(compressed_data.data()), compressed_data.size());
    for (uint32_t i = 0; i < full_block_count; i++)
    {
        decodeOneBlock(compressed_data_span, BLOCK_SIZE, prev_row, current_segment);
        postings.addMany(current_segment.size(), current_segment.data());
    }
    if (tail_block_size)
    {
        decodeOneBlock(compressed_data_span, tail_block_size, prev_row, current_segment);
        postings.addMany(current_segment.size(), current_segment.data());
    }
}

void PostingListCodecImpl::serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const
{
    info.offsets.reserve(segment_descriptors.size());
    info.ranges.reserve(segment_descriptors.size());

    for (const auto & descriptor : segment_descriptors)
    {
        info.offsets.emplace_back(out.count());
        info.ranges.emplace_back(descriptor.row_begin, descriptor.row_end);
        Header header(static_cast<uint8_t>(codec_type), descriptor.compressed_data_size, descriptor.cardinality, descriptor.row_begin);
        header.write(out);
        out.write(compressed_data.data() + descriptor.compressed_data_offset, descriptor.compressed_data_size);
    }
}

namespace
{
void encodeU8(uint8_t x, std::span<char> & out)
{
    out[0] = static_cast<char>(x);
    out = out.subspan(1);
}

uint8_t decodeU8(std::span<const std::byte> & in)
{
    auto v = static_cast<uint8_t>(in[0]);
    in = in.subspan(1);
    return v;
}
}

void PostingListCodecImpl::compressBlock(std::span<uint32_t> segment)
{
    auto & last_segment = segment_descriptors.back();
    last_segment.cardinality += segment.size();
    last_segment.row_end = prev_row;

    auto [needed_bytes, max_bits] = impl::BlockCodec::calculateNeededBytesAndMaxBits(segment);
    size_t memory_gap = compressed_data.capacity() - compressed_data.size();
    size_t need_bytes = needed_bytes + 1;
    if (memory_gap < need_bytes)
    {
        auto min_need = need_bytes - memory_gap;
        compressed_data.reserve(compressed_data.size() + 2 * min_need);
    }
    /// Block Layout: [1byte(max_bits)][payload]
    size_t offset = compressed_data.size();
    compressed_data.resize(compressed_data.size() + need_bytes);
    std::span<char> compressed_data_span(compressed_data.data() + offset, need_bytes);
    encodeU8(max_bits, compressed_data_span);
    auto used = impl::BlockCodec::encode(segment, max_bits, compressed_data_span);
    chassert(used == needed_bytes && compressed_data_span.empty());

    last_segment.compressed_data_size = compressed_data.size() - last_segment.compressed_data_offset;
    current_segment.clear();
}

void PostingListCodecImpl::decodeOneBlock(
        std::span<const std::byte> & in, size_t count, uint32_t & prev_row, std::vector<uint32_t> & current_segment)
{
    if (in.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected at least {} bytes, but got {}", 1, in.size());

    uint8_t bits = decodeU8(in);
    current_segment.resize(count);

    /// Decode postings to buffer named temp.
    std::span<uint32_t> current_span(current_segment.data(), current_segment.size());
    impl::BlockCodec::decode(in, count, bits, current_span);

    /// Restore the original array from the decompressed delta values.
    std::inclusive_scan(current_segment.begin(), current_segment.end(), current_segment.begin(), std::plus<uint32_t>{}, prev_row);
    prev_row = current_segment.empty() ? prev_row : current_segment.back();
}

void PostingListCodecSIMDComp::decode(ReadBuffer & in, PostingList & postings) const
{
    PostingListCodecImpl impl;
    impl.decode(in, postings);
}

void PostingListCodecSIMDComp::encode(
        const PostingListBuilder & postings, size_t posting_list_block_size, TokenPostingsInfo & info, WriteBuffer & out) const
{
    PostingListCodecImpl impl(posting_list_block_size);

    if (postings.isSmall())
    {
        const auto & small_postings = postings.getSmall();
        size_t num_postings = postings.size();
        for (size_t i = 0; i < num_postings; ++i)
            impl.insert(small_postings[i]);
    }
    else
    {
        std::vector<uint32_t> rowids;
        rowids.resize(postings.size());
        const auto & large_postings = postings.getLarge();
        large_postings.toUint32Array(rowids.data());
        std::span<uint32_t> rowids_view(rowids.data(), rowids.size());

        while (rowids_view.size() >= BLOCK_SIZE)
        {
            auto front = rowids_view.first(BLOCK_SIZE);
            impl.insert(front);
            rowids_view = rowids_view.subspan(BLOCK_SIZE);
        }

        if (!rowids_view.empty())
        {
            for (auto rowid : rowids_view)
                impl.insert(rowid);
        }
    }

    impl.encode(out, info);
}
}

