#include <Storages/MergeTree/FastPFORBlockCodec.h>

#include <config.h>
#include <Common/Exception.h>

#include <algorithm>
#include <cstring>

#if USE_FASTPFOR
#include <fastpfor/simdfastpfor.h>
#include <fastpfor/variablebyte.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

#if USE_FASTPFOR

namespace
{
    /// Must match BitpackingBlockCodec's BLOCK_SIZE; kept local to avoid pulling the SIMD-only header in.
    constexpr size_t BLOCK_SIZE = 128;
    static_assert(FastPFORBlockCodec::maxBlockBytes() == 16 * BLOCK_SIZE + 256);

    /// Output scratch for one encoded block, in 32-bit words. Comfortably above the worst-case encoding of
    /// a single 128-value block (one SIMDFastPFor page or one VariableByte tail), so FastPFOR never reports
    /// a buffer overrun.
    constexpr size_t ENCODE_SCRATCH_WORDS = 4 * BLOCK_SIZE + 128;
    /// Input scratch for decode: at most maxBlockBytes() of payload plus headroom for the 16-byte SIMD
    /// over-read, kept zeroed so a corrupted block cannot make the decoder read stale or out-of-bounds data.
    constexpr size_t DECODE_IN_WORDS = FastPFORBlockCodec::maxBlockBytes() / sizeof(uint32_t) + 8;

    /// Bounded FastPFOR VariableByte tail decoder. The library's VariableByte::decodeArray decodes every
    /// terminated value in its input span and only sets nvalue afterwards, so on corrupted input it can
    /// write past the fixed 128-slot block buffer before any count check runs. This bounded variant stops
    /// with CORRUPTED_DATA as soon as it would produce more than `max_values`, and rejects runaway varints,
    /// so a corrupted tail can never overrun the output. FastPFOR marks the final byte of each value with
    /// the high bit (0x80); continuation bytes carry 7 low bits. Trailing non-terminated bytes (the 32-bit
    /// padding VariableByte appends) are consumed without producing a value. Returns the produced count.
    size_t decodeVariableByteTailBounded(const uint8_t * p, const uint8_t * end, uint32_t * out, size_t max_values)
    {
        size_t produced = 0;
        while (p < end)
        {
            uint64_t value = 0;
            uint32_t shift = 0;
            bool terminated = false;
            while (p < end)
            {
                const uint8_t c = *p++;
                value |= static_cast<uint64_t>(c & 0x7F) << shift;
                if (c & 0x80)
                {
                    terminated = true;
                    break;
                }
                shift += 7;
                /// A uint32 value needs at most five 7-bit groups; a sixth continuation byte is corruption.
                if (shift > 28)
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "Corrupted FastPFOR posting block: malformed VariableByte tail value");
            }
            if (!terminated)
                break;
            if (produced == max_values)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Corrupted FastPFOR posting block: VariableByte tail produced more than the expected {} values", max_values);
            out[produced++] = static_cast<uint32_t>(value);
        }
        return produced;
    }

    /// Validate one SIMDFastPFor<4> single-block page (PageSize == BlockSize, so exactly one block) before
    /// handing the buffer to the library decoder. SIMDFastPFor::decodeArray / __decodeArray follow on-disk
    /// offsets (`wheremeta`, the byte-container size, the exception bitmap and arrays) with no bounds checks
    /// in release builds, so a corrupted .pst page can read past `decode_in` or write past `decode_out`.
    /// This mirrors that pointer walk and rejects anything that would step outside [base, base + words) or
    /// out of the 128-slot output block, so corrupted indexes raise CORRUPTED_DATA instead of reading OOB.
    /// Page layout (words): [count][wheremeta = 1 + 4*b][4*b packed words][bytesize][byte container]
    /// [bitmap][exception arrays].
    void validateSimdSingleBlockPage(const uint32_t * base, size_t words)
    {
        auto corrupt = [](const char * what)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted FastPFOR posting block: {}", what);
        };

        if (words < 2)
            corrupt("page shorter than its header");

        /// decodeArray reads the value count from word 0; a full block always encodes exactly BLOCK_SIZE.
        if (base[0] != BLOCK_SIZE)
            corrupt("full block does not encode 128 values");

        const uint32_t wheremeta = base[1];
        if (wheremeta < 1 || (wheremeta - 1) % 4 != 0)
            corrupt("invalid wheremeta offset");
        const uint32_t b = (wheremeta - 1) / 4;
        if (b > 32)
            corrupt("block bit width exceeds 32");

        /// The byte-container size word sits at base[1 + wheremeta]; the packed region base[2 .. 1 + wheremeta)
        /// is implicitly bounded by it.
        const size_t bytesize_idx = static_cast<size_t>(1) + wheremeta;
        if (bytesize_idx >= words)
            corrupt("byte-container size word out of bounds");
        const uint32_t bytesize = base[bytesize_idx];

        const uint8_t * bc = reinterpret_cast<const uint8_t *>(base + bytesize_idx + 1);
        const size_t bc_words = (static_cast<size_t>(bytesize) + sizeof(uint32_t) - 1) / sizeof(uint32_t);
        const size_t bitmap_idx = bytesize_idx + 1 + bc_words;
        if (bitmap_idx >= words)
            corrupt("exception bitmap word out of bounds");

        if (bytesize < 2)
            corrupt("byte container too small");
        const uint8_t bc_b = bc[0];
        const uint8_t cexcept = bc[1];
        if (bc_b != b)
            corrupt("byte-container bit width disagrees with wheremeta");

        uint8_t maxbits = 0;
        if (cexcept > 0)
        {
            /// byte container is [b][cexcept][maxbits][cexcept position bytes].
            if (bytesize != static_cast<uint32_t>(3) + cexcept)
                corrupt("byte-container size disagrees with exception count");
            maxbits = bc[2];
            if (maxbits <= b || maxbits > 32)
                corrupt("invalid exception max bit width");
            /// Exception positions index the 128-slot output block directly (out[pos]).
            for (uint32_t k = 0; k < cexcept; ++k)
                if (bc[3 + k] >= BLOCK_SIZE)
                    corrupt("exception position out of block range");
        }
        else if (bytesize != 2)
        {
            corrupt("byte-container size disagrees with exception count");
        }

        /// Only k == maxbits - b is serialized, and only when it is >= 2 (the == 1 case stores the implicit
        /// value 1 and emits no array). The bitmap must match that exactly.
        const uint32_t bitmap = base[bitmap_idx];
        uint32_t expected_bitmap = 0;
        if (cexcept > 0 && static_cast<uint32_t>(maxbits - b) >= 2)
            expected_bitmap = 1u << ((maxbits - b) - 1);
        if (bitmap != expected_bitmap)
            corrupt("unexpected exception bitmap");

        if (expected_bitmap != 0)
        {
            const uint32_t k = static_cast<uint32_t>(maxbits - b);
            const size_t size_idx = bitmap_idx + 1;
            if (size_idx >= words)
                corrupt("exception array size word out of bounds");
            const uint32_t exc_size = base[size_idx];
            if (exc_size != cexcept)
                corrupt("exception array size disagrees with exception count");
            /// unpackmesimd consumes one size word plus ceil(size * k / 32) packed words.
            const size_t exc_words = static_cast<size_t>(1) + (static_cast<size_t>(exc_size) * k + 31) / 32;
            if (size_idx + exc_words > words)
                corrupt("exception array out of bounds");
        }
    }
}

struct FastPFORBlockCodec::Impl
{
    /// `CompositeCodec<SIMDFastPFor<4>, VariableByte>` is replicated inline (rather than used directly) so
    /// the inner `SIMDFastPFor` can be built with a small page size (one block), shrinking its internal
    /// `bytescontainer` from ~64 KB to ~130 bytes — important because one codec instance is held per cursor.
    FastPForLib::SIMDFastPFor<4> simd{BLOCK_SIZE};
    FastPForLib::VariableByte vbyte;

    alignas(16) uint32_t encode_scratch[ENCODE_SCRATCH_WORDS] = {};
    alignas(16) uint32_t decode_in[DECODE_IN_WORDS] = {};
    /// SIMDFastPFor writes decode output with aligned stores, so this must be 16-byte aligned.
    alignas(16) uint32_t decode_out[BLOCK_SIZE] = {};
};

FastPFORBlockCodec::FastPFORBlockCodec() : impl(std::make_unique<Impl>()) {}
FastPFORBlockCodec::~FastPFORBlockCodec() = default;

size_t FastPFORBlockCodec::encode(std::span<const uint32_t> in, std::span<char> & out)
{
    const size_t count = in.size();
    if (count == 0 || count > BLOCK_SIZE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FastPFOR block encode expects 1..{} values, got {}", BLOCK_SIZE, count);

    /// Mirror CompositeCodec<SIMDFastPFor<4>, VariableByte>::encodeArray: the SIMD codec compresses the
    /// 128-aligned prefix (a whole block or nothing), VariableByte the (<128) remainder. The SIMD codec is
    /// always invoked — for a pure tail it still writes a one-word zero-length header that decode relies on.
    const size_t rounded = count / BLOCK_SIZE * BLOCK_SIZE;

    size_t produced_words = ENCODE_SCRATCH_WORDS;
    impl->simd.encodeArray(in.data(), rounded, impl->encode_scratch, produced_words);

    if (rounded < count)
    {
        size_t tail_words = ENCODE_SCRATCH_WORDS - produced_words;
        impl->vbyte.encodeArray(in.data() + rounded, count - rounded, impl->encode_scratch + produced_words, tail_words);
        produced_words += tail_words;
    }

    const size_t bytes = produced_words * sizeof(uint32_t);
    if (bytes > maxBlockBytes())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "FastPFOR block encode produced {} bytes for {} values, exceeding the per-block cap {}",
            bytes, count, maxBlockBytes());
    if (bytes > out.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "FastPFOR block encode needs {} bytes but only {} remain in the output buffer", bytes, out.size());

    std::memcpy(out.data(), impl->encode_scratch, bytes);
    out = out.subspan(bytes);
    return bytes;
}

size_t FastPFORBlockCodec::decode(std::span<const std::byte> & in, size_t count, std::span<uint32_t> out)
{
    if (count == 0 || count > BLOCK_SIZE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FastPFOR block decode expects 1..{} values, got {}", BLOCK_SIZE, count);
    if (out.size() < count)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FastPFOR block decode output holds {} slots but {} requested", out.size(), count);

    const bool is_full_block = (count == BLOCK_SIZE);
    /// A full block self-delimits via the SIMD page, so an over-long input is fine and we cap the copy.
    /// A tail block ends with the length-driven VariableByte codec, so the caller must pass its exact span.
    const size_t copy_bytes = is_full_block ? std::min(in.size(), maxBlockBytes()) : in.size();

    if (copy_bytes < sizeof(uint32_t))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted FastPFOR posting block: only {} bytes available", copy_bytes);
    if (copy_bytes % sizeof(uint32_t) != 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted FastPFOR posting block: byte length {} is not a multiple of 4", copy_bytes);
    if (copy_bytes > maxBlockBytes())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted FastPFOR posting block: byte length {} exceeds the per-block cap {}", copy_bytes, maxBlockBytes());

    const size_t copy_words = copy_bytes / sizeof(uint32_t);

    /// Copy into the owned, zero-padded, aligned scratch so the SIMD over-read can only touch our memory.
    std::memcpy(impl->decode_in, in.data(), copy_bytes);
    std::memset(impl->decode_in + copy_words, 0, (DECODE_IN_WORDS - copy_words) * sizeof(uint32_t));

    size_t consumed_words = 0;
    try
    {
        const uint32_t * const begin = impl->decode_in;

        if (is_full_block)
        {
            /// Full block: a single self-delimited SIMDFastPFor page. Validate every on-disk offset against
            /// the input length (and the bit width / exception positions) before decoding, so a corrupted
            /// page cannot read past decode_in or write past the 128-slot decode_out.
            validateSimdSingleBlockPage(begin, copy_words);

            size_t decoded = count;
            const uint32_t * end_ptr = impl->simd.decodeArray(begin, copy_words, impl->decode_out, decoded);
            if (decoded != count)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Corrupted FastPFOR posting block: SIMD codec produced {} values, expected {}", decoded, count);
            consumed_words = static_cast<size_t>(end_ptr - begin);
        }
        else
        {
            /// Tail block: the SIMD codec wrote only a zero-length page header (one word encoding 0 values);
            /// the values follow as a length-driven VariableByte run. Reject a non-zero header (it would
            /// otherwise drive __decodeArray off the end of the block) and decode the tail with a bounded
            /// decoder that cannot overrun decode_out.
            if (begin[0] != 0)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Corrupted FastPFOR posting block: tail block has a non-empty SIMD header");

            const uint8_t * tail_begin = reinterpret_cast<const uint8_t *>(begin + 1);
            const uint8_t * tail_end = reinterpret_cast<const uint8_t *>(begin + copy_words);
            const size_t produced = decodeVariableByteTailBounded(tail_begin, tail_end, impl->decode_out, count);
            if (produced != count)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Corrupted FastPFOR posting block: VariableByte tail decoded {} values, expected {}", produced, count);
            /// The whole block span (header + padded VariableByte run) belongs to this tail block.
            consumed_words = copy_words;
        }
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const std::exception & e)
    {
        /// FastPFOR's own guards throw std::logic_error / NotEnoughStorage; surface them as corruption.
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted FastPFOR posting block: {}", e.what());
    }

    const size_t consumed_bytes = consumed_words * sizeof(uint32_t);
    if (consumed_bytes > in.size())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted FastPFOR posting block: decoder consumed {} bytes but only {} were available", consumed_bytes, in.size());

    std::memcpy(out.data(), impl->decode_out, count * sizeof(uint32_t));
    in = in.subspan(consumed_bytes);
    return consumed_bytes;
}

#else

struct FastPFORBlockCodec::Impl
{
};

FastPFORBlockCodec::FastPFORBlockCodec()
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "FastPFOR posting list codec is not available: ClickHouse was built without FastPFOR");
}
FastPFORBlockCodec::~FastPFORBlockCodec() = default;

size_t FastPFORBlockCodec::encode(std::span<const uint32_t>, std::span<char> &)
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "FastPFOR posting list codec is not available: ClickHouse was built without FastPFOR");
}

size_t FastPFORBlockCodec::decode(std::span<const std::byte> &, size_t, std::span<uint32_t>)
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "FastPFOR posting list codec is not available: ClickHouse was built without FastPFOR");
}

#endif

}
