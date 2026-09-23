#include <Storages/MergeTree/PostingListBlockCodec.h>

#include <Compression/PFor.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

/// A full posting list block is bit-packed as whole SIMD blocks, which is what the SIMD implementation is efficient at.
static_assert(IPostingListBlockCodec::BLOCK_SIZE % BitpackingBlockCodec::BLOCK_SIZE == 0,
    "The posting list block size must be a multiple of the bitpacking block size");

namespace
{
    void writeByte(uint8_t x, std::span<char> & out)
    {
        out[0] = static_cast<char>(x);
        out = out.subspan(1);
    }

    uint8_t readByte(std::span<const std::byte> & in)
    {
        auto v = static_cast<uint8_t>(in[0]);
        in = in.subspan(1);
        return v;
    }

    /// [1 byte bits][bitpacked payload]. The bits byte selects the per-block bit width; the payload is the
    /// SIMD/portable bit-packed stream produced by BitpackingBlockCodec.
    class BitpackingPostingListBlockCodec : public IPostingListBlockCodec
    {
    public:
        size_t encodeBlock(std::span<uint32_t> deltas, PODArray<char> & out) override
        {
            auto [needed_bytes_without_header, max_bits] = BitpackingBlockCodec::calculateNeededBytesAndMaxBits(deltas);
            size_t needed_bytes_with_header = needed_bytes_without_header + 1;

            /// Block Layout: [1byte(max_bits)][payload]
            size_t offset = out.size();
            out.resize(out.size() + needed_bytes_with_header);
            std::span<char> out_span(out.data() + offset, needed_bytes_with_header);
            writeByte(static_cast<uint8_t>(max_bits), out_span);
            auto used_memory = BitpackingBlockCodec::encode(deltas, max_bits, out_span);

            if (used_memory != needed_bytes_without_header || !out_span.empty())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Bitpacking encode size mismatch: expected {} bytes payload with {} bits encoding for {} integers, "
                    "but actually used {} bytes with {} bytes remaining in buffer",
                    needed_bytes_without_header,
                    max_bits,
                    deltas.size(),
                    used_memory,
                    out_span.size());
            }

            return needed_bytes_with_header;
        }

        size_t decodeBlock(std::span<const std::byte> & in, size_t count, std::span<uint32_t> out) override
        {
            if (in.empty())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected at least {} bytes, but got {}", 1, in.size());

            uint8_t bits = readByte(in);
            if (bits > 32)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected bits <= 32, but got {}", bits);

            size_t required_size = BitpackingBlockCodec::bitpackingCompressedBytes(count, bits);
            if (in.size() < required_size)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected data size {}, but got {}", required_size, in.size());

            size_t consumed_size = BitpackingBlockCodec::decode(in, count, bits, out);
            if (required_size != consumed_size)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Bitpacking decode size mismatch: expected to consume {} bytes for {} integers with {} bits, but actually consumed {} bytes",
                required_size,
                count,
                bits,
                consumed_size);

            /// Total bytes consumed from `in`: the bits byte plus the bit-packed payload.
            return 1 + consumed_size;
        }

        /// `1` (bits header) + `4 * BLOCK_SIZE` (bit-pure max at `bits = 32`) + 16 (SIMD alignment slack).
        size_t maxBlockBytes() const override { return 1 + sizeof(uint32_t) * BLOCK_SIZE + 16; }

        IPostingListCodec::Type type() const override { return IPostingListCodec::Type::Bitpacking; }
    };

    /// One PFor block over gaps `SegmentedPostingListCodec` already computed, hence `Delta::none`.
    /// TODO(ahmadov): move delta into PFor once `deltaApply` stops spilling, today `d0` is slower on exception blocks.
    class PForPostingListBlockCodec : public IPostingListBlockCodec
    {
    public:
        size_t encodeBlock(std::span<uint32_t> deltas, PODArray<char> & out) override
        {
            /// `MAX_BLOCK_BYTES` bounds one block, so an oversized input would overrun the reserved tail.
            if (deltas.empty() || deltas.size() > BLOCK_SIZE)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "PFor block must hold 1 to {} values, got {}", BLOCK_SIZE, deltas.size());

            /// The encoded size is only known afterwards; `PODArray::resize` does not zero-fill, so grow to the bound and shrink.
            const size_t offset = out.size();
            out.resize(offset + MAX_BLOCK_BYTES);
            const size_t written = PFor::encodeBlocks<uint32_t>(deltas, PFor::Delta::none, reinterpret_cast<uint8_t *>(out.data() + offset));
            chassert(written > 0 && written <= MAX_BLOCK_BYTES);
            out.resize(offset + written);
            return written;
        }

        size_t decodeBlock(std::span<const std::byte> & in, size_t count, std::span<uint32_t> out) override
        {
            chassert(count > 0 && count <= out.size());
            const auto * data = reinterpret_cast<const uint8_t *>(in.data());

            /// `count` is never zero here, so a zero return means malformed input, not an empty block.
            const size_t consumed = PFor::decodeBlocks<uint32_t>(data, count, PFor::Delta::none, out.data(), data + in.size());
            if (consumed == 0)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Corrupted data: malformed PFor block of {} values in {} available bytes", count, in.size());

            in = in.subspan(consumed);
            return consumed;
        }

        size_t maxBlockBytes() const override { return MAX_BLOCK_BYTES; }

        IPostingListCodec::Type type() const override { return IPostingListCodec::Type::PFor; }

    private:
        static constexpr size_t MAX_BLOCK_BYTES = PFor::maxCompressedBytes<uint32_t>(BLOCK_SIZE);
    };

}

std::unique_ptr<IPostingListBlockCodec> createPostingListBlockCodec(IPostingListCodec::Type type)
{
    switch (type)
    {
        case IPostingListCodec::Type::None:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Posting list codec 'None' has no per-block codec");
        case IPostingListCodec::Type::Bitpacking:
            return std::make_unique<BitpackingPostingListBlockCodec>();
        case IPostingListCodec::Type::PFor:
            return std::make_unique<PForPostingListBlockCodec>();
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown posting list codec type: {}", static_cast<int>(type));
}

}
