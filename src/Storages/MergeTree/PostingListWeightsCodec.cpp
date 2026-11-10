#include <Storages/MergeTree/PostingListWeightsCodec.h>

#include <Compression/PFor.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

/// The codecs below store `(weight - 1)`: the weights are positive, so small weights, such as term frequencies,
/// become small values that are encoded compactly, and a block of weights equal to 1 is a block of zeros.
namespace
{
    constexpr size_t BLOCK_SIZE = IPostingListBlockCodec::BLOCK_SIZE;

    void subtractOne(std::span<const uint32_t> weights, uint32_t * values)
    {
        chassert(!weights.empty() && weights.size() <= BLOCK_SIZE);

        for (size_t i = 0; i < weights.size(); ++i)
        {
            chassert(weights[i] != 0);
            values[i] = weights[i] - 1;
        }
    }

    void addOne(std::span<uint32_t> values, size_t count)
    {
        for (size_t i = 0; i < count; ++i)
            values[i] += 1;
    }

    /// [1 byte bits][bit-packed payload]. A block of zeros has `bits = 0` and no payload.
    class BitpackingWeightsCodec final : public IPostingListWeightsCodec
    {
    public:
        size_t encodeBlock(std::span<const uint32_t> weights, PODArray<char> & out) override
        {
            uint32_t values[BLOCK_SIZE];
            subtractOne(weights, values);
            std::span<const uint32_t> values_span(values, weights.size());

            auto [payload_bytes, bits] = BitpackingBlockCodec::calculateNeededBytesAndMaxBits(values_span);

            const size_t offset = out.size();
            out.resize(offset + 1 + payload_bytes);
            out[offset] = static_cast<char>(bits);

            std::span<char> payload(out.data() + offset + 1, payload_bytes);
            const size_t written = BitpackingBlockCodec::encode(values_span, bits, payload);

            if (written != payload_bytes)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Bitpacking encode size mismatch: expected {} bytes for {} weights with {} bits, but written {} bytes",
                    payload_bytes, weights.size(), bits, written);
            }

            return 1 + payload_bytes;
        }

        size_t encodeOnes(size_t count, PODArray<char> & out) override
        {
            chassert(count > 0 && count <= BLOCK_SIZE);
            out.push_back(char{0});
            return 1;
        }

        size_t decodeBlock(std::span<const std::byte> & in, size_t count, std::span<uint32_t> out) override
        {
            chassert(count > 0 && count <= out.size());

            const auto [bits, payload_bytes] = readHeader(in, count);
            const size_t consumed = BitpackingBlockCodec::decode(in, count, bits, out);

            if (consumed != payload_bytes)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Bitpacking decode size mismatch: expected to consume {} bytes for {} weights with {} bits, but consumed {} bytes",
                    payload_bytes, count, bits, consumed);
            }

            addOne(out, count);
            return 1 + payload_bytes;
        }

        size_t skipBlock(std::span<const std::byte> & in, size_t count) override
        {
            chassert(count > 0 && count <= BLOCK_SIZE);

            const size_t payload_bytes = readHeader(in, count).second;
            in = in.subspan(payload_bytes);
            return 1 + payload_bytes;
        }

        /// `1` (bits header) + `4 * BLOCK_SIZE` (bit-pure max at `bits = 32`) + 16 (SIMD alignment slack).
        size_t maxBlockBytes() const override { return 1 + sizeof(uint32_t) * BLOCK_SIZE + 16; }

    private:
        /// Reads the bits byte of a block of `count` values and checks that its payload is in `in`.
        /// Returns the bits and the payload size.
        static std::pair<uint8_t, size_t> readHeader(std::span<const std::byte> & in, size_t count)
        {
            if (in.empty())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected at least {} bytes, but got {}", 1, in.size());

            const auto bits = static_cast<uint8_t>(in[0]);
            in = in.subspan(1);
            if (bits > 32)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected bits <= 32, but got {}", bits);

            const size_t payload_bytes = BitpackingBlockCodec::bitpackingCompressedBytes(count, bits);
            if (in.size() < payload_bytes)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected data size {}, but got {}", payload_bytes, in.size());

            return {bits, payload_bytes};
        }
    };

    /// `PFor::encodeZeros` writes one header per PFor block, so a posting list block of zeros takes one byte.
    static_assert(BLOCK_SIZE <= PFor::BLOCK, "A posting list block must fit in one PFor block");

    /// One PFor block.
    class PForWeightsCodec final : public IPostingListWeightsCodec
    {
    public:
        size_t encodeBlock(std::span<const uint32_t> weights, PODArray<char> & out) override
        {
            uint32_t values[BLOCK_SIZE];
            subtractOne(weights, values);

            /// The encoded size is only known afterwards; `PODArray::resize` does not zero-fill, so grow to the bound and shrink.
            const size_t offset = out.size();
            out.resize(offset + MAX_BLOCK_BYTES);
            const size_t written = PFor::encodeBlocks<uint32_t>(std::span<const uint32_t>(values, weights.size()), PFor::Delta::none, reinterpret_cast<uint8_t *>(out.data() + offset));

            if (written == 0 || written > MAX_BLOCK_BYTES)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "PFor encode size mismatch: written {} bytes for {} weights, expected 1 to {} bytes",
                    written, weights.size(), MAX_BLOCK_BYTES);
            }

            out.resize(offset + written);
            return written;
        }

        size_t encodeOnes(size_t count, PODArray<char> & out) override
        {
            chassert(count > 0 && count <= BLOCK_SIZE);

            const size_t offset = out.size();
            out.resize(offset + 1);
            return PFor::encodeZeros(count, reinterpret_cast<uint8_t *>(out.data() + offset));
        }

        size_t decodeBlock(std::span<const std::byte> & in, size_t count, std::span<uint32_t> out) override
        {
            chassert(count > 0 && count <= out.size());
            const auto * data = reinterpret_cast<const uint8_t *>(in.data());

            /// `count` is never zero here, so a zero return means malformed input, not an empty block.
            const size_t consumed = PFor::decodeBlocks<uint32_t>(data, count, PFor::Delta::none, out.data(), data + in.size());
            if (consumed == 0)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: malformed PFor block of {} values in {} available bytes", count, in.size());

            in = in.subspan(consumed);
            addOne(out, count);
            return consumed;
        }

        size_t skipBlock(std::span<const std::byte> & in, size_t count) override
        {
            chassert(count > 0 && count <= BLOCK_SIZE);
            const auto * data = reinterpret_cast<const uint8_t *>(in.data());

            /// The block size follows from its header, so the values are not decoded.
            const size_t skipped = PFor::skipBlocks<uint32_t>(data, count, data + in.size());
            if (skipped == 0)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: malformed PFor block of {} values in {} available bytes", count, in.size());

            in = in.subspan(skipped);
            return skipped;
        }

        size_t maxBlockBytes() const override { return MAX_BLOCK_BYTES; }

    private:
        static constexpr size_t MAX_BLOCK_BYTES = PFor::maxCompressedBytes<uint32_t>(BLOCK_SIZE);
    };
}

std::unique_ptr<IPostingListWeightsCodec> createPostingListWeightsCodec(IPostingListCodec::Type type)
{
    switch (type)
    {
        case IPostingListCodec::Type::None:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Posting list codec 'None' has no per-block codec");
        case IPostingListCodec::Type::Bitpacking:
            return std::make_unique<BitpackingWeightsCodec>();
        case IPostingListCodec::Type::PFor:
            return std::make_unique<PForWeightsCodec>();
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown posting list codec type: {}", static_cast<int>(type));
}

}
