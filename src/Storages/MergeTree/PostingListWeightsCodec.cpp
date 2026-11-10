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

namespace
{
    /// Stores `(weight - 1)` with a per-block codec of the row ids: the weights are positive,
    /// so small weights, such as term frequencies, become small values that the block codec encodes compactly.
    template <typename BlockCodec>
    class WeightsCodecFromBlockCodec final : public IPostingListWeightsCodec
    {
    public:
        size_t encodeBlock(std::span<const uint32_t> weights, PODArray<char> & out) override
        {
            chassert(!weights.empty() && weights.size() <= BLOCK_SIZE);

            uint32_t values[BLOCK_SIZE];
            for (size_t i = 0; i < weights.size(); ++i)
            {
                chassert(weights[i] != 0);
                values[i] = weights[i] - 1;
            }

            return block_codec.encodeBlock(std::span<const uint32_t>(values, weights.size()), out);
        }

        /// Writes the block of zero `(weight - 1)` in the framing of the block codec, without encoding the values (see the specializations).
        size_t encodeOnes(size_t count, PODArray<char> & out) override;

        size_t decodeBlock(std::span<const std::byte> & in, size_t count, std::span<uint32_t> out) override
        {
            const size_t consumed = block_codec.decodeBlock(in, count, out);

            for (size_t i = 0; i < count; ++i)
                out[i] += 1;

            return consumed;
        }

        /// Steps over the block by the framing of the block codec, without decoding it (see the specializations).
        size_t skipBlock(std::span<const std::byte> & in, size_t count) override;

        size_t maxBlockBytes() const override { return block_codec.maxBlockBytes(); }

    private:
        static constexpr size_t BLOCK_SIZE = IPostingListBlockCodec::BLOCK_SIZE;

        BlockCodec block_codec;
    };

    template <>
    size_t WeightsCodecFromBlockCodec<BitpackingPostingListBlockCodec>::encodeOnes(size_t count, PODArray<char> & out)
    {
        chassert(count > 0 && count <= BLOCK_SIZE);

        /// A zero bits byte without payload: every value in the block is 0.
        out.push_back(char{0});
        return 1;
    }

    /// `PFor::encodeZeros` writes one header per PFor block, so a posting list block of zeros takes one byte.
    static_assert(IPostingListBlockCodec::BLOCK_SIZE <= PFor::BLOCK, "A posting list block must fit in one PFor block");

    template <>
    size_t WeightsCodecFromBlockCodec<PForPostingListBlockCodec>::encodeOnes(size_t count, PODArray<char> & out)
    {
        chassert(count > 0 && count <= BLOCK_SIZE);

        const size_t offset = out.size();
        out.resize(offset + 1);
        const size_t written = PFor::encodeZeros(count, reinterpret_cast<uint8_t *>(out.data() + offset));
        chassert(written == 1);
        return written;
    }

    template <>
    size_t WeightsCodecFromBlockCodec<BitpackingPostingListBlockCodec>::skipBlock(std::span<const std::byte> & in, size_t count)
    {
        if (in.empty())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected at least {} bytes, but got {}", 1, in.size());

        auto bits = static_cast<uint8_t>(in[0]);
        in = in.subspan(1);
        if (bits > 32)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected bits <= 32, but got {}", bits);

        /// `bits == 0` means every value in the block is 0 and no payload was written.
        size_t required_size = BitpackingBlockCodec::bitpackingCompressedBytes(count, bits);
        if (in.size() < required_size)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected data size {}, but got {}", required_size, in.size());

        in = in.subspan(required_size);

        /// Total bytes skipped in `in`: the bits byte plus the bit-packed payload.
        return 1 + required_size;
    }

    template <>
    size_t WeightsCodecFromBlockCodec<PForPostingListBlockCodec>::skipBlock(std::span<const std::byte> & in, size_t count)
    {
        chassert(count > 0 && count <= BLOCK_SIZE);
        const auto * data = reinterpret_cast<const uint8_t *>(in.data());

        /// The block size follows from its header, so the values are not decoded.
        const size_t skipped = PFor::skipBlocks<uint32_t>(data, count, data + in.size());
        if (skipped == 0)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupted data: malformed PFor block of {} values in {} available bytes", count, in.size());

        in = in.subspan(skipped);
        return skipped;
    }
}

std::unique_ptr<IPostingListWeightsCodec> createWeightsCodecFromBlockCodec(IPostingListCodec::Type type)
{
    switch (type)
    {
        case IPostingListCodec::Type::None:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Posting list codec 'None' has no per-block codec");
        case IPostingListCodec::Type::Bitpacking:
            return std::make_unique<WeightsCodecFromBlockCodec<BitpackingPostingListBlockCodec>>();
        case IPostingListCodec::Type::PFor:
            return std::make_unique<WeightsCodecFromBlockCodec<PForPostingListBlockCodec>>();
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown posting list codec type: {}", static_cast<int>(type));
}

}
