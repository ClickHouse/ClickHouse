#include <algorithm>
#include <city.h>
#include <Compression/CompressedSizeCalculator.h>
#include <Compression/CompressionFactory.h>
#include <base/defines.h>


namespace DB
{

namespace
{

/// The buffer grows from this size by doubling, up to the block size.
constexpr size_t initial_buffer_size = 64 * 1024;

/// Compressed size of a single block as `compress` would produce it (block header + payload).
/// If possible, calculate output size cheaply. If not, compress into a scratch buffer and keep only the returned size.
UInt32 getCompressedBlockSize(const ICompressionCodec & codec, const char * src, UInt32 src_size, PODArray<char> & scratch)
{
    if (auto calculated = codec.tryGetCompressedSize(src, src_size))
        return ICompressionCodec::getHeaderSize() + *calculated;

    scratch.resize(codec.getCompressedReserveSize(src_size));
    return codec.compress(src, src_size, scratch.data());
}

}

CompressedSizeCalculator::CompressedSizeCalculator(CompressionCodecPtr codec_, size_t block_size_)
    : BufferWithOwnMemory<WriteBuffer>(std::min(block_size_, initial_buffer_size))
    , codec(std::move(codec_))
    , block_size(block_size_)
{
    if (!codec)
        codec = CompressionCodecFactory::instance().getDefaultCodec();
}

void CompressedSizeCalculator::nextImpl()
{
    /// The working buffer is the tail of `memory`, behind the bytes of the block collected so far.
    const size_t collected = (working_buffer.begin() - memory.data()) + offset();

    /// The buffer is full but the block is not: a write that did not fit. Grow and keep collecting; `resize` keeps the content.
    if (collected < block_size && collected == memory.size() && !finalizing)
    {
        memory.resize(std::min(memory.size() * 2, block_size));
        internal_buffer = Buffer(memory.data(), memory.data() + memory.size());
        working_buffer = Buffer(memory.data() + collected, memory.data() + memory.size());
        return;
    }

    /// The block is complete, the buffer is finalized, or the serialization ends the block with `next`.
    compressBlock(collected);
}

void CompressedSizeCalculator::compressBlock(size_t size)
{
    chassert(size <= INT_MAX);
    /// On-disk block layout must match CompressedWriteBuffer::nextImpl (checksum + header + payload)
    compressed_total
        += sizeof(CityHash_v1_0_2::uint128) + getCompressedBlockSize(*codec, memory.data(), static_cast<UInt32>(size), scratch);
    working_buffer = internal_buffer;
}

void CompressedSizeCalculator::finalizeImpl()
{
    finalizing = true;
    next();
    /// `next` does nothing when no byte followed an explicit `next`, whose bytes are still collected.
    if (const size_t collected = working_buffer.begin() - memory.data())
        compressBlock(collected);
}

}
