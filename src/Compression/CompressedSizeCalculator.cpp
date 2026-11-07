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
constexpr size_t initial_buffer_size = 16 * 1024;

size_t initialBufferSize(size_t block_size, size_t adaptive_initial_size)
{
    return std::min(block_size, adaptive_initial_size ? adaptive_initial_size : initial_buffer_size);
}

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

CompressedSizeCalculator::CompressedSizeCalculator(CompressionCodecPtr codec_, size_t block_size_, size_t adaptive_initial_size_)
    : BufferWithOwnMemory<WriteBuffer>(initialBufferSize(block_size_, adaptive_initial_size_))
    , codec(std::move(codec_))
    , block_size(block_size_)
    , adaptive(adaptive_initial_size_ != 0)
{
    if (!codec)
        codec = CompressionCodecFactory::instance().getDefaultCodec();
}

void CompressedSizeCalculator::nextImpl()
{
    const size_t collected = blockBytes();
    const bool buffer_full = collected == memory.size();

    /// The buffer is full but the block is not: a write that did not fit. Grow and keep collecting; `resize` keeps the content.
    if (!adaptive && buffer_full && collected < block_size && !closing)
    {
        memory.resize(std::min(memory.size() * 2, block_size));
        internal_buffer = Buffer(memory.data(), memory.data() + memory.size());
        working_buffer = Buffer(memory.data() + collected, memory.data() + memory.size());
        return;
    }

    /// The block is complete, is ended on purpose, or the serialization ends it with `next`.
    compressBlock(collected);

    /// As `BufferWithOwnMemory::growAdaptiveBufferAfterFlush`: an adaptive buffer doubles after a block that filled it.
    if (adaptive && buffer_full && memory.size() < block_size)
    {
        memory.resize(std::min(memory.size() * 2, block_size));
        internal_buffer = Buffer(memory.data(), memory.data() + memory.size());
        working_buffer = internal_buffer;
        pos = working_buffer.begin();
    }
}

void CompressedSizeCalculator::compressBlock(size_t size)
{
    chassert(size <= INT_MAX);
    /// On-disk block layout must match CompressedWriteBuffer::nextImpl (checksum + header + payload)
    compressed_total
        += sizeof(CityHash_v1_0_2::uint128) + getCompressedBlockSize(*codec, memory.data(), static_cast<UInt32>(size), scratch);
    working_buffer = internal_buffer;
    /// `next` resets `pos` itself; `endBlock` reaches here without it, and `count` adds `offset` to the bytes counted.
    pos = working_buffer.begin();
}

void CompressedSizeCalculator::endBlock()
{
    closing = true;
    next();
    closing = false;

    /// `next` does nothing when no byte followed a grow, whose bytes are still collected.
    if (const size_t collected = working_buffer.begin() - memory.data())
        compressBlock(collected);
}

void CompressedSizeCalculator::finalizeImpl()
{
    endBlock();
}

}
