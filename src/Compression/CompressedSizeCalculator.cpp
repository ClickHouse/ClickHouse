#include <city.h>
#include <Compression/CompressedSizeCalculator.h>
#include <Compression/CompressionFactory.h>
#include <base/defines.h>
#include <base/scope_guard.h>


namespace DB
{

namespace
{

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

CompressedSizeCalculator::CompressedSizeCalculator(CompressionCodecPtr codec_, size_t buf_size, size_t initial_size)
    : BufferWithOwnMemory<WriteBuffer>(initial_size ? std::min(initial_size, buf_size) : buf_size)
    , codec(std::move(codec_))
    , block_size(buf_size)
{
    if (!codec)
        codec = CompressionCodecFactory::instance().getDefaultCodec();
}

void CompressedSizeCalculator::endBlock()
{
    ending_block = true;
    SCOPE_EXIT({ ending_block = false; });
    next();
}

void CompressedSizeCalculator::nextImpl()
{
    /// A write needs room in a buffer smaller than a block: the buffer grows and the block stays open.
    if (!ending_block && !available() && memory.size() < block_size)
    {
        const size_t size = offset();
        memory.resize(std::min(block_size, memory.size() * 2));
        set(memory.data(), memory.size());
        nextimpl_working_buffer_offset = size;
        /// `next` counts the bytes of the buffer as written after this, and they are still in it.
        bytes -= size;
        return;
    }

    chassert(offset() <= INT_MAX);
    const UInt32 decompressed_size = static_cast<UInt32>(offset());
    /// Lives for one block: a scratch kept by every calculator would hold a block's worth of memory per stream of every
    /// group of `estimateCompressionRatio`.
    PODArray<char> scratch;

    /// On-disk block layout must match CompressedWriteBuffer::nextImpl (checksum + header + payload)
    compressed_total
        += sizeof(CityHash_v1_0_2::uint128) + getCompressedBlockSize(*codec, working_buffer.begin(), decompressed_size, scratch);
}

}
