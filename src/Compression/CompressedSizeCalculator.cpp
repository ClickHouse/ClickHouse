#include <city.h>
#include <Compression/CompressedSizeCalculator.h>
#include <Compression/CompressionFactory.h>
#include <base/defines.h>


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

/// On-disk block layout must match CompressedWriteBuffer::nextImpl (checksum + header + payload)
UInt64 getOnDiskBlockSize(const ICompressionCodec & codec, const char * src, size_t src_size, PODArray<char> & scratch)
{
    chassert(src_size <= INT_MAX);
    return sizeof(CityHash_v1_0_2::uint128) + getCompressedBlockSize(codec, src, static_cast<UInt32>(src_size), scratch);
}

}

CompressedSizeCalculator::CompressedSizeCalculator(CompressionCodecPtr codec_, size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size)
    , codec(std::move(codec_))
{
    if (!codec)
        codec = CompressionCodecFactory::instance().getDefaultCodec();
}

UInt64 CompressedSizeCalculator::getCompressedBytes() const
{
    if (!offset())
        return compressed_total;

    /// Not `scratch`: this is const and may run concurrently on one shared instance.
    PODArray<char> open_block_scratch;
    return compressed_total + getOnDiskBlockSize(*codec, working_buffer.begin(), offset(), open_block_scratch);
}

void CompressedSizeCalculator::nextImpl()
{
    compressed_total += getOnDiskBlockSize(*codec, working_buffer.begin(), offset(), scratch);
}

}
