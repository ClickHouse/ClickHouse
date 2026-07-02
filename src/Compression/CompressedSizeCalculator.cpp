#include <city.h>

#include <base/defines.h>

#include <Compression/CompressedSizeCalculator.h>
#include <Compression/CompressionFactory.h>


namespace DB
{

CompressedSizeCalculator::CompressedSizeCalculator(CompressionCodecPtr codec_, size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size)
    , codec(std::move(codec_))
{
    if (!codec)
        codec = CompressionCodecFactory::instance().getDefaultCodec();
}

UInt32 CompressedSizeCalculator::getCompressedBlockSize(
    const ICompressionCodec & codec, const char * src, UInt32 src_size, PODArray<char> & scratch)
{
    /// If possible, calculate output size cheaply. If not, compress into a scratch buffer and keep only the returned size.
    if (auto calculated = codec.tryGetCompressedSize(src, src_size))
        return ICompressionCodec::getHeaderSize() + *calculated;

    scratch.resize(codec.getCompressedReserveSize(src_size));
    return codec.compress(src, src_size, scratch.data());
}

void CompressedSizeCalculator::nextImpl()
{
    if (!offset())
        return;

    chassert(offset() <= INT_MAX);
    const UInt32 decompressed_size = static_cast<UInt32>(offset());

    /// On-disk block layout must match CompressedWriteBuffer::nextImpl (checksum + header + payload)
    compressed_total
        += sizeof(CityHash_v1_0_2::uint128) + getCompressedBlockSize(*codec, working_buffer.begin(), decompressed_size, scratch);
}

}
