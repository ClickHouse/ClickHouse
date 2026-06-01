#include <city.h>

#include <base/defines.h>

#include <Compression/CompressedSizeEstimator.h>
#include <Compression/CompressionFactory.h>


namespace DB
{

CompressedSizeEstimator::CompressedSizeEstimator(CompressionCodecPtr codec_, size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size)
    , codec(std::move(codec_))
{
    if (!codec)
        codec = CompressionCodecFactory::instance().getDefaultCodec();
}

void CompressedSizeEstimator::nextImpl()
{
    if (!offset())
        return;

    chassert(offset() <= INT_MAX);
    const UInt32 decompressed_size = static_cast<UInt32>(offset());

    UInt32 compressed_size;
    if (auto predicted = codec->tryGetCompressedSize(working_buffer.begin(), decompressed_size))
    {
        compressed_size = ICompressionCodec::getHeaderSize() + *predicted;
    }
    else
    {
        /// Fallback: codec cannot predict its output size cheaply. Compress into a scratch buffer and keep only the returned size
        scratch.resize(codec->getCompressedReserveSize(decompressed_size));
        compressed_size = codec->compress(working_buffer.begin(), decompressed_size, scratch.data());
    }

    /// On-disk block layout must match CompressedWriteBuffer::nextImpl (checksum + header + payload)
    compressed_total += sizeof(CityHash_v1_0_2::uint128) + compressed_size;
}

}
