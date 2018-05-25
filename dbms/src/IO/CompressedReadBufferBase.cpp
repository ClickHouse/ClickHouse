#include <IO/CompressedReadBufferBase.h>

#include <vector>

#include <string.h>
#include <city.h>
#include <lz4.h>
#include <zstd.h>

#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <common/unaligned.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressedStream.h>
#include <IO/WriteHelpers.h>
#include <Compression/CompressionCodecFactory.h>


namespace ProfileEvents
{
    extern const Event ReadCompressedBytes;
    extern const Event CompressedReadBufferBlocks;
    extern const Event CompressedReadBufferBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_COMPRESSION_METHOD;
    extern const int TOO_LARGE_SIZE_COMPRESSED;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int CANNOT_DECOMPRESS;
}


/// Read compressed data into compressed_buffer. Get size of decompressed data from block header. Checksum if need.
/// Returns number of compressed bytes read.
size_t CompressedReadBufferBase::readCompressedData(size_t & size_decompressed, size_t & size_compressed_without_checksum)
{
    if (compressed_in->eof())
        return 0;

    CityHash_v1_0_2::uint128 checksum;
    compressed_in->readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));

    own_compressed_buffer.resize(COMPRESSED_BLOCK_HEADER_SIZE);
    compression_pipe = CompressionCodecFactory::instance().get_pipe(compressed_in);

    size_compressed_without_checksum = compression_pipe->getCompressedSize();
    size_t size_compressed = size_compressed_without_checksum + compression_pipe->getHeaderSize();
    size_decompressed = compression_pipe->getDecompressedSize();

    if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
        throw Exception("Too large size_compressed. Most likely corrupted data.", ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

    ProfileEvents::increment(ProfileEvents::ReadCompressedBytes, size_compressed + sizeof(checksum));

    /// Is whole compressed block located in 'compressed_in' buffer?
    if (compressed_in->offset() >= compression_pipe->getHeaderSize() &&
        compressed_in->position() + size_compressed - compression_pipe->getHeaderSize() <= compressed_in->buffer().end())
    {
        compressed_in->position() -= compression_pipe->getHeaderSize();
        compressed_buffer = compressed_in->position();
        compressed_in->position() += size_compressed;
    }
    else
    {
        own_compressed_buffer.resize(size_compressed);
        compressed_buffer = &own_compressed_buffer[0];
        compressed_in->readStrict(compressed_buffer + compression_pipe->getHeaderSize(),
                                  size_compressed - compression_pipe->getHeaderSize());
    }

    if (!disable_checksum && checksum != CityHash_v1_0_2::CityHash128(compressed_buffer, size_compressed))
        throw Exception("Checksum doesn't match: corrupted data.", ErrorCodes::CHECKSUM_DOESNT_MATCH);

    return size_compressed + sizeof(checksum);
}


void CompressedReadBufferBase::decompress(char * to, size_t size_decompressed, size_t size_compressed_without_checksum)
{
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBlocks);
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBytes, size_decompressed);

    auto header_size = compression_pipe->getHeaderSize();
    compression_pipe->decompress(compressed_buffer + header_size, to,
                                 size_compressed_without_checksum, size_decompressed);
}


/// 'compressed_in' could be initialized lazily, but before first call of 'readCompressedData'.
CompressedReadBufferBase::CompressedReadBufferBase(ReadBuffer * in)
    : compressed_in(in), own_compressed_buffer(COMPRESSED_BLOCK_HEADER_SIZE)
{
}


CompressedReadBufferBase::~CompressedReadBufferBase() = default;    /// Proper destruction of unique_ptr of forward-declared type.


}
