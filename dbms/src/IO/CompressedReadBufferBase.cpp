#include <IO/CompressedReadBufferBase.h>

#include <vector>

#include <string.h>
#include <city.h>
#include <zstd.h>

#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/hex.h>
#include <common/unaligned.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressedStream.h>
#include <IO/WriteHelpers.h>


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

static constexpr auto CHECKSUM_SIZE{sizeof(CityHash_v1_0_2::uint128)};

/// Read compressed data into compressed_buffer. Get size of decompressed data from block header. Checksum if need.
/// Returns number of compressed bytes read.
size_t CompressedReadBufferBase::readCompressedData(size_t & size_decompressed, size_t & size_compressed_without_checksum)
{
    if (compressed_in->eof())
        return 0;

    CityHash_v1_0_2::uint128 checksum;
    compressed_in->readStrict(reinterpret_cast<char *>(&checksum), CHECKSUM_SIZE);

    UInt8 header_size = ICompressionCodec::getHeaderSize();
    own_compressed_buffer.resize(header_size);
    compressed_in->readStrict(own_compressed_buffer.data(), header_size);

    UInt8 method = ICompressionCodec::readMethod(own_compressed_buffer.data());

    if (!codec)
        codec = CompressionCodecFactory::instance().get(method);
    else if (method != codec->getMethodByte())
        throw Exception("Data compressed with different method, given method byte " + getHexUIntLowercase(method) + ", previous method byte " + getHexUIntLowercase(codec->getMethodByte()), ErrorCodes::CANNOT_DECOMPRESS);

    size_compressed_without_checksum = ICompressionCodec::readCompressedBlockSize(own_compressed_buffer.data());
    size_decompressed = ICompressionCodec::readDecompressedBlockSize(own_compressed_buffer.data());

    if (size_compressed_without_checksum > DBMS_MAX_COMPRESSED_SIZE)
        throw Exception("Too large size_compressed_without_checksum: " + toString(size_compressed_without_checksum) + ". Most likely corrupted data.", ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

    ProfileEvents::increment(ProfileEvents::ReadCompressedBytes, size_compressed_without_checksum + CHECKSUM_SIZE);

    /// Is whole compressed block located in 'compressed_in->' buffer?
    if (compressed_in->offset() >= header_size &&
        compressed_in->position() + size_compressed_without_checksum + codec->getAdditionalSizeAtTheEndOfBuffer()  - header_size <= compressed_in->buffer().end())
    {
        compressed_in->position() -= header_size;
        compressed_buffer = compressed_in->position();
        compressed_in->position() += size_compressed_without_checksum;
    }
    else
    {
        own_compressed_buffer.resize(size_compressed_without_checksum + codec->getAdditionalSizeAtTheEndOfBuffer());
        compressed_buffer = own_compressed_buffer.data();
        compressed_in->readStrict(compressed_buffer + header_size, size_compressed_without_checksum - header_size);
    }

    if (!disable_checksum)
    {
        auto checksum_calculated = CityHash_v1_0_2::CityHash128(compressed_buffer, size_compressed_without_checksum);
        if (checksum != checksum_calculated)
            throw Exception("Checksum doesn't match: corrupted data."
                            " Reference: " + getHexUIntLowercase(checksum.first) + getHexUIntLowercase(checksum.second)
                            + ". Actual: " + getHexUIntLowercase(checksum_calculated.first) + getHexUIntLowercase(checksum_calculated.second)
                            + ". Size of compressed block: " + toString(size_compressed_without_checksum),
                            ErrorCodes::CHECKSUM_DOESNT_MATCH);
    }


    return size_compressed_without_checksum + sizeof(checksum);
}


void CompressedReadBufferBase::decompress(char * to, size_t size_decompressed, size_t size_compressed_without_checksum)
{
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBlocks);
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBytes, size_decompressed);

    UInt8 method = ICompressionCodec::readMethod(compressed_buffer);

    if (!codec)
        codec = CompressionCodecFactory::instance().get(method);
    else if (codec->getMethodByte() != method)
        throw Exception("Data compressed with different method, given method byte " + getHexUIntLowercase(method) + ", previous method byte " + getHexUIntLowercase(codec->getMethodByte()), ErrorCodes::CANNOT_DECOMPRESS);

    codec->decompress(compressed_buffer, size_compressed_without_checksum, to);
}


/// 'compressed_in' could be initialized lazily, but before first call of 'readCompressedData'.
CompressedReadBufferBase::CompressedReadBufferBase(ReadBuffer * in)
    : compressed_in(in), own_compressed_buffer(0)
{
}


CompressedReadBufferBase::~CompressedReadBufferBase() = default;    /// Proper destruction of unique_ptr of forward-declared type.


}

