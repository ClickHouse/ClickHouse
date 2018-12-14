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


/// Read compressed data into compressed_buffer. Get size of decompressed data from block header. Checksum if need.
/// Returns number of compressed bytes read.
size_t CompressedReadBufferBase::readCompressedData(size_t & size_decompressed, size_t & size_compressed_without_checksum)
{
    if (compressed_in->eof())
        return 0;

    CityHash_v1_0_2::uint128 checksum;
    compressed_in->readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));

    own_compressed_buffer.resize(COMPRESSED_BLOCK_HEADER_SIZE);
    compressed_in->readStrict(own_compressed_buffer.data(), COMPRESSED_BLOCK_HEADER_SIZE);

    UInt8 method = own_compressed_buffer[0];    /// See CompressedWriteBuffer.h

    size_t & size_compressed = size_compressed_without_checksum;

    if (method == static_cast<UInt8>(CompressionMethodByte::LZ4) ||
        method == static_cast<UInt8>(CompressionMethodByte::ZSTD) ||
        method == static_cast<UInt8>(CompressionMethodByte::NONE))
    {
        size_compressed = unalignedLoad<UInt32>(&own_compressed_buffer[1]);
        size_decompressed = unalignedLoad<UInt32>(&own_compressed_buffer[5]);
    }
    else
        throw Exception("Unknown compression method: " + toString(method), ErrorCodes::UNKNOWN_COMPRESSION_METHOD);

    if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
        throw Exception("Too large size_compressed: " + toString(size_compressed) + ". Most likely corrupted data.", ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

    ProfileEvents::increment(ProfileEvents::ReadCompressedBytes, size_compressed + sizeof(checksum));

    /// Is whole compressed block located in 'compressed_in' buffer?
    if (compressed_in->offset() >= COMPRESSED_BLOCK_HEADER_SIZE &&
        compressed_in->position() + size_compressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER - COMPRESSED_BLOCK_HEADER_SIZE <= compressed_in->buffer().end())
    {
        compressed_in->position() -= COMPRESSED_BLOCK_HEADER_SIZE;
        compressed_buffer = compressed_in->position();
        compressed_in->position() += size_compressed;
    }
    else
    {
        own_compressed_buffer.resize(size_compressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
        compressed_buffer = own_compressed_buffer.data();
        compressed_in->readStrict(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, size_compressed - COMPRESSED_BLOCK_HEADER_SIZE);
    }

    if (!disable_checksum)
    {
        auto checksum_calculated = CityHash_v1_0_2::CityHash128(compressed_buffer, size_compressed);
        if (checksum != checksum_calculated)
            throw Exception("Checksum doesn't match: corrupted data."
                " Reference: " + getHexUIntLowercase(checksum.first) + getHexUIntLowercase(checksum.second)
                + ". Actual: " + getHexUIntLowercase(checksum_calculated.first) + getHexUIntLowercase(checksum_calculated.second)
                + ". Size of compressed block: " + toString(size_compressed) + ".",
                ErrorCodes::CHECKSUM_DOESNT_MATCH);
    }

    return size_compressed + sizeof(checksum);
}


void CompressedReadBufferBase::decompress(char * to, size_t size_decompressed, size_t size_compressed_without_checksum)
{
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBlocks);
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBytes, size_decompressed);

    UInt8 method = compressed_buffer[0];    /// See CompressedWriteBuffer.h

    if (method == static_cast<UInt8>(CompressionMethodByte::LZ4))
    {
        LZ4::decompress(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_compressed_without_checksum, size_decompressed, lz4_stat);
    }
    else if (method == static_cast<UInt8>(CompressionMethodByte::ZSTD))
    {
        size_t res = ZSTD_decompress(
            to, size_decompressed,
            compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, size_compressed_without_checksum - COMPRESSED_BLOCK_HEADER_SIZE);

        if (ZSTD_isError(res))
            throw Exception("Cannot ZSTD_decompress: " + std::string(ZSTD_getErrorName(res)), ErrorCodes::CANNOT_DECOMPRESS);
    }
    else if (method == static_cast<UInt8>(CompressionMethodByte::NONE))
    {
        memcpy(to, &compressed_buffer[COMPRESSED_BLOCK_HEADER_SIZE], size_decompressed);
    }
    else
        throw Exception("Unknown compression method: " + toString(method), ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
}


/// 'compressed_in' could be initialized lazily, but before first call of 'readCompressedData'.
CompressedReadBufferBase::CompressedReadBufferBase(ReadBuffer * in)
    : compressed_in(in), own_compressed_buffer(COMPRESSED_BLOCK_HEADER_SIZE)
{
}


CompressedReadBufferBase::~CompressedReadBufferBase() = default;    /// Proper destruction of unique_ptr of forward-declared type.


}

