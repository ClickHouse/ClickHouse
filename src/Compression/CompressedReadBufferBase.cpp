#include "CompressedReadBufferBase.h"

#include <cstring>
#include <cassert>
#include <city.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/hex.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Compression/CompressionInfo.h>
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
    extern const int TOO_LARGE_SIZE_COMPRESSED;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int CANNOT_DECOMPRESS;
    extern const int CORRUPTED_DATA;
}

using Checksum = CityHash_v1_0_2::uint128;


/// Validate checksum of data, and if it mismatches, find out possible reason and throw exception.
static void validateChecksum(char * data, size_t size, const Checksum expected_checksum)
{
    auto calculated_checksum = CityHash_v1_0_2::CityHash128(data, size);
    if (expected_checksum == calculated_checksum)
        return;

    std::stringstream message;

    /// TODO mess up of endianess in error message.
    message << "Checksum doesn't match: corrupted data."
        " Reference: " + getHexUIntLowercase(expected_checksum.first) + getHexUIntLowercase(expected_checksum.second)
        + ". Actual: " + getHexUIntLowercase(calculated_checksum.first) + getHexUIntLowercase(calculated_checksum.second)
        + ". Size of compressed block: " + toString(size);

    const char * message_hardware_failure = "This is most likely due to hardware failure. If you receive broken data over network and the error does not repeat every time, this can be caused by bad RAM on network interface controller or bad controller itself or bad RAM on network switches or bad CPU on network switches (look at the logs on related network switches; note that TCP checksums don't help) or bad RAM on host (look at dmesg or kern.log for enormous amount of EDAC errors, ECC-related reports, Machine Check Exceptions, mcelog; note that ECC memory can fail if the number of errors is huge) or bad CPU on host. If you read data from disk, this can be caused by disk bit rott. This exception protects ClickHouse from data corruption due to hardware failures.";

    auto flip_bit = [](char * buf, size_t pos)
    {
        buf[pos / 8] ^= 1 << pos % 8;
    };

    /// Check if the difference caused by single bit flip in data.
    for (size_t bit_pos = 0; bit_pos < size * 8; ++bit_pos)
    {
        flip_bit(data, bit_pos);

        auto checksum_of_data_with_flipped_bit = CityHash_v1_0_2::CityHash128(data, size);
        if (expected_checksum == checksum_of_data_with_flipped_bit)
        {
            message << ". The mismatch is caused by single bit flip in data block at byte " << (bit_pos / 8) << ", bit " << (bit_pos % 8) << ". "
                << message_hardware_failure;
            throw Exception(message.str(), ErrorCodes::CHECKSUM_DOESNT_MATCH);
        }

        flip_bit(data, bit_pos);    /// Restore
    }

    /// Check if the difference caused by single bit flip in stored checksum.
    size_t difference = __builtin_popcountll(expected_checksum.first ^ calculated_checksum.first)
        + __builtin_popcountll(expected_checksum.second ^ calculated_checksum.second);

    if (difference == 1)
    {
        message << ". The mismatch is caused by single bit flip in checksum. "
            << message_hardware_failure;
        throw Exception(message.str(), ErrorCodes::CHECKSUM_DOESNT_MATCH);
    }

    throw Exception(message.str(), ErrorCodes::CHECKSUM_DOESNT_MATCH);
}


/// Read compressed data into compressed_buffer. Get size of decompressed data from block header. Checksum if need.
/// Returns number of compressed bytes read.
size_t CompressedReadBufferBase::readCompressedData(size_t & size_decompressed, size_t & size_compressed_without_checksum)
{
    if (compressed_in->eof())
        return 0;

    Checksum checksum;
    compressed_in->readStrict(reinterpret_cast<char *>(&checksum), sizeof(Checksum));

    UInt8 header_size = ICompressionCodec::getHeaderSize();
    own_compressed_buffer.resize(header_size);
    compressed_in->readStrict(own_compressed_buffer.data(), header_size);

    uint8_t method = ICompressionCodec::readMethod(own_compressed_buffer.data());

    if (!codec)
        codec = CompressionCodecFactory::instance().get(method);
    else if (method != codec->getMethodByte())
        throw Exception("Data compressed with different methods, given method byte 0x"
                        + getHexUIntLowercase(method)
                        + ", previous method byte 0x"
                        + getHexUIntLowercase(codec->getMethodByte()),
                        ErrorCodes::CANNOT_DECOMPRESS);

    size_compressed_without_checksum = ICompressionCodec::readCompressedBlockSize(own_compressed_buffer.data());
    size_decompressed = ICompressionCodec::readDecompressedBlockSize(own_compressed_buffer.data());

    /// This is for clang static analyzer.
    assert(size_decompressed > 0);

    if (size_compressed_without_checksum > DBMS_MAX_COMPRESSED_SIZE)
        throw Exception("Too large size_compressed_without_checksum: "
                        + toString(size_compressed_without_checksum)
                        + ". Most likely corrupted data.",
                        ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

    if (size_compressed_without_checksum < header_size)
        throw Exception("Can't decompress data: the compressed data size (" + toString(size_compressed_without_checksum)
            + ", this should include header size) is less than the header size (" + toString(header_size) + ")", ErrorCodes::CORRUPTED_DATA);

    ProfileEvents::increment(ProfileEvents::ReadCompressedBytes, size_compressed_without_checksum + sizeof(Checksum));

    auto additional_size_at_the_end_of_buffer = codec->getAdditionalSizeAtTheEndOfBuffer();

    /// Is whole compressed block located in 'compressed_in->' buffer?
    if (compressed_in->offset() >= header_size &&
        compressed_in->position() + size_compressed_without_checksum + additional_size_at_the_end_of_buffer  - header_size <= compressed_in->buffer().end())
    {
        compressed_in->position() -= header_size;
        compressed_buffer = compressed_in->position();
        compressed_in->position() += size_compressed_without_checksum;
    }
    else
    {
        own_compressed_buffer.resize(size_compressed_without_checksum + additional_size_at_the_end_of_buffer);
        compressed_buffer = own_compressed_buffer.data();
        compressed_in->readStrict(compressed_buffer + header_size, size_compressed_without_checksum - header_size);
    }

    if (!disable_checksum)
        validateChecksum(compressed_buffer, size_compressed_without_checksum, checksum);

    return size_compressed_without_checksum + sizeof(Checksum);
}


void CompressedReadBufferBase::decompress(char * to, size_t size_decompressed, size_t size_compressed_without_checksum)
{
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBlocks);
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBytes, size_decompressed);

    uint8_t method = ICompressionCodec::readMethod(compressed_buffer);

    if (!codec)
        codec = CompressionCodecFactory::instance().get(method);
    else if (codec->getMethodByte() != method)
        throw Exception("Data compressed with different methods, given method byte "
                        + getHexUIntLowercase(method)
                        + ", previous method byte "
                        + getHexUIntLowercase(codec->getMethodByte()),
                        ErrorCodes::CANNOT_DECOMPRESS);

    codec->decompress(compressed_buffer, size_compressed_without_checksum, to);
}


/// 'compressed_in' could be initialized lazily, but before first call of 'readCompressedData'.
CompressedReadBufferBase::CompressedReadBufferBase(ReadBuffer * in)
    : compressed_in(in), own_compressed_buffer(0)
{
}


CompressedReadBufferBase::~CompressedReadBufferBase() = default;    /// Proper destruction of unique_ptr of forward-declared type.


}

