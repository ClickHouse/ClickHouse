#include "CompressedReadBufferBase.h"

#include <bit>
#include <cstring>
#include <cassert>
#include <city.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <base/hex.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/BufferWithOwnMemory.h>
#include <Compression/CompressionInfo.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


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
static void validateChecksum(char * data, size_t size, const Checksum expected_checksum, bool external_data)
{
    auto calculated_checksum = CityHash_v1_0_2::CityHash128(data, size);
    if (expected_checksum == calculated_checksum)
        return;

    WriteBufferFromOwnString message;

    /// TODO mess up of endianness in error message.
    message << "Checksum doesn't match: corrupted data."
        " Reference: " + getHexUIntLowercase(expected_checksum)
        + ". Actual: " + getHexUIntLowercase(calculated_checksum)
        + ". Size of compressed block: " + toString(size);

    const char * message_hardware_failure = "This is most likely due to hardware failure. "
                                            "If you receive broken data over network and the error does not repeat every time, "
                                            "this can be caused by bad RAM on network interface controller or bad controller itself "
                                            "or bad RAM on network switches or bad CPU on network switches "
                                            "(look at the logs on related network switches; note that TCP checksums don't help) "
                                            "or bad RAM on host (look at dmesg or kern.log for enormous amount of EDAC errors, "
                                            "ECC-related reports, Machine Check Exceptions, mcelog; note that ECC memory can fail "
                                            "if the number of errors is huge) or bad CPU on host. If you read data from disk, "
                                            "this can be caused by disk bit rot. This exception protects ClickHouse "
                                            "from data corruption due to hardware failures.";

    int error_code = external_data ? ErrorCodes::CANNOT_DECOMPRESS : ErrorCodes::CHECKSUM_DOESNT_MATCH;

    auto flip_bit = [](char * buf, size_t pos)
    {
        buf[pos / 8] ^= 1 << pos % 8;
    };

    /// If size is too huge, then this may be caused by corruption.
    /// And anyway this is pretty heavy, so avoid burning too much CPU here.
    if (size < (1ULL << 20))
    {
        /// We need to copy data from ReadBuffer to flip bits as ReadBuffer should be immutable
        PODArray<char> tmp_buffer(data, data + size);
        char * tmp_data = tmp_buffer.data();

        /// Check if the difference caused by single bit flip in data.
        for (size_t bit_pos = 0; bit_pos < size * 8; ++bit_pos)
        {
            flip_bit(tmp_data, bit_pos);

            auto checksum_of_data_with_flipped_bit = CityHash_v1_0_2::CityHash128(tmp_data, size);
            if (expected_checksum == checksum_of_data_with_flipped_bit)
            {
                message << ". The mismatch is caused by single bit flip in data block at byte " << (bit_pos / 8) << ", bit " << (bit_pos % 8) << ". "
                    << message_hardware_failure;
                throw Exception::createDeprecated(message.str(), error_code);
            }

            flip_bit(tmp_data, bit_pos);    /// Restore
        }
    }

    /// Check if the difference caused by single bit flip in stored checksum.
    size_t difference = std::popcount(expected_checksum.low64 ^ calculated_checksum.low64)
        + std::popcount(expected_checksum.high64 ^ calculated_checksum.high64);

    if (difference == 1)
    {
        message << ". The mismatch is caused by single bit flip in checksum. "
            << message_hardware_failure;
        throw Exception::createDeprecated(message.str(), error_code);
    }

    throw Exception::createDeprecated(message.str(), error_code);
}

static void readHeaderAndGetCodecAndSize(
    const char * compressed_buffer,
    UInt8 header_size,
    CompressionCodecPtr & codec,
    size_t & size_decompressed,
    size_t & size_compressed_without_checksum,
    bool allow_different_codecs,
    bool external_data)
{
    uint8_t method = ICompressionCodec::readMethod(compressed_buffer);

    if (!codec)
    {
        codec = CompressionCodecFactory::instance().get(method);
    }
    else if (method != codec->getMethodByte())
    {
        if (allow_different_codecs)
        {
            codec = CompressionCodecFactory::instance().get(method);
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Data compressed with different methods, given method "
                            "byte 0x{}, previous method byte 0x{}",
                            getHexUIntLowercase(method), getHexUIntLowercase(codec->getMethodByte()));
        }
    }

    if (external_data)
        codec->setExternalDataFlag();

    size_compressed_without_checksum = codec->readCompressedBlockSize(compressed_buffer);
    size_decompressed = codec->readDecompressedBlockSize(compressed_buffer);

    /// This is for clang static analyzer.
    assert(size_decompressed > 0);

    if (size_compressed_without_checksum > DBMS_MAX_COMPRESSED_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_SIZE_COMPRESSED, "Too large size_compressed_without_checksum: {}. "
                        "Most likely corrupted data.", size_compressed_without_checksum);

    if (size_compressed_without_checksum < header_size)
        throw Exception(external_data ? ErrorCodes::CANNOT_DECOMPRESS : ErrorCodes::CORRUPTED_DATA, "Can't decompress data: "
            "the compressed data size ({}, this should include header size) is less than the header size ({})",
            size_compressed_without_checksum, static_cast<size_t>(header_size));
}

/// Read compressed data into compressed_buffer. Get size of decompressed data from block header. Checksum if need.
/// Returns number of compressed bytes read.
size_t CompressedReadBufferBase::readCompressedData(size_t & size_decompressed, size_t & size_compressed_without_checksum, bool always_copy)
{
    if (compressed_in->eof())
        return 0;

    UInt8 header_size = ICompressionCodec::getHeaderSize();
    own_compressed_buffer.resize(header_size + sizeof(Checksum));

    compressed_in->readStrict(own_compressed_buffer.data(), sizeof(Checksum) + header_size);

    readHeaderAndGetCodecAndSize(
        own_compressed_buffer.data() + sizeof(Checksum),
        header_size,
        codec,
        size_decompressed,
        size_compressed_without_checksum,
        allow_different_codecs,
        external_data);

    auto additional_size_at_the_end_of_buffer = codec->getAdditionalSizeAtTheEndOfBuffer();

    /// Is whole compressed block located in 'compressed_in->' buffer?
    if (!always_copy &&
        compressed_in->offset() >= header_size + sizeof(Checksum) &&
        compressed_in->available() >= (size_compressed_without_checksum - header_size) + additional_size_at_the_end_of_buffer + sizeof(Checksum))
    {
        compressed_in->position() -= header_size;
        compressed_buffer = compressed_in->position();
        compressed_in->position() += size_compressed_without_checksum;
    }
    else
    {
        own_compressed_buffer.resize(sizeof(Checksum) + size_compressed_without_checksum + additional_size_at_the_end_of_buffer);
        compressed_buffer = own_compressed_buffer.data() + sizeof(Checksum);
        compressed_in->readStrict(compressed_buffer + header_size, size_compressed_without_checksum - header_size);
    }

    if (!disable_checksum)
    {
        Checksum checksum;
        ReadBufferFromMemory checksum_in(own_compressed_buffer.data(), sizeof(checksum));
        readBinaryLittleEndian(checksum.low64, checksum_in);
        readBinaryLittleEndian(checksum.high64, checksum_in);

        validateChecksum(compressed_buffer, size_compressed_without_checksum, checksum, external_data);
    }

    ProfileEvents::increment(ProfileEvents::ReadCompressedBytes, size_compressed_without_checksum + sizeof(Checksum));
    return size_compressed_without_checksum + sizeof(Checksum);
}

/// Read compressed data into compressed_buffer for asynchronous decompression to avoid the situation of "read compressed block across the compressed_in".
size_t CompressedReadBufferBase::readCompressedDataBlockForAsynchronous(size_t & size_decompressed, size_t & size_compressed_without_checksum)
{
    UInt8 header_size = ICompressionCodec::getHeaderSize();
    /// Make sure the whole header located in 'compressed_in->' buffer.
    if (compressed_in->eof() || (compressed_in->available() < (header_size + sizeof(Checksum))))
        return 0;

    own_compressed_buffer.resize(header_size + sizeof(Checksum));
    compressed_in->readStrict(own_compressed_buffer.data(), sizeof(Checksum) + header_size);

    readHeaderAndGetCodecAndSize(
        own_compressed_buffer.data() + sizeof(Checksum),
        header_size,
        codec,
        size_decompressed,
        size_compressed_without_checksum,
        allow_different_codecs,
        external_data);

    auto additional_size_at_the_end_of_buffer = codec->getAdditionalSizeAtTheEndOfBuffer();

    /// Make sure the whole compressed block located in 'compressed_in->' buffer.
    /// Otherwise, abandon header and restore original offset of compressed_in
    if (compressed_in->offset() >= header_size + sizeof(Checksum) &&
        compressed_in->available() >= (size_compressed_without_checksum - header_size) + additional_size_at_the_end_of_buffer + sizeof(Checksum))
    {
        compressed_in->position() -= header_size;
        compressed_buffer = compressed_in->position();
        compressed_in->position() += size_compressed_without_checksum;

        if (!disable_checksum)
        {
            Checksum checksum;
            ReadBufferFromMemory checksum_in(own_compressed_buffer.data(), sizeof(checksum));
            readBinaryLittleEndian(checksum.low64, checksum_in);
            readBinaryLittleEndian(checksum.high64, checksum_in);

            validateChecksum(compressed_buffer, size_compressed_without_checksum, checksum, external_data);
        }

        ProfileEvents::increment(ProfileEvents::ReadCompressedBytes, size_compressed_without_checksum + sizeof(Checksum));
        return size_compressed_without_checksum + sizeof(Checksum);
    }

    compressed_in->position() -= (sizeof(Checksum) + header_size);
    return 0;
}

static void readHeaderAndGetCodec(const char * compressed_buffer, size_t size_decompressed, CompressionCodecPtr & codec,
                                  bool allow_different_codecs, bool external_data)
{
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBlocks);
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBytes, size_decompressed);

    uint8_t method = ICompressionCodec::readMethod(compressed_buffer);

    if (!codec)
    {
        codec = CompressionCodecFactory::instance().get(method);
    }
    else if (codec->getMethodByte() != method)
    {
        if (allow_different_codecs)
        {
            codec = CompressionCodecFactory::instance().get(method);
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Data compressed with different methods, given method "
                            "byte 0x{}, previous method byte 0x{}",
                            getHexUIntLowercase(method), getHexUIntLowercase(codec->getMethodByte()));
        }
    }

    if (external_data)
        codec->setExternalDataFlag();
}

void CompressedReadBufferBase::decompressTo(char * to, size_t size_decompressed, size_t size_compressed_without_checksum)
{
    readHeaderAndGetCodec(compressed_buffer, size_decompressed, codec, allow_different_codecs, external_data);
    codec->decompress(compressed_buffer, static_cast<UInt32>(size_compressed_without_checksum), to);
}

void CompressedReadBufferBase::decompress(BufferBase::Buffer & to, size_t size_decompressed, size_t size_compressed_without_checksum)
{
    readHeaderAndGetCodec(compressed_buffer, size_decompressed, codec, allow_different_codecs, external_data);

    if (codec->isNone())
    {
        /// Shortcut for NONE codec to avoid extra memcpy.
        /// We doing it by changing the buffer `to` to point to existing uncompressed data.

        UInt8 header_size = ICompressionCodec::getHeaderSize();
        if (size_compressed_without_checksum < header_size)
            throw Exception(external_data ? ErrorCodes::CANNOT_DECOMPRESS : ErrorCodes::CORRUPTED_DATA,
                "Can't decompress data: the compressed data size ({}, this should include header size) is less than the header size ({})",
                    size_compressed_without_checksum, static_cast<size_t>(header_size));

        to = BufferBase::Buffer(compressed_buffer + header_size, compressed_buffer + size_compressed_without_checksum);
    }
    else
        codec->decompress(compressed_buffer, static_cast<UInt32>(size_compressed_without_checksum), to.begin());
}

/// 'compressed_in' could be initialized lazily, but before first call of 'readCompressedData'.
CompressedReadBufferBase::CompressedReadBufferBase(ReadBuffer * in, bool allow_different_codecs_, bool external_data_)
    : compressed_in(in), own_compressed_buffer(0), allow_different_codecs(allow_different_codecs_), external_data(external_data_)
{
}


CompressedReadBufferBase::~CompressedReadBufferBase() = default; /// Proper destruction of unique_ptr of forward-declared type.

}
