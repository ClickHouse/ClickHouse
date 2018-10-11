#include <Compression/ICompressionCodec.h>
#include <IO/LZ4_decompress_faster.h>
#include <common/unaligned.h>
#include <IO/CompressedStream.h>
#include <Common/hex.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/typeid_cast.h>
#include <Compression/CompressionFactory.h>
#include <zstd.h>

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
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int TOO_LARGE_SIZE_COMPRESSED;
    extern const int UNKNOWN_COMPRESSION_METHOD;
    extern const int CANNOT_DECOMPRESS;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

CompressionCodecReadBufferPtr ICompressionCodec::liftCompressed(ReadBuffer & origin)
{
    return std::make_shared<CompressionCodecReadBuffer>(origin);
}

CompressionCodecWriteBufferPtr ICompressionCodec::liftCompressed(WriteBuffer & origin)
{
    return std::make_shared<CompressionCodecWriteBuffer>(*this, origin);
}

CompressionCodecReadBuffer::CompressionCodecReadBuffer(ReadBuffer & origin)
    : origin(origin)
{
}

/// Read compressed data into compressed_buffer. Get size of decompressed data from block header. Checksum if need.
/// Returns number of compressed bytes read.
size_t CompressionCodecReadBuffer::readCompressedData(size_t & size_decompressed, size_t & size_compressed)
{
    if (origin.eof())
        return 0;

    CityHash_v1_0_2::uint128 checksum;
    origin.readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));

    own_compressed_buffer.resize(COMPRESSED_BLOCK_HEADER_SIZE);
    origin.readStrict(own_compressed_buffer.data(), COMPRESSED_BLOCK_HEADER_SIZE);

    size_compressed = unalignedLoad<UInt32>(&own_compressed_buffer[1]);
    size_decompressed = unalignedLoad<UInt32>(&own_compressed_buffer[5]);

    if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
        throw Exception("Too large size_compressed: " + toString(size_compressed) + ". Most likely corrupted data.", ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

    ProfileEvents::increment(ProfileEvents::ReadCompressedBytes, size_compressed + sizeof(checksum));

    /// Is whole compressed block located in 'origin' buffer?
    if (origin.offset() >= COMPRESSED_BLOCK_HEADER_SIZE &&
        origin.position() + size_compressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER - COMPRESSED_BLOCK_HEADER_SIZE <= origin.buffer().end())
    {
        origin.position() -= COMPRESSED_BLOCK_HEADER_SIZE;
        compressed_buffer = origin.position();
        origin.position() += size_compressed;
    }
    else
    {
        own_compressed_buffer.resize(size_compressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
        compressed_buffer = own_compressed_buffer.data();
        origin.readStrict(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, size_compressed - COMPRESSED_BLOCK_HEADER_SIZE);
    }

    auto checksum_calculated = CityHash_v1_0_2::CityHash128(compressed_buffer, size_compressed);
    if (checksum != checksum_calculated)
        throw Exception("Checksum doesn't match: corrupted data."
                        " Reference: " + getHexUIntLowercase(checksum.first) + getHexUIntLowercase(checksum.second)
                        + ". Actual: " + getHexUIntLowercase(checksum_calculated.first) + getHexUIntLowercase(checksum_calculated.second)
                        + ". Size of compressed block: " + toString(size_compressed) + ".",
                        ErrorCodes::CHECKSUM_DOESNT_MATCH);

    return size_compressed + sizeof(checksum);
}

void CompressionCodecReadBuffer::decompress(char * to, size_t size_decompressed, size_t size_compressed_without_checksum)
{
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBlocks);
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBytes, size_decompressed);

    UInt8 current_method = compressed_buffer[0];    /// See CompressedWriteBuffer.h
    if (current_method != method)
        codec =  CompressionCodecFactory::instance().get(method);

    codec->decompress(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE,
        size_compressed_without_checksum - COMPRESSED_BLOCK_HEADER_SIZE, to, size_decompressed);
}

bool CompressionCodecReadBuffer::nextImpl()
{
    size_t size_decompressed;
    size_t size_compressed_without_checksum;

    size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum);
    if (!size_compressed)
        return false;

    memory.resize(size_decompressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
    working_buffer = Buffer(memory.data(), &memory[size_decompressed]);

    decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

    return true;
}

void CompressionCodecReadBuffer::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    if (const auto file_in = dynamic_cast<ReadBufferFromFileBase *>(&origin))
    {
        if (size_compressed &&
            offset_in_compressed_file == file_in->getPositionInFile() - size_compressed &&
            offset_in_decompressed_block <= working_buffer.size())
        {
            bytes += offset();
            pos = working_buffer.begin() + offset_in_decompressed_block;
            /// `bytes` can overflow and get negative, but in `count()` everything will overflow back and get right.
            bytes -= offset();
        }
        else
        {
            file_in->seek(offset_in_compressed_file);

            bytes += offset();
            nextImpl();

            if (offset_in_decompressed_block > working_buffer.size())
                throw Exception("Seek position is beyond the decompressed block"
                                " (pos: " + toString(offset_in_decompressed_block) + ", block size: " + toString(working_buffer.size()) + ")",
                                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

            pos = working_buffer.begin() + offset_in_decompressed_block;
            bytes -= offset();
        }
    }
    else
        throw Exception("CompressionCodec: cannot seek in non-file buffer", ErrorCodes::LOGICAL_ERROR);
}

CompressionCodecWriteBuffer::~CompressionCodecWriteBuffer()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void CompressionCodecWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

    size_t uncompressed_size = offset();
    size_t compressed_reserve_size = compression_codec.getCompressedReserveSize(uncompressed_size);

    compressed_buffer.resize(header_size + compressed_reserve_size);
    compressed_buffer[0] = compression_codec.getMethodByte();
    size_t compressed_size = header_size + compression_codec.compress(working_buffer.begin(), uncompressed_size, &compressed_buffer[header_size]);

    UInt32 compressed_size_32 = compressed_size;
    UInt32 uncompressed_size_32 = uncompressed_size;
    unalignedStore(&compressed_buffer[1], compressed_size_32);
    unalignedStore(&compressed_buffer[5], uncompressed_size_32);
    CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_buffer.data(), compressed_size);
    out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));
    out.write(compressed_buffer.data(), compressed_size);
}

CompressionCodecWriteBuffer::CompressionCodecWriteBuffer(ICompressionCodec & compression_codec, WriteBuffer & out, size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size), out(out), compression_codec(compression_codec)
{
}

}
