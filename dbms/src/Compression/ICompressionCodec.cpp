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

static constexpr auto CHECKSUM_SIZE{sizeof(CityHash_v1_0_2::uint128)};

UInt32 ICompressionCodec::compress(char * source, UInt32 source_size, char * dest) const
{
    dest[0] = getMethodByte();
    UInt8 header_size = getHeaderSize();
    /// Write data from header_size
    UInt32 compressed_bytes_written = doCompressData(source, source_size, &dest[header_size]);
    unalignedStore<UInt32>(&dest[1], compressed_bytes_written + header_size);
    unalignedStore<UInt32>(&dest[5], source_size);
    return header_size + compressed_bytes_written;
}


UInt32 ICompressionCodec::decompress(char * source, UInt32 source_size, char * dest) const
{
    UInt8 method = source[0];
    if (method != getMethodByte())
        throw Exception("Can't decompress data with codec byte " + toString(method) + " from codec with byte " + toString(method), ErrorCodes::CANNOT_DECOMPRESS);

    UInt8 header_size = getHeaderSize();
    UInt32 decompressed_size = unalignedLoad<UInt32>(&source[5]);
    doDecompressData(&source[header_size], source_size - header_size, dest, decompressed_size);
    return decompressed_size;

}

UInt32 ICompressionCodec::readCompressedBlockSize(const char * source)
{
    return unalignedLoad<UInt32>(&source[1]);
}


UInt32 ICompressionCodec::readDecompressedBlockSize(const char * source)
{
    return unalignedLoad<UInt32>(&source[5]);
}


UInt8 ICompressionCodec::readMethod(const char * source)
{
    return static_cast<UInt8>(source[0]);
}

CompressionCodecReadBufferPtr liftCompressed(CompressionCodecPtr codec, ReadBuffer & origin)
{
    return std::make_shared<CompressionCodecReadBuffer>(codec, origin);
}

CompressionCodecWriteBufferPtr liftCompressed(CompressionCodecPtr codec, WriteBuffer & origin)
{
    return std::make_shared<CompressionCodecWriteBuffer>(codec, origin);
}

CompressionCodecReadBuffer::CompressionCodecReadBuffer(CompressionCodecPtr codec_, ReadBuffer & origin_)
    : codec(codec_)
    , origin(origin_)
{
}


/// Read compressed data into compressed_buffer. Get size of decompressed data from block header. Checksum if need.
/// Returns number of compressed bytes read.
std::pair<UInt32, UInt32> CompressionCodecReadBuffer::readCompressedData()
{
    if (origin.eof())
        return std::make_pair(0, 0);

    CityHash_v1_0_2::uint128 checksum;
    origin.readStrict(reinterpret_cast<char *>(&checksum), CHECKSUM_SIZE);

    UInt8 header_size = ICompressionCodec::getHeaderSize();
    own_compressed_buffer.resize(header_size);
    origin.readStrict(own_compressed_buffer.data(), header_size);

    UInt8 method = ICompressionCodec::readMethod(own_compressed_buffer.data());

    if (method != codec->getMethodByte())
        throw Exception("Can't decompress with method " + getHexUIntLowercase(method) + ", with codec " + getHexUIntLowercase(codec->getMethodByte()), ErrorCodes::CANNOT_DECOMPRESS);

    UInt32 size_to_read_compressed = ICompressionCodec::readCompressedBlockSize(own_compressed_buffer.data());
    UInt32 size_decompressed = ICompressionCodec::readDecompressedBlockSize(own_compressed_buffer.data());

    if (size_to_read_compressed > DBMS_MAX_COMPRESSED_SIZE)
        throw Exception("Too large size_to_read_compressed: " + toString(size_to_read_compressed) + ". Most likely corrupted data.", ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

    ProfileEvents::increment(ProfileEvents::ReadCompressedBytes, size_to_read_compressed + CHECKSUM_SIZE);

    /// Is whole compressed block located in 'origin' buffer?
    if (origin.offset() >= header_size &&
        origin.position() + size_to_read_compressed + codec->getAdditionalSizeAtTheEndOfBuffer()  - header_size <= origin.buffer().end())
    {
        origin.position() -= header_size;
        compressed_buffer = origin.position();
        origin.position() += size_to_read_compressed;
    }
    else
    {
        own_compressed_buffer.resize(size_to_read_compressed + codec->getAdditionalSizeAtTheEndOfBuffer());
        compressed_buffer = own_compressed_buffer.data();
        origin.readStrict(compressed_buffer + header_size, size_to_read_compressed - header_size);
    }

    auto checksum_calculated = CityHash_v1_0_2::CityHash128(compressed_buffer, size_to_read_compressed);
    if (checksum != checksum_calculated)
        throw Exception("Checksum doesn't match: corrupted data."
                        " Reference: " + getHexUIntLowercase(checksum.first) + getHexUIntLowercase(checksum.second)
                        + ". Actual: " + getHexUIntLowercase(checksum_calculated.first) + getHexUIntLowercase(checksum_calculated.second)
                        + ". Size of compressed block: " + toString(size_to_read_compressed),
                        ErrorCodes::CHECKSUM_DOESNT_MATCH);

    return std::make_pair(size_to_read_compressed, size_decompressed);
}

void CompressionCodecReadBuffer::decompress(char * to, UInt32 size_compressed)
{
    UInt8 method = ICompressionCodec::readMethod(compressed_buffer);

    if (!codec)
        codec = CompressionCodecFactory::instance().get(method);
    else if (codec->getMethodByte() != method)
        throw Exception("Can't decompress data with byte " + getHexUIntLowercase(method) + " expected byte " + getHexUIntLowercase(codec->getMethodByte()), ErrorCodes::CANNOT_DECOMPRESS);

    codec->decompress(compressed_buffer, size_compressed, to);
}

bool CompressionCodecReadBuffer::nextImpl()
{
    UInt32 size_decompressed;

    std::tie(read_compressed_bytes_for_last_time, size_decompressed) = readCompressedData();
    if (!read_compressed_bytes_for_last_time)
        return false;

    memory.resize(size_decompressed + codec->getAdditionalSizeAtTheEndOfBuffer());
    working_buffer = Buffer(memory.data(), &memory[size_decompressed]);

    decompress(working_buffer.begin(), read_compressed_bytes_for_last_time);

    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBytes, size_decompressed);
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBlocks);

    return true;
}

void CompressionCodecReadBuffer::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    if (const auto file_in = dynamic_cast<ReadBufferFromFileBase *>(&origin))
    {
        UInt32 readed_size_with_checksum = read_compressed_bytes_for_last_time + CHECKSUM_SIZE;
        UInt32 last_readed_block_start_pos = file_in->getPositionInFile() - readed_size_with_checksum;
        /// We seek in already uncompressed block
        if (readed_size_with_checksum && /// we already have read something
            offset_in_compressed_file == last_readed_block_start_pos && /// our position is exactly at required byte
            offset_in_decompressed_block <= working_buffer.size()) /// our buffer size is more, than required position in uncompressed block
        {
            bytes += offset();
            pos = working_buffer.begin() + offset_in_decompressed_block;
            /// `bytes` can overflow and get negative, but in `count()` everything will overflow back and get right.
            bytes -= offset();
        }
        else /// or we have to read and uncompress further
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

    size_t decompressed_size = offset();
    UInt32 compressed_reserve_size = codec->getCompressedReserveSize(decompressed_size);
    compressed_buffer.resize(compressed_reserve_size);
    UInt32 compressed_size = codec->compress(working_buffer.begin(), decompressed_size, compressed_buffer.data());

    CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_buffer.data(), compressed_size);
    out.write(reinterpret_cast<const char *>(&checksum), CHECKSUM_SIZE);
    out.write(compressed_buffer.data(), compressed_size);
}

CompressionCodecWriteBuffer::CompressionCodecWriteBuffer(CompressionCodecPtr codec_, WriteBuffer & out_, size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size), out(out_), codec(codec_)
{
}

}
