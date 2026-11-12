#include <city.h>
#include <cstring>
#include <algorithm>

#include <base/types.h>
#include <base/defines.h>

#include <IO/WriteHelpers.h>

#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressedWriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TOO_LARGE_SIZE_COMPRESSED;
}

void CompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    chassert(offset() <= INT_MAX);
    UInt32 decompressed_size = static_cast<UInt32>(offset());
    UInt32 compressed_reserve_size = codec->getCompressedReserveSize(decompressed_size);
    if (uncompressed_codec)
        compressed_reserve_size = std::max(compressed_reserve_size, uncompressed_codec->getCompressedReserveSize(decompressed_size));

    /// The block size is limited by `maxBlockSize`, so this is checked before anything is allocated.
    if (compressed_reserve_size > DBMS_MAX_COMPRESSED_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_SIZE_COMPRESSED,
            "Cannot write a compressed block: {} bytes may compress to {} bytes, the maximum is {}",
            decompressed_size, compressed_reserve_size, DBMS_MAX_COMPRESSED_SIZE);

    /** During compression we need buffer with capacity >= compressed_reserve_size + CHECKSUM_SIZE.
      *
      * If output buffer has necessary capacity, we can compress data directly into the output buffer.
      * Then we can write checksum at the output buffer begin.
      *
      * If output buffer does not have necessary capacity. Compress data into a temporary buffer.
      * Then we can write checksum and copy the temporary buffer into the output buffer.
      */
    const bool compress_in_place = out.available() >= compressed_reserve_size + sizeof(CityHash_v1_0_2::uint128);
    char * compressed_ptr = nullptr;
    if (compress_in_place)
    {
        compressed_ptr = out.position() + sizeof(CityHash_v1_0_2::uint128);
    }
    else
    {
        compressed_buffer.resize(compressed_reserve_size);
        compressed_ptr = compressed_buffer.data();
    }

    UInt32 compressed_size = codec->compress(working_buffer.begin(), decompressed_size, compressed_ptr, codec_state.get());

    /// The block is stored as is if compression saves less than 1/8 of its size.
    if (uncompressed_codec && static_cast<UInt64>(compressed_size) * 8 > static_cast<UInt64>(decompressed_size) * 7)
        compressed_size = uncompressed_codec->compress(working_buffer.begin(), decompressed_size, compressed_ptr);

    CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_ptr, compressed_size);

    writeBinaryLittleEndian(checksum.low64, out);
    writeBinaryLittleEndian(checksum.high64, out);

    if (compress_in_place)
        out.position() += compressed_size;
    else
        out.write(compressed_buffer.data(), compressed_size);

    growAdaptiveBufferAfterFlush();
}

void CompressedWriteBuffer::finalizeImpl()
{
    /// Don't try to resize buffer in nextImpl.
    use_adaptive_buffer_size = false;
    next();
    BufferWithOwnMemory<WriteBuffer>::finalizeImpl();
}

/** The largest block size, not above `buf_size`, that a reader accepts: a block has at most DBMS_MAX_DECOMPRESSED_SIZE bytes
  * and must not compress to more than DBMS_MAX_COMPRESSED_SIZE bytes, which a chain of expanding codecs (e.g. `FPC`) can exceed.
  * The size is limited up front rather than by splitting a block on flush, because marks point inside the current block.
  */
static size_t maxBlockSize(size_t buf_size, const ICompressionCodec & codec)
{
    size_t size = std::min<size_t>(buf_size, DBMS_MAX_DECOMPRESSED_SIZE);
    while (size > 1 && codec.getCompressedReserveSize(static_cast<UInt32>(size)) > DBMS_MAX_COMPRESSED_SIZE)
        size /= 2;
    return size;
}

static CompressionCodecPtr codecOrDefault(CompressionCodecPtr codec)
{
    return codec ? codec : CompressionCodecFactory::instance().getDefaultCodec();
}

CompressedWriteBuffer::CompressedWriteBuffer(
    WriteBuffer & out_, CompressionCodecPtr codec_, size_t buf_size, bool use_adaptive_buffer_size_, size_t adaptive_buffer_initial_size)
    : BufferWithOwnMemory<WriteBuffer>(
        adaptiveBufferInitialSize(use_adaptive_buffer_size_, adaptive_buffer_initial_size, maxBlockSize(buf_size, *codecOrDefault(codec_))))
    , out(out_)
    , codec(codecOrDefault(std::move(codec_)))
    , requested_buf_size(buf_size)
{
    enableAdaptiveBufferGrowth(use_adaptive_buffer_size_, maxBlockSize(buf_size, *codec));
    codec_state = codec->createCompressionState();
}

void CompressedWriteBuffer::cancelImpl() noexcept
{
    BufferWithOwnMemory<WriteBuffer>::cancelImpl();
    out.cancel();
}

void CompressedWriteBuffer::storeIncompressibleBlocksUncompressed()
{
    chassert(!codec->isEncryption());
    uncompressed_codec = CompressionCodecFactory::instance().get("NONE", {});
}

void CompressedWriteBuffer::setCodec(CompressionCodecPtr codec_)
{
    // Flush all the pending data that was supposed to be compressed with the old codec.
    next();
    if (offset() != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompressedWriteBuffer: offset() is not zero");

    chassert(codec_);
    codec = std::move(codec_);
    codec_state = codec->createCompressionState();

    /// The buffer is empty, so it can be resized to the block size limit of the new codec.
    size_t max_size = maxBlockSize(requested_buf_size, *codec);
    adaptive_buffer_max_size = max_size;
    if (memory.size() > max_size || (!use_adaptive_buffer_size && memory.size() != max_size))
        resize(max_size);
}
}
