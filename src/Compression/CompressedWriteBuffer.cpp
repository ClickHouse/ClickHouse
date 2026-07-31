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
}

static_assert(COMPRESSED_BLOCK_PREFIX_SIZE == sizeof(CityHash_v1_0_2::uint128) + ICompressionCodec::getHeaderSize());

void CompressedWriteBuffer::setupBufferForNextBlock()
{
    /** The NONE codec stores data verbatim, so when we exclusively own `out`, the caller can write
      * the data straight into it, right after room reserved for the checksum and the header, and
      * nextImpl only fills those service bytes in place - no copy of the payload.
      *
      * The direct window has to fit a whole block plus the prefix, or the data would be silently
      * re-chunked into frames smaller than the configured block size. Some nested buffers start
      * smaller than requested and grow later (adaptive local and encrypted files, the S3 and Azure
      * blob writers), so the check is repeated for every block: until there is room, the data goes
      * through the owned buffer and is copied on flush.
      */
    if (codec->isNone() && out_buffer_is_exclusive)
    {
        const size_t required_size = block_size + COMPRESSED_BLOCK_PREFIX_SIZE;

        /// A flush (not a finalize) frees the beginning of the nested buffer, but helps only if
        /// that buffer is large enough in the first place.
        if (out.available() < required_size && out.buffer().size() >= required_size)
            out.next();

        if (out.available() >= required_size)
        {
            expected_out_position = out.position();
            char * data_begin = out.position() + COMPRESSED_BLOCK_PREFIX_SIZE;
            working_buffer = Buffer(data_begin, data_begin + block_size);
            pos = working_buffer.begin();
            return;
        }
    }

    if (memory.size() < block_size)
        memory.resize(block_size);

    working_buffer = Buffer(memory.data(), memory.data() + block_size);
    pos = working_buffer.begin();
    expected_out_position = nullptr;
}

void CompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    chassert(offset() <= INT_MAX);
    UInt32 decompressed_size = static_cast<UInt32>(offset());

    /// nextImpl was called because the block is complete, not because of an explicit flush.
    const bool block_is_full = !available();

    if (expected_out_position)
    {
        /// Zero-copy path: the data is already in `out`, after the room reserved for the checksum
        /// and the header; fill them in place and advance `out`.
        if (out.position() != expected_out_position)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CompressedWriteBuffer: the position of the output buffer declared exclusive was moved by someone else, "
                "the block written in place would be corrupted");

        char * header_ptr = out.position() + sizeof(CityHash_v1_0_2::uint128);

        /// NONE stores data as is, so the compressed payload size equals the uncompressed size.
        UInt32 compressed_size = codec->writeHeader(header_ptr, decompressed_size, decompressed_size);

        CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(header_ptr, compressed_size);

        writeBinaryLittleEndian(checksum.low64, out);
        writeBinaryLittleEndian(checksum.high64, out);

        out.position() += compressed_size;

        growBlockSize(block_is_full);
        setupBufferForNextBlock();
        return;
    }

    UInt32 compressed_reserve_size = codec->getCompressedReserveSize(decompressed_size);

    /** During compression we need buffer with capacity >= compressed_reserve_size + CHECKSUM_SIZE.
      *
      * If output buffer has necessary capacity, we can compress data directly into the output buffer.
      * Then we can write checksum at the output buffer begin.
      *
      * If output buffer does not have necessary capacity. Compress data into a temporary buffer.
      * Then we can write checksum and copy the temporary buffer into the output buffer.
      */
    if (out.available() >= compressed_reserve_size + sizeof(CityHash_v1_0_2::uint128))
    {
        char * out_compressed_ptr = out.position() + sizeof(CityHash_v1_0_2::uint128);
        UInt32 compressed_size = codec->compress(working_buffer.begin(), decompressed_size, out_compressed_ptr);

        CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(out_compressed_ptr, compressed_size);

        writeBinaryLittleEndian(checksum.low64, out);
        writeBinaryLittleEndian(checksum.high64, out);

        out.position() += compressed_size;
    }
    else
    {
        compressed_buffer.resize(compressed_reserve_size);
        UInt32 compressed_size = codec->compress(working_buffer.begin(), decompressed_size, compressed_buffer.data());

        CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_buffer.data(), compressed_size);

        writeBinaryLittleEndian(checksum.low64, out);
        writeBinaryLittleEndian(checksum.high64, out);

        out.write(compressed_buffer.data(), compressed_size);
    }

    growBlockSize(block_is_full);

    /// Re-point the working buffer: the nested buffer may have grown in the meantime, so for the
    /// NONE codec the zero-copy path can take over.
    setupBufferForNextBlock();
}

void CompressedWriteBuffer::growBlockSize(bool block_is_full)
{
    if (block_is_full && use_adaptive_buffer_size && block_size < adaptive_buffer_max_size)
        block_size = std::min(block_size * 2, adaptive_buffer_max_size);
}

void CompressedWriteBuffer::finalizeImpl()
{
    /// Don't try to resize buffer in nextImpl.
    use_adaptive_buffer_size = false;
    next();
    BufferWithOwnMemory<WriteBuffer>::finalizeImpl();
}

CompressedWriteBuffer::CompressedWriteBuffer(
    WriteBuffer & out_, CompressionCodecPtr codec_, size_t buf_size, bool use_adaptive_buffer_size_, size_t adaptive_buffer_initial_size)
    /// The adaptive buffer grows from the initial size up to buf_size (the max), so the
    /// initial allocation must not exceed it (see WriteBufferFromFileDescriptor for details).
    : BufferWithOwnMemory<WriteBuffer>(use_adaptive_buffer_size_ ? std::min(adaptive_buffer_initial_size, buf_size) : buf_size)
    , out(out_)
    , codec(std::move(codec_))
    , use_adaptive_buffer_size(use_adaptive_buffer_size_)
    , adaptive_buffer_max_size(buf_size)
    , block_size(use_adaptive_buffer_size_ ? std::min(adaptive_buffer_initial_size, buf_size) : buf_size)
{
    if (!codec)
        codec = CompressionCodecFactory::instance().getDefaultCodec();
}

void CompressedWriteBuffer::declareOutBufferExclusive()
{
    if (count() != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompressedWriteBuffer: the out buffer must be declared exclusive before writing any data");

    out_buffer_is_exclusive = true;
    setupBufferForNextBlock();
}

void CompressedWriteBuffer::cancelImpl() noexcept
{
    BufferWithOwnMemory<WriteBuffer>::cancelImpl();
    out.cancel();
}

void CompressedWriteBuffer::setCodec(CompressionCodecPtr codec_)
{
    // Flush all the pending data that was supposed to be compressed with the old codec.
    next();
    if (offset() != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompressedWriteBuffer: offset() is not zero");

    chassert(codec_);
    codec = std::move(codec_);

    /// Re-point the working buffer for the new codec (directly into `out` for NONE, owned otherwise).
    setupBufferForNextBlock();
}
}
