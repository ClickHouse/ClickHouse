#include <city.h>
#include <cstring>
#include <algorithm>

#include <base/types.h>
#include <base/defines.h>

#include <IO/WriteHelpers.h>

#include <Compression/CompressionFactory.h>
#include <Compression/CompressedWriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/// Service bytes that precede the data of every compressed block on disk: the checksum followed
/// by the compression header (method byte + compressed size + uncompressed size).
static constexpr size_t COMPRESSED_BLOCK_PREFIX_SIZE
    = sizeof(CityHash_v1_0_2::uint128) + ICompressionCodec::getHeaderSize();

void CompressedWriteBuffer::setupBufferForNextBlock()
{
    /** The NONE codec stores data verbatim, so there is no need to route it through a separate
      * uncompressed buffer: we can let the caller write the data straight into the output buffer,
      * leaving room for the checksum and the header in front of it. Then nextImpl() only has to
      * fill in those service bytes in place, without copying the payload.
      *
      * This requires `out` to expose enough contiguous free space. When it does not (e.g. `out`
      * has a tiny buffer), we fall back to the owned buffer and copy the data on flush.
      *
      * It also requires that we exclusively own `out`: we keep a window of out's buffer reserved
      * across the caller's writes, which is only valid if nothing else moves out.position() in the
      * meantime. Hence the path is gated on `out_buffer_is_exclusive`.
      */
    if (codec->isNone() && out_buffer_is_exclusive)
    {
        /// Refresh the output buffer if it can't hold the service bytes plus at least one data byte.
        /// next() here is a flush, not a finalize, so it is safe even while several
        /// CompressedWriteBuffers share one `out`.
        if (out.available() < COMPRESSED_BLOCK_PREFIX_SIZE + 1)
            out.next();

        if (out.available() >= COMPRESSED_BLOCK_PREFIX_SIZE + 1)
        {
            size_t window = std::min(out.available() - COMPRESSED_BLOCK_PREFIX_SIZE, adaptive_buffer_max_size);
            char * data_begin = out.position() + COMPRESSED_BLOCK_PREFIX_SIZE;
            working_buffer = Buffer(data_begin, data_begin + window);
            pos = working_buffer.begin();
            current_block_is_direct = true;
            return;
        }
    }

    working_buffer = Buffer(memory.data(), memory.data() + memory.size());
    pos = working_buffer.begin();
    current_block_is_direct = false;
}

void CompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    chassert(offset() <= INT_MAX);
    UInt32 decompressed_size = static_cast<UInt32>(offset());

    if (current_block_is_direct)
    {
        /// Zero-copy path for the NONE codec: the data has already been written by the caller
        /// directly into `out`, right after the space reserved for the checksum and the header.
        /// We only fill in the header and the checksum in place and advance `out`.
        char * header_ptr = out.position() + sizeof(CityHash_v1_0_2::uint128);
        chassert(working_buffer.begin() == header_ptr + ICompressionCodec::getHeaderSize());

        /// NONE stores data as is, so the compressed payload size equals the uncompressed size.
        UInt32 compressed_size = codec->writeHeader(header_ptr, decompressed_size, decompressed_size);

        CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(header_ptr, compressed_size);

        writeBinaryLittleEndian(checksum.low64, out);
        writeBinaryLittleEndian(checksum.high64, out);

        out.position() += compressed_size;

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

    if (codec->isNone() && out_buffer_is_exclusive)
    {
        /// This block went through the owned buffer because `out` didn't have enough room for the
        /// zero-copy path; try to switch back to writing directly into `out` for the next block.
        setupBufferForNextBlock();
    }
    else if (!available() && use_adaptive_buffer_size && memory.size() < adaptive_buffer_max_size)
    {
        /// Increase buffer size for next data if adaptive buffer size is used and nextImpl was called because of end of buffer.
        memory.resize(std::min(memory.size() * 2, adaptive_buffer_max_size));
        BufferBase::set(memory.data(), memory.size(), 0);
    }
}

void CompressedWriteBuffer::finalizeImpl()
{
    /// Don't try to resize buffer in nextImpl.
    use_adaptive_buffer_size = false;
    next();
    BufferWithOwnMemory<WriteBuffer>::finalizeImpl();
}

CompressedWriteBuffer::CompressedWriteBuffer(
    WriteBuffer & out_, CompressionCodecPtr codec_, size_t buf_size, bool use_adaptive_buffer_size_, size_t adaptive_buffer_initial_size,
    bool out_buffer_is_exclusive_)
    /// The adaptive buffer grows from the initial size up to buf_size (the max), so the
    /// initial allocation must not exceed it (see WriteBufferFromFileDescriptor for details).
    : BufferWithOwnMemory<WriteBuffer>(use_adaptive_buffer_size_ ? std::min(adaptive_buffer_initial_size, buf_size) : buf_size)
    , out(out_)
    , codec(std::move(codec_))
    , out_buffer_is_exclusive(out_buffer_is_exclusive_)
    , use_adaptive_buffer_size(use_adaptive_buffer_size_)
    , adaptive_buffer_max_size(buf_size)
{
    if (!codec)
        codec = CompressionCodecFactory::instance().getDefaultCodec();

    /// Point the working buffer at `out` for the zero-copy NONE path when possible.
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
