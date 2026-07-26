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
        /** The direct window has to hold a whole block plus the service bytes in front of it.
          * If we settled for less, the block would end as soon as the window is full, and the data
          * would be silently re-chunked into frames smaller than the configured block size.
          *
          * We cannot know this in advance from the caller side: some nested buffers start smaller
          * than the size requested from `writeFile` and grow only later - local and encrypted files
          * with `use_adaptive_write_buffer`, and the S3 and Azure blob write buffers, which start at
          * `min(buf_size, DBMS_DEFAULT_BUFFER_SIZE)`. Hence the check is repeated for every block:
          * until the nested buffer is large enough we use the owned buffer and copy on flush (which
          * streams a full block across several `out.write` calls, honoring the block size), and the
          * zero-copy path turns on by itself as soon as there is room for a full block.
          */
        const size_t required_size = block_size + COMPRESSED_BLOCK_PREFIX_SIZE;

        /// Flushing helps only if the nested buffer is large enough in the first place.
        /// next() here is a flush, not a finalize, so it is safe even while several
        /// CompressedWriteBuffers share one `out`.
        if (out.available() < required_size && out.buffer().size() >= required_size)
            out.next();

        if (out.available() >= required_size)
        {
            char * data_begin = out.position() + COMPRESSED_BLOCK_PREFIX_SIZE;
            working_buffer = Buffer(data_begin, data_begin + block_size);
            pos = working_buffer.begin();
            current_block_is_direct = true;
            return;
        }
    }

    if (memory.size() < block_size)
        memory.resize(block_size);

    working_buffer = Buffer(memory.data(), memory.data() + block_size);
    pos = working_buffer.begin();
    current_block_is_direct = false;
}

void CompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    chassert(offset() <= INT_MAX);
    UInt32 decompressed_size = static_cast<UInt32>(offset());

    /// nextImpl was called because the block is complete, not because of an explicit flush.
    const bool block_is_full = !available();

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

    /// This block went through the owned buffer - either because the codec is not NONE, or because
    /// `out` didn't have enough room for the zero-copy path. Re-point the working buffer: for the
    /// NONE codec the nested buffer may have grown in the meantime, so the direct path can take over.
    setupBufferForNextBlock();
}

void CompressedWriteBuffer::growBlockSize(bool block_is_full)
{
    /// Increase the block size if the adaptive buffer size is used and nextImpl was called because
    /// the block is complete (and not because of an explicit flush of a partially filled block).
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
    , block_size(use_adaptive_buffer_size_ ? std::min(adaptive_buffer_initial_size, buf_size) : buf_size)
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
