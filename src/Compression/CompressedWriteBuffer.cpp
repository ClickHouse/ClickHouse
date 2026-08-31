#include <city.h>
#include <cstring>
#include <algorithm>

#include <base/types.h>
#include <base/defines.h>
#include <base/unaligned.h>

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

void CompressedWriteBuffer::setupBufferForNextBlock()
{
    static_assert(COMPRESSED_BLOCK_PREFIX_SIZE == sizeof(CityHash_v1_0_2::uint128) + ICompressionCodec::getHeaderSize());

    /// Write directly into exclusive `out` for `NONE` when a whole frame fits.
    if (codec->isNone() && out_buffer_is_exclusive)
    {
        const size_t required_size = block_size + COMPRESSED_BLOCK_PREFIX_SIZE;

        if (out.available() < required_size && out.buffer().size() >= required_size)
            out.next();

        if (out.available() >= required_size)
        {
            char * data_begin = out.position() + COMPRESSED_BLOCK_PREFIX_SIZE;
            working_buffer = Buffer(data_begin, data_begin + block_size);
            pos = working_buffer.begin();
            block_is_written_in_place = true;
            return;
        }
    }

    if (memory.size() < block_size)
        memory.resize(block_size);

    working_buffer = Buffer(memory.data(), memory.data() + block_size);
    pos = working_buffer.begin();
    block_is_written_in_place = false;
}

void CompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    chassert(offset() <= INT_MAX);
    UInt32 decompressed_size = static_cast<UInt32>(offset());

    if (block_is_written_in_place)
    {
        /// The frame sits right in front of the working buffer, inside `out`, which is exclusively
        /// ours (see declareOutBufferExclusive), so `out` has not moved since the buffer was set up.
        /// Checked in every build, not only in debug: the working buffer is an alias into `out`, and
        /// wrappers such as `HashingWriteBuffer` hand that alias further out, so a foreign write or
        /// flush would otherwise be committed to disk as a well-formed block of wrong bytes. Failing
        /// here cancels the whole chain (see `WriteBuffer::next`), so nothing incorrect is written.
        char * frame_begin = working_buffer.begin() - COMPRESSED_BLOCK_PREFIX_SIZE;
        if (out.position() != frame_begin)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The output buffer of CompressedWriteBuffer was declared exclusive, but it was written to or flushed "
                "by someone else while a block was assembled in place");

        char * header_ptr = frame_begin + sizeof(CityHash_v1_0_2::uint128);

        UInt32 compressed_size = codec->writeHeader(header_ptr, decompressed_size, decompressed_size);

        CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(header_ptr, compressed_size);

        /// Raw stores: the checksum goes in front of a payload that is already in place, so `out`
        /// must not be flushed in between, as a buffered write could do when the frame ends the buffer.
        unalignedStoreLittleEndian<UInt64>(frame_begin, checksum.low64);
        unalignedStoreLittleEndian<UInt64>(frame_begin + sizeof(UInt64), checksum.high64);

        out.position() = pos;
    }
    else
    {
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
    }

    /// Grow only after a full adaptive block.
    if (!available() && use_adaptive_buffer_size && block_size < adaptive_buffer_max_size)
        block_size = std::min(block_size * 2, adaptive_buffer_max_size);

    setupBufferForNextBlock();
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
    : BufferWithOwnMemory<WriteBuffer>(adaptiveBufferInitialSize(use_adaptive_buffer_size_, adaptive_buffer_initial_size, buf_size))
    , out(out_)
    , codec(std::move(codec_))
    , block_size(memory.size())
{
    enableAdaptiveBufferGrowth(use_adaptive_buffer_size_, buf_size);
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

    setupBufferForNextBlock();
}
}
