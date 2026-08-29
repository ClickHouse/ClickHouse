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

namespace
{
    /// Resolved here rather than in the constructor body: the cap needs the codec, and the base
    /// class initializer consumes buf_size before the body runs.
    CompressionCodecPtr resolveCodec(CompressionCodecPtr codec)
    {
        return codec ? codec : CompressionCodecFactory::instance().getDefaultCodec();
    }

    /// A frame's size_decompressed is offset(), so buffer capacity is its upper bound.
    size_t capBufferSize(const CompressionCodecPtr & codec, size_t buf_size)
    {
        return resolveCodec(codec)->capUncompressedFrameSize(buf_size);
    }
}

void CompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    chassert(offset() <= INT_MAX);
    UInt32 decompressed_size = static_cast<UInt32>(offset());
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

    growAdaptiveBufferAfterFlush();
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
    /// The adaptive buffer grows from the initial size up to the max, so the initial allocation must
    /// not exceed it (see WriteBufferFromFileDescriptor for details). Both bounds are capped by what
    /// the codec can describe in a frame header.
    : BufferWithOwnMemory<WriteBuffer>(
        adaptiveBufferInitialSize(use_adaptive_buffer_size_, adaptive_buffer_initial_size, capBufferSize(codec_, buf_size)))
    , out(out_)
    , codec(resolveCodec(std::move(codec_)))
{
    enableAdaptiveBufferGrowth(use_adaptive_buffer_size_, capBufferSize(codec, buf_size));
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

    /// The capacity was sized for the previous codec, whose bound may be looser than this one's.
    adaptive_buffer_max_size = codec->capUncompressedFrameSize(adaptive_buffer_max_size);
    if (memory.size() > adaptive_buffer_max_size)
    {
        memory.resize(adaptive_buffer_max_size);
        BufferBase::set(memory.data(), memory.size(), 0);
    }
}
}
