#include <city.h>
#include <cstring>

#include <base/types.h>
#include <base/unaligned.h>
#include <base/defines.h>

#include <IO/WriteHelpers.h>

#include <Compression/CompressionFactory.h>
#include <Compression/CompressedWriteBuffer.h>


namespace DB
{

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

    /// Increase buffer size for next data if adaptive buffer size is used and nextImpl was called because of end of buffer.
    if (!available() && use_adaptive_buffer_size && memory.size() < adaptive_buffer_max_size)
    {
        memory.resize(std::min(memory.size() * 2, adaptive_buffer_max_size));
        BufferBase::set(memory.data(), memory.size(), 0);
    }
}

void CompressedWriteBuffer::finalizeImpl()
{
    /// Don't try to resize buffer in nextImpl.
    use_adaptive_buffer_size = false;
    next();
}

CompressedWriteBuffer::CompressedWriteBuffer(
    WriteBuffer & out_, CompressionCodecPtr codec_, size_t buf_size, bool use_adaptive_buffer_size_, size_t adaptive_buffer_initial_size)
    : BufferWithOwnMemory<WriteBuffer>(use_adaptive_buffer_size_ ? adaptive_buffer_initial_size : buf_size)
    , out(out_)
    , codec(std::move(codec_))
    , use_adaptive_buffer_size(use_adaptive_buffer_size_)
    , adaptive_buffer_max_size(buf_size)
{
}

CompressedWriteBuffer::~CompressedWriteBuffer()
{
    if (!canceled)
        finalize();
}


}
