#include <city.h>
#include <cstring>

#include <base/types.h>
#include <base/unaligned.h>

#include <Compression/CompressionFactory.h>
#include "CompressedWriteBuffer.h"


namespace DB
{

namespace ErrorCodes
{
}

static constexpr auto CHECKSUM_SIZE{sizeof(CityHash_v1_0_2::uint128)};

void CompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    size_t decompressed_size = offset();
    UInt32 compressed_reserve_size = codec->getCompressedReserveSize(decompressed_size);

    /** During compression we need buffer with capacity >= compressed_reserve_size + CHECKSUM_SIZE.
      *
      * If output buffer has necessary capacity, we can compress data directly in output buffer.
      * Then we can write checksum at the output buffer begin.
      *
      * If output buffer does not have necessary capacity. Compress data in temporary buffer.
      * Then we can write checksum and temporary buffer in output buffer.
      */
    if (out.available() >= compressed_reserve_size + CHECKSUM_SIZE)
    {
        char * out_checksum_ptr = out.position();
        char * out_compressed_ptr = out.position() + CHECKSUM_SIZE;
        UInt32 compressed_size = codec->compress(working_buffer.begin(), decompressed_size, out_compressed_ptr);

        CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(out_compressed_ptr, compressed_size);
        memcpy(out_checksum_ptr, reinterpret_cast<const char *>(&checksum), CHECKSUM_SIZE);
        out.position() += CHECKSUM_SIZE + compressed_size;
    }
    else
    {
        compressed_buffer.resize(compressed_reserve_size);
        UInt32 compressed_size = codec->compress(working_buffer.begin(), decompressed_size, compressed_buffer.data());

        CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_buffer.data(), compressed_size);
        out.write(reinterpret_cast<const char *>(&checksum), CHECKSUM_SIZE);
        out.write(compressed_buffer.data(), compressed_size);
    }
}

CompressedWriteBuffer::CompressedWriteBuffer(WriteBuffer & out_, CompressionCodecPtr codec_, size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size), out(out_), codec(std::move(codec_))
{
}

}
