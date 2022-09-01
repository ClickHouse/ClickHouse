#include <city.h>
#include <string.h>

#include <base/unaligned.h>
#include <base/types.h>

#include "CompressedWriteBuffer.h"
#include <Compression/CompressionFactory.h>


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
    compressed_buffer.resize(compressed_reserve_size);
    UInt32 compressed_size = codec->compress(working_buffer.begin(), decompressed_size, compressed_buffer.data());

    CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_buffer.data(), compressed_size);
    out.write(reinterpret_cast<const char *>(&checksum), CHECKSUM_SIZE);
    out.write(compressed_buffer.data(), compressed_size);
}

CompressedWriteBuffer::~CompressedWriteBuffer()
{
    finalize();
}

CompressedWriteBuffer::CompressedWriteBuffer(
    WriteBuffer & out_,
    CompressionCodecPtr codec_,
    size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size), out(out_), codec(std::move(codec_))
{
}

}
