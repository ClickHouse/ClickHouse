#include "config.h"

#if USE_SNAPPY
#include <algorithm>

#include <snappy.h>

#include <IO/HadoopSnappyWriteBuffer.h>

namespace DB
{

namespace
{

/// Maximum uncompressed data per Hadoop-snappy block.
///
/// Kept well under DBMS_DEFAULT_BUFFER_SIZE so that both the compressed subblock
/// and the decompressed block fit into HadoopSnappyReadBuffer's internal buffers:
/// it rejects subblocks whose compressed length exceeds DBMS_DEFAULT_BUFFER_SIZE
/// and decodes a whole block into a single read buffer. 256 KiB also matches the
/// block size conventionally used by Hadoop's snappy codec.
constexpr size_t MAX_BLOCK_SIZE = 256 * 1024;

void writeBigEndian32(WriteBuffer & out, uint32_t value)
{
    const uint8_t bytes[4]
        = {static_cast<uint8_t>((value >> 24) & 0xff),
           static_cast<uint8_t>((value >> 16) & 0xff),
           static_cast<uint8_t>((value >> 8) & 0xff),
           static_cast<uint8_t>(value & 0xff)};
    out.write(reinterpret_cast<const char *>(bytes), sizeof(bytes));
}

}

void HadoopSnappyWriteBuffer::writeBlock(const char * data, size_t size)
{
    /// Block header: uncompressed length of the whole block (big-endian 4 bytes).
    writeBigEndian32(*out, static_cast<uint32_t>(size));

    /// A single subblock: compressed length (big-endian 4 bytes) followed by the raw snappy data.
    compress_buffer.resize(snappy::MaxCompressedLength(size));
    size_t compressed_size = 0;
    snappy::RawCompress(data, size, compress_buffer.data(), &compressed_size);

    writeBigEndian32(*out, static_cast<uint32_t>(compressed_size));
    out->write(compress_buffer.data(), compressed_size);
}

void HadoopSnappyWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    const char * data = working_buffer.begin();
    size_t remaining = offset();

    /// Split into blocks of at most MAX_BLOCK_SIZE uncompressed bytes.
    while (remaining > 0)
    {
        size_t block_size = std::min(remaining, MAX_BLOCK_SIZE);
        writeBlock(data, block_size);
        data += block_size;
        remaining -= block_size;
    }
}

}

#endif
