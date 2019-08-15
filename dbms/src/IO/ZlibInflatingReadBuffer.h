#pragma once

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>

#include <zlib.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ZLIB_INFLATE_FAILED;
}

/// Reads compressed data from ReadBuffer in_ and performs decompression using zlib library.
/// This buffer is able to seamlessly decompress multiple concatenated zlib streams.
class ZlibInflatingReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    ZlibInflatingReadBuffer(
            ReadBuffer & in_,
            CompressionMethod compression_method,
            size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
            char * existing_memory = nullptr,
            size_t alignment = 0);

    ~ZlibInflatingReadBuffer() override;

private:
    bool nextImpl() override;

    ReadBuffer & in;
    z_stream zstr;
    bool eof;
};

}
