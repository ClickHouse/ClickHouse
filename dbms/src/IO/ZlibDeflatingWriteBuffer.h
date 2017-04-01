#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ZlibCompressionMethod.h>

#include <zlib.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ZLIB_DEFLATE_FAILED;
}

/// Performs compression using zlib library and writes compressed data to out_ WriteBuffer.
class ZlibDeflatingWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    ZlibDeflatingWriteBuffer(
            WriteBuffer & out_,
            ZlibCompressionMethod compression_method,
            int compression_level,
            size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
            char * existing_memory = nullptr,
            size_t alignment = 0);

    /// Flush all pending data and write zlib footer to the underlying buffer.
    /// After the first call to this function, subsequent calls will have no effect and
    /// an attempt to write to this buffer will result in exception.
    void finish();

    ~ZlibDeflatingWriteBuffer() override;

private:
    void nextImpl() override;

    WriteBuffer & out;
    z_stream zstr;
    bool finished = false;
};

}
