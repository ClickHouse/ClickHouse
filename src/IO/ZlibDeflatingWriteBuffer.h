#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBufferDecorator.h>


#include <zlib.h>


namespace DB
{

/// Performs compression using zlib library and writes compressed data to out_ WriteBuffer.
class ZlibDeflatingWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    ZlibDeflatingWriteBuffer(
            std::unique_ptr<WriteBuffer> out_,
            CompressionMethod compression_method,
            int compression_level,
            size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
            char * existing_memory = nullptr,
            size_t alignment = 0);

private:
    void nextImpl() override;

    /// Flush all pending data and write zlib footer to the underlying buffer.
    /// After the first call to this function, subsequent calls will have no effect and
    /// an attempt to write to this buffer will result in exception.
    virtual void finalizeBefore() override;
    virtual void finalizeAfter() override;

    z_stream zstr;
};

}
