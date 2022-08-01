#pragma once

#include <Common/config.h>

#if USE_SNAPPY
#include <IO/WriteBufferDecorator.h>
#include <IO/WriteBuffer.h>

namespace DB
{
/// Performs compression using snappy library and write compressed data to the underlying buffer.
class SnappyWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    explicit SnappyWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

private:
    void nextImpl() override;

    void finalizeBefore() override;

    String uncompress_buffer;
    String compress_buffer;
};

}

#endif

