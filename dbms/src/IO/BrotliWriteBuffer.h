#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{
class BrotliWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    BrotliWriteBuffer(
            WriteBuffer & out_,
            int compression_level,
            size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
            char * existing_memory = nullptr,
            size_t alignment = 0);

    ~BrotliWriteBuffer() override;

private:
    void nextImpl() override;

    WriteBuffer & out;
    bool finished = false;
};

}
