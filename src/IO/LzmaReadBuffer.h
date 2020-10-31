#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBuffer.h>

#include <lzma.h>

namespace DB
{
namespace ErrorCodes
{
}

class LzmaReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    LzmaReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~LzmaReadBuffer() override;

private:
    bool nextImpl() override;

    std::unique_ptr<ReadBuffer> in;
    lzma_stream lstr;
    bool eof;
};

}