#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBuffer.h>


namespace DB
{
namespace ErrorCodes
{
}

class Lz4InflatingReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    Lz4InflatingReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~Lz4InflatingReadBuffer() override;

private:
    bool nextImpl() override;

    std::unique_ptr<ReadBuffer> in;
};

}
