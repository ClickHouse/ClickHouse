#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBuffer.h>

#include <lz4.h>
#include <lz4frame.h>


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

    size_t ret;

    void * src;
    void * dst;

    size_t src_capacity;
    size_t dst_capacity;

    size_t in_available;

    LZ4F_dctx* dctx;
    size_t dctx_status;


    bool eof = false;

};

}
