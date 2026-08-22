#pragma once

#include <IO/CompressedReadBufferWrapper.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBuffer.h>

#include <lz4.h>
#include <lz4frame.h>


namespace DB
{

class Lz4InflatingReadBuffer : public CompressedReadBufferWrapper
{
public:
    explicit Lz4InflatingReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~Lz4InflatingReadBuffer() override;

private:
    bool nextImpl() override;

    LZ4F_dctx* dctx;

    bool in_eof = false;
};

}
