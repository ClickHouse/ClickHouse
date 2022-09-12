#pragma once

#include <IO/CompressedReadBufferWrapper.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBuffer.h>

#include <zstd.h>


namespace DB
{
namespace ErrorCodes
{
}

class ZstdInflatingReadBuffer : public CompressedReadBufferWrapper
{
public:
    explicit ZstdInflatingReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~ZstdInflatingReadBuffer() override;

private:
    bool nextImpl() override;

    ZSTD_DCtx * dctx;
    ZSTD_inBuffer input;
    ZSTD_outBuffer output;
    bool eof_flag = false;
};

}
