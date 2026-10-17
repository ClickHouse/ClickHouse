#pragma once

#include <IO/CompressedReadBufferWrapper.h>
#include <IO/ReadBuffer.h>

#include <lzma.h>

namespace DB
{

class LZMAInflatingReadBuffer : public CompressedReadBufferWrapper
{
public:
    explicit LZMAInflatingReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~LZMAInflatingReadBuffer() override;

private:
    bool nextImpl() override;

    lzma_stream lstr;
    bool eof_flag;
};

}
