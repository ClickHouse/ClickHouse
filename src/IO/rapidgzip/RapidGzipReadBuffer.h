#pragma once

#include <IO/CompressedReadBufferWrapper.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class RapidGzipReadBufferImpl;

class RapidGzipReadBuffer : public CompressedReadBufferWrapper
{
public:
    explicit RapidGzipReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~RapidGzipReadBuffer() override;
private:
    bool nextImpl() override;

    std::unique_ptr<RapidGzipReadBufferImpl> impl;
};

}
