#pragma once

#include <IO/ReadBuffer.h>
#include <IO/CompressedReadBufferWrapper.h>


namespace DB
{

class Bzip2ReadBuffer : public CompressedReadBufferWrapper
{
public:
    explicit Bzip2ReadBuffer(
            std::unique_ptr<ReadBuffer> in_,
            size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
            char * existing_memory = nullptr,
            size_t alignment = 0);

    ~Bzip2ReadBuffer() override;

private:
    bool nextImpl() override;

    class Bzip2StateWrapper;
    std::unique_ptr<Bzip2StateWrapper> bz;

    bool eof_flag;
};

}

