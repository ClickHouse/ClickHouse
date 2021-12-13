#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{

class Bzip2WriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    Bzip2WriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~Bzip2WriteBuffer() override;

    void finalize() override { finish(); }

private:
    void nextImpl() override;

    void finish();
    void finishImpl();

    class Bzip2StateWrapper;
    std::unique_ptr<Bzip2StateWrapper> bz;

    std::unique_ptr<WriteBuffer> out;

    bool finished = false;
};

}
