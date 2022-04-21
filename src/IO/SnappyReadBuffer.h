#pragma once

#include <Common/config.h>

#if USE_SNAPPY

#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{
class SnappyReadBuffer : public BufferWithOwnMemory<SeekableReadBuffer>
{
public:
    explicit SnappyReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~SnappyReadBuffer() override;

    bool nextImpl() override;
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

private:
    std::unique_ptr<ReadBuffer> in;
    String compress_buffer;
    String uncompress_buffer;
};

}
#endif
