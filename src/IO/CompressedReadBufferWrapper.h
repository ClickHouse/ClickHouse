#pragma once
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class CompressedReadBufferWrapper : public BufferWithOwnMemory<ReadBuffer>
{
public:
    CompressedReadBufferWrapper(
      std::unique_ptr<ReadBuffer> in_,
      size_t buf_size,
      char * existing_memory,
      size_t alignment)
    : BufferWithOwnMemory<ReadBuffer>(buf_size, existing_memory, alignment)
    , in(std::move(in_)) {}

    const ReadBuffer & getWrappedReadBuffer() const { return *in; }
    ReadBuffer & getWrappedReadBuffer() { return *in; }

    void prefetch() override { in->prefetch(); }

protected:
    std::unique_ptr<ReadBuffer> in;
};

}
