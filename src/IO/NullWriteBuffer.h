#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <boost/noncopyable.hpp>

namespace DB
{

/// Simply do nothing, can be used to measure amount of written bytes.
class NullWriteBuffer : public BufferWithOwnMemory<WriteBuffer>, boost::noncopyable
{
public:
    explicit NullWriteBuffer(size_t buf_size = 16<<10, char * existing_memory = nullptr, size_t alignment = false);
    void nextImpl() override;
};

}
