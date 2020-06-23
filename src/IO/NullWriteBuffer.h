#pragma once

#include "src/IO/WriteBuffer.h"
#include "src/IO/BufferWithOwnMemory.h"
#include <boost/noncopyable.hpp>

namespace DB
{

/// Simply do nothing, can be used to measure amount of written bytes.
class NullWriteBuffer : public BufferWithOwnMemory<WriteBuffer>, boost::noncopyable
{
public:
    NullWriteBuffer(size_t buf_size = 16<<10, char * existing_memory = nullptr, size_t alignment = false);
    void nextImpl() override;
};

}
