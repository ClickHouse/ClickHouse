#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <boost/noncopyable.hpp>

namespace DB
{

/// Simply do nothing, can be used to measure amount of written bytes.
class NullWriteBuffer : public WriteBuffer, boost::noncopyable
{
public:
    NullWriteBuffer();
    void nextImpl() override;

private:
    char data[128];
};

}
