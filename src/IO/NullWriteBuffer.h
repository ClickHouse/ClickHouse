#pragma once

#include <IO/WriteBuffer.h>

namespace DB
{

/// Simply do nothing, can be used to measure amount of written bytes.
class NullWriteBuffer : public WriteBufferFromPointer
{
public:
    NullWriteBuffer();
    ~NullWriteBuffer() override;

    void nextImpl() override;

private:
    char data[128];
};

}
