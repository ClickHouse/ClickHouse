#pragma once

#include <IO/WriteBuffer.h>

namespace DB
{

/// Simply do nothing, can be used to measure amount of written bytes.
class NullWriteBuffer : public WriteBufferFromPointer
{
public:
    NullWriteBuffer()
    : WriteBufferFromPointer(nullptr, 0)
    {
        finalize();
    }

    void nextImpl() override
    {
        // no op
    }
};

}
