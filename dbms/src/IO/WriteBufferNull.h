#pragma once
#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>


namespace DB
{

/// Doesn't do anything, just resets working buffer
/// Assume that we almost don't write to it
class WriteBufferNull : public BufferWithOwnMemory<WriteBuffer>
{
public:

    explicit WriteBufferNull(size_t dummy_buffer_size = 32) /// DBMS_DEFAULT_BUFFER_SIZE is too much
        : BufferWithOwnMemory<WriteBuffer>(dummy_buffer_size) {}

    void nextImpl() override
    {
        set(internal_buffer.begin(), internal_buffer.size());
    }
};

}
