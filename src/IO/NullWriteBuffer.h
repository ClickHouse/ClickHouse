#pragma once

#include <IO/WriteBuffer.h>
#include <Common/ProfileEvents.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{

/// Simply do nothing, can be used to measure amount of written bytes.
class NullWriteBuffer final : public WriteBufferFromPointer
{
public:
    NullWriteBuffer();
    explicit NullWriteBuffer(const ProfileEvents::Event & write_event);
    ~NullWriteBuffer() override;

    void nextImpl() override;

private:
    ProfileEvents::Event write_event;
    char data[128];
};

/// Similar to above, but allocated memory,
/// which is useful when WriteBufferFromFileDecorator<NullWriteBufferWithMemory> is used.
class NullWriteBufferWithMemory final : public BufferWithOwnMemory<WriteBufferFromPointer>
{
public:
    explicit NullWriteBufferWithMemory(size_t size);
    ~NullWriteBufferWithMemory() override;

    void nextImpl() override {}
};

}
