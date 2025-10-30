#pragma once

#include <IO/WriteBuffer.h>
#include <Common/ProfileEvents.h>

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

}
