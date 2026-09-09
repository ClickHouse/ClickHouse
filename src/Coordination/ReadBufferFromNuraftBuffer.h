#pragma once
#include <IO/ReadBufferFromMemory.h>

#include <libnuraft/nuraft.hxx>

namespace DB
{

/// Reads from the start of the buffer (data_begin()), not from current position (data()/pos()).
/// Doesn't update buffer's pos().
class ReadBufferFromNuraftBuffer : public ReadBufferFromMemory
{
public:
    nuraft::ptr<nuraft::buffer> maybe_owned_buffer;

    explicit ReadBufferFromNuraftBuffer(nuraft::ptr<nuraft::buffer> buffer)
        : ReadBufferFromMemory(buffer->data_begin(), buffer->size()), maybe_owned_buffer(buffer)
    {}
    explicit ReadBufferFromNuraftBuffer(nuraft::buffer & buffer)
        : ReadBufferFromMemory(buffer.data_begin(), buffer.size())
    {}
};

}
