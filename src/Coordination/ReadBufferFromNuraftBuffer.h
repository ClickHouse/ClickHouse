#pragma once
#include <IO/ReadBufferFromMemory.h>

#include <libnuraft/nuraft.hxx>

namespace DB
{

class ReadBufferFromNuraftBuffer : public ReadBufferFromMemory
{
public:
    explicit ReadBufferFromNuraftBuffer(nuraft::ptr<nuraft::buffer> buffer);
    explicit ReadBufferFromNuraftBuffer(nuraft::buffer & buffer);
};

}
