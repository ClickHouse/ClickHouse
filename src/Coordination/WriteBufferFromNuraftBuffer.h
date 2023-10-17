#pragma once

#include <IO/WriteBuffer.h>
#include <libnuraft/nuraft.hxx>

namespace DB
{

class WriteBufferFromNuraftBuffer : public WriteBuffer
{
public:
    WriteBufferFromNuraftBuffer();

    nuraft::ptr<nuraft::buffer> getBuffer();

    ~WriteBufferFromNuraftBuffer() override;

private:
    void finalizeImpl() override final;

    void nextImpl() override;

    nuraft::ptr<nuraft::buffer> buffer;

    static constexpr size_t initial_size = 32;
    static constexpr size_t size_multiplier = 2;
};

}
