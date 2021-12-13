#pragma once

#include <IO/WriteBuffer.h>
#include <libnuraft/nuraft.hxx>

namespace DB
{

class WriteBufferFromNuraftBuffer : public WriteBuffer
{
private:
    nuraft::ptr<nuraft::buffer> buffer;
    bool is_finished = false;

    static constexpr size_t initial_size = 32;
    static constexpr size_t size_multiplier = 2;

    void nextImpl() override;

public:
    WriteBufferFromNuraftBuffer();

    void finalize() override final;
    nuraft::ptr<nuraft::buffer> getBuffer();
    bool isFinished() const { return is_finished; }

    ~WriteBufferFromNuraftBuffer() override;
};

}
