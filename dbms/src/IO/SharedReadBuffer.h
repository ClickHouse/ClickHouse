#pragma once

#include <Formats/FormatFactory.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class SharedReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    SharedReadBuffer(
            ReadBuffer& in_,
            std::shared_ptr<std::mutex>& mutex_,
            FormatFactory::ChunkCreator getChunk_,
            size_t min_size_)
        : BufferWithOwnMemory<ReadBuffer>(),
        mutex(mutex_),
        in(in_),
        getChunk(getChunk_),
        min_size(min_size_)
    {
    }

private:
    bool nextImpl() override
    {
        std::lock_guard<std::mutex> lock(*mutex);
        if (in.eof())
            return false;
        bool res = getChunk(in, memory, min_size);
        if (!res)
            return false;
        working_buffer = Buffer(memory.data(), memory.data() + memory.size());
        return true;
    }

private:
    std::shared_ptr<std::mutex> mutex;
    ReadBuffer & in;
    FormatFactory::ChunkCreator getChunk;
    size_t min_size;
};
}
