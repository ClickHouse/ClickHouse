#pragma once
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Formats/FormatFactory.h>
#include <shared_mutex>

namespace DB {

class SharedReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
    // reuse previous declaration
    using ChunkCreator = std::function<bool(const FormatSettings & format_settings, ReadBuffer & in, DB::Memory<> & memory, size_t min_size)>;
public:
    SharedReadBuffer(
            ReadBuffer& in_,
            const FormatSettings & format_settings_,
            std::shared_ptr<std::mutex>& mutex_,
            ChunkCreator getChunk_,
            size_t min_size_)
        : BufferWithOwnMemory<ReadBuffer>(),
        mutex(mutex_),
        in(in_),
        format_settings(format_settings_),
        getChunk(getChunk_),
        min_size(min_size_)
    {
    }

private:
    // TODO throw some relevant exceptions
    bool nextImpl() override
    {
        std::lock_guard<std::mutex> lock(*mutex);
        if (in.eof())
            return false;
        bool res = getChunk(format_settings, in, memory, min_size);
        if (!res)
            return false;
        working_buffer = Buffer(memory.data(), memory.data() + memory.size());
        return true;
    }
private:
    std::shared_ptr<std::mutex> mutex;
    ReadBuffer & in;
    const FormatSettings & format_settings;
    ChunkCreator getChunk;
    size_t min_size;
};
}
