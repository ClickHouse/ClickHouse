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
            ReadBuffer & in_,
            std::shared_ptr<std::mutex> & mutex_,
            FormatFactory::FileSegmentationEngine file_segmentation_engine_,
            size_t min_size_)
        : BufferWithOwnMemory<ReadBuffer>(),
        mutex(mutex_),
        in(in_),
        file_segmentation_engine(file_segmentation_engine_),
        min_size(min_size_)
    {
    }

private:
    bool nextImpl() override
    {
        if (eof)
            return false;

        std::lock_guard<std::mutex> lock(*mutex);
        if (in.eof())
        {
            eof = true;
            return false;
        }

        bool res = file_segmentation_engine(in, memory, min_size);
        if (!res)
            return false;

        working_buffer = Buffer(memory.data(), memory.data() + memory.size());
        return true;
    }

private:
    bool eof = false;
    std::shared_ptr<std::mutex> mutex;
    ReadBuffer & in;
    FormatFactory::FileSegmentationEngine file_segmentation_engine;
    size_t min_size;
};
}
