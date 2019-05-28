#pragma once

#include <Formats/FormatFactory.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>


namespace DB
{

/** Allows many threads to read from single ReadBuffer
  * Each SharedReadBuffer has his own memory,
  *  which he filles under shared mutex using FileSegmentationEngine.
  */
class SharedReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    SharedReadBuffer(
            ReadBuffer & in_,
            std::shared_ptr<std::mutex> & mutex_,
            FormatFactory::FileSegmentationEngine file_segmentation_engine_,
            size_t min_chunk_size_)
        : BufferWithOwnMemory<ReadBuffer>(),
        mutex(mutex_),
        in(in_),
        file_segmentation_engine(file_segmentation_engine_),
        min_chunk_size(min_chunk_size_)
    {
    }

private:
    bool nextImpl() override
    {
        if (eof || !mutex)
            return false;

        std::lock_guard<std::mutex> lock(*mutex);
        if (in.eof())
        {
            eof = true;
            return false;
        }

        bool res = file_segmentation_engine(in, memory, min_chunk_size);
        if (!res)
            return false;

        working_buffer = Buffer(memory.data(), memory.data() + memory.size());
        return true;
    }

private:
    // Pointer to common mutex.
    std::shared_ptr<std::mutex> mutex;

    // Original ReadBuffer to read from.
    ReadBuffer & in;

    // Function to fill working_buffer.
    FormatFactory::FileSegmentationEngine file_segmentation_engine;

    // FileSegmentationEngine parameter.
    size_t min_chunk_size;

    // Indicator of the eof. Save extra lock acquiring.
    bool eof{false};
};
}
