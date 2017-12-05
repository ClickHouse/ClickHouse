#pragma once

#include <Common/Arena.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/** Writes data contiguously into Arena.
  * As it will be located in contiguous memory segment, it can be read back with ReadBufferFromMemory.
  */
class WriteBufferFromArena : public WriteBuffer
{
private:
    Arena & arena;

    void acquireSpaceInArena()
    {
        /// We will write directly to the remaining space in last chunk in arena.
        /// If it will be not enough, we will allocate and move data to the next, larger chunk.
        auto ptr_size = arena.getRemainingBufferInCurrentChunk();
        set(ptr_size.first, ptr_size.second);
    }

    void nextImpl() override
    {
        if (!hasPendingData())
        {
            /// Allocate new chunk in Arena, at least twice in size of used buffer,
            /// and copy the data in front of it.

            size_t data_size = count();

            arena.assumeAllocated(data_size);
            arena.realloc(buffer().begin(), data_size, data_size * 2);
        }

        /// After we possibly moved old data, we can append new data contiguously after it.
        acquireSpaceInArena();
    }

public:
    WriteBufferFromArena(Arena & arena_)
        : WriteBuffer(nullptr, 0), arena(arena_)
    {
        acquireSpaceInArena();
    }
};

}

