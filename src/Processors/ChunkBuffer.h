#pragma once

#include <Processors/Chunk.h>
#include <base/defines.h>

#include <mutex>
#include <queue>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

/** A thread-safe buffer for storing chunks from multiple inputs.
  * Inputs can append chunks to the buffer, and once all inputs are finished,
  * chunks can be extracted from the buffer. Implements a simple producer-consumer pattern.
  * Used to implement common subplan step result buffering.
  */
struct ChunkBuffer
{
    void append(Chunk && chunk)
    {
        std::lock_guard lock(mutex);
        chunks.push(std::move(chunk));
    }

    Chunk extractNext()
    {
        std::lock_guard lock(mutex);

        if (!isReady())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Trying to extract chunk from ChunkBuffer before all inputs are finished (unfinished inputs: {})",
                unfinished_inputs);

        if (chunks.empty())
            return {};

        Chunk chunk = std::move(chunks.front());
        chunks.pop();
        return chunk;
    }

    void onInputFinish()
    {
        std::lock_guard lock(mutex);
        chassert(unfinished_inputs > 0);
        --unfinished_inputs;
    }

    bool isReady() const { return unfinished_inputs == 0; }

    void setInputsNumber(size_t num) { unfinished_inputs = num; }

private:
    std::mutex mutex;
    std::queue<Chunk> chunks;
    size_t unfinished_inputs = 1; /// Initialized to 1 to avoid being ready before setting the actual number of inputs
};

using ChunkBufferPtr = std::shared_ptr<ChunkBuffer>;

}
