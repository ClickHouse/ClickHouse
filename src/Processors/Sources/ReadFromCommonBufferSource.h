#pragma once

#include <Processors/ISource.h>
#include <Processors/QueryPlan/SaveSubqueryResultToBufferStep.h>

namespace DB
{

class ReadFromCommonBufferSource : public ISource
{
public:
    explicit ReadFromCommonBufferSource(SharedHeader header, ChunkBufferPtr chunk_buffer_)
        : ISource(std::move(header))
        , chunk_buffer(std::move(chunk_buffer_))
    {
    }

    String getName() const override { return "ReadFromCommonBufferSource"; }

    Chunk generate() override
    {
        std::lock_guard lock(chunk_buffer->mutex);
        if (chunk_buffer->index >= chunk_buffer->chunks.size())
        {
            chunk_buffer->chunks.clear();
            return {};
        }

        return std::move(chunk_buffer->chunks[chunk_buffer->index++]);
    }

private:
    ChunkBufferPtr chunk_buffer;
};

}
