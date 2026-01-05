#pragma once

#include <Processors/ChunkBuffer.h>
#include <Processors/ISource.h>

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

    Chunk generate() override { return chunk_buffer->extractNext(); }

private:
    ChunkBufferPtr chunk_buffer;
};

}
