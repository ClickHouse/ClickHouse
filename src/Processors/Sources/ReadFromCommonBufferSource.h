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

    Chunk generate() override { return chunk_buffer->extractNext(); }

private:
    ChunkBufferPtr chunk_buffer;
};

}
