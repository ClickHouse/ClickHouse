#pragma once

#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

struct ChunkBuffer;
using ChunkBufferPtr = std::shared_ptr<ChunkBuffer>;

class ReadFromCommonBufferStep : public ISourceStep
{
public:
    ReadFromCommonBufferStep(
        const SharedHeader & header_,
        ChunkBufferPtr chunk_buffer_,
        size_t max_streams_);

    String getName() const override { return "ReadFromCommonBuffer"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
private:
    ChunkBufferPtr chunk_buffer;
    size_t max_streams;
};

}
