#pragma once

#include <Processors/Chunk.h>
#include <Processors/QueryPlan/ITransformingStep.h>

#include <base/types.h>

namespace DB
{

/// Cursor of the partition from which this chunk was originated.
struct StreamingChunkCursorInfo : public ChunkInfoCloneable<StreamingChunkCursorInfo>
{
    String partition_id;
    Int64 last_block_number = -1;
    Int64 last_block_offset = -1;
};

/// This step will calculate and set StreamingChunkCursorInfo for each chunk.
class BuildStreamingChunkCursorStep : public ITransformingStep
{
public:
    explicit BuildStreamingChunkCursorStep(SharedHeader input_header_);

    String getName() const override { return "BuildStreamingChunkCursor"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    QueryPlanStepPtr clone() const override;

private:
    void updateOutputHeader() override;
};

}
