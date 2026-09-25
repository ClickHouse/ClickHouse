#pragma once

#include <Processors/Chunk.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/MergeTree/Streaming/Cursors/CursorUtils.h>

#include <base/types.h>

namespace DB
{

struct PartitionCursorInfo : public ChunkInfoCloneable<PartitionCursorInfo>
{
    String partition_id;
    PartitionCursor first;
    PartitionCursor last;
};

class StampPartitionCursorsStep : public ITransformingStep
{
    void updateOutputHeader() override;

public:
    StampPartitionCursorsStep(SharedHeader input_header_, String partition_id_, bool unordered_);

    String getName() const override { return "StampPartitionCursors"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    QueryPlanStepPtr clone() const override;

private:
    const String partition_id;
    const bool unordered;
};

}
