#pragma once

#include <Processors/Chunk.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/MergeTree/Streaming/Cursors/CursorUtils.h>

#include <base/types.h>

namespace DB
{

/// Cursor of the partition from which this chunk was originated.
struct PartitionCursorInfo : public ChunkInfoCloneable<PartitionCursorInfo>
{
    String partition_id;
    PartitionCursor cursor;
};

/// This step will calculate and set PartitionCursorInfo for each chunk.
class StampPartitionCursorsStep : public ITransformingStep
{
public:
    StampPartitionCursorsStep(SharedHeader input_header_, bool unordered_);

    String getName() const override { return "StampPartitionCursors"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    QueryPlanStepPtr clone() const override;

private:
    void updateOutputHeader() override;

    const bool unordered;
};

}
