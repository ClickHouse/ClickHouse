#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Chunk.h>

#include <Core/Field.h>

namespace DB
{

/// Watermark of a concrete partition.
struct PartitionWatermarkInfo : public ChunkInfoCloneable<PartitionWatermarkInfo>
{
    String partition_id;
    Field watermark;
};

/// Calculates the partition watermark on the aligned stream. Attaches PartitionWatermarkInfo.
class StampPartitionWatermarksStep : public ITransformingStep
{
    void updateOutputHeader() override;

public:
    StampPartitionWatermarksStep(SharedHeader input_header_, String partition_id_);

    String getName() const override { return "StampPartitionWatermarks"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    QueryPlanStepPtr clone() const override;

private:
    const String partition_id;
};

}
