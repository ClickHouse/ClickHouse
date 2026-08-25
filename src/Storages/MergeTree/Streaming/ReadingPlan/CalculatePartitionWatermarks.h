#pragma once

#include <Processors/QueryPlan/Streaming/CalculateWatermarksStep.h>
#include <Processors/Chunk.h>

#include <Core/Field.h>

namespace DB
{

/// Watermark of a concrete partition; attached by the calculator to the data chunk it was calculated from.
struct PartitionWatermarkInfo : public ChunkInfoCloneable<PartitionWatermarkInfo>
{
    String partition_id;
    Field watermark;
};

/// Scopes the calculated watermarks to a concrete partition by attaching PartitionWatermarkInfo to data chunks.
class CalculatePartitionWatermarksStep : public CalculateWatermarksStep
{
public:
    CalculatePartitionWatermarksStep(SharedHeader input_header_, WatermarkSettingsPtr watermark_settings_, Field initial_watermark_, ContextPtr context_, String partition_id_);

    String getName() const override { return "CalculatePartitionWatermarks"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    QueryPlanStepPtr clone() const override;

private:
    const String partition_id;
};

}
