#pragma once

#include <Processors/Chunk.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

#include <Core/Field.h>

namespace DB
{

/// Watermark of a concrete partition over all its rows up to the commit-order position of the chunk.
struct PartitionWatermarkInfo : public ChunkInfoCloneable<PartitionWatermarkInfo>
{
    String partition_id;
    Field watermark;
};

/// Aligns the data stream (right input) with the watermark stream (left input).
class AlignStreamsStep : public IQueryPlanStep
{
    void updateOutputHeader() override;

public:
    AlignStreamsStep(SharedHeader metadata_header_, SharedHeader data_header_, String partition_id_, Field initial_watermark_);

    String getName() const override { return "AlignStreams"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;
    void describePipeline(FormatSettings & settings) const override;

private:
    const String partition_id;
    const Field initial_watermark;
};

}
