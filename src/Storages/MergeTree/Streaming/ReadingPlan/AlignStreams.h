#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>


namespace DB
{

/// Aligns the data stream (right input) with the watermark markers of the metadata stream (left input).
class AlignStreamsStep : public IQueryPlanStep
{
    void updateOutputHeader() override;

public:
    AlignStreamsStep(SharedHeader metadata_header_, SharedHeader data_header_);

    String getName() const override { return "AlignStreams"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;
    void describePipeline(FormatSettings & settings) const override;
};

}
