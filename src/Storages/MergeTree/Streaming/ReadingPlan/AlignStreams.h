#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>


namespace DB
{

/// Aligns the data stream (right input) with metadata stream (left input). Metadata is, for example, calculated watermark.
class AlignStreamsStep : public IQueryPlanStep
{
    void updateOutputHeader() override;

public:
    AlignStreamsStep(SharedHeader left_header_, SharedHeader right_header_);

    String getName() const override { return "AlignStreams"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;
    void describePipeline(FormatSettings & settings) const override;
};

}
