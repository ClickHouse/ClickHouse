#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

/// Query-plan step that calibrates watermarks across multiple streaming inputs.
class WatermarkMergerStep : public IQueryPlanStep
{
public:
    WatermarkMergerStep(SharedHeaders input_headers_, size_t num_output_streams_);

    String getName() const override { return "WatermarkMerger"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    size_t num_output_streams;
};

}
