#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

#include <Core/Field.h>

namespace DB
{

/// Aggregates the watermark markers of a stream so the emitted watermarks never regress.
class RaiseWatermarksStep : public ITransformingStep
{
    void updateOutputHeader() override;

public:
    RaiseWatermarksStep(SharedHeader input_header_, Field initial_watermark_);

    String getName() const override { return "RaiseWatermarks"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    QueryPlanStepPtr clone() const override;

private:
    const Field initial_watermark;
};

}
