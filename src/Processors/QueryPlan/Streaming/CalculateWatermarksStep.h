#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

#include <Interpreters/Context_fwd.h>

#include <Core/Streaming/Settings.h>

namespace DB
{

/// Evaluates the watermark expression, emits a watermark marker after each data chunk.
class CalculateWatermarksStep : public ITransformingStep
{
    void updateOutputHeader() override;

public:
    CalculateWatermarksStep(SharedHeader input_header_, WatermarkSettingsPtr watermark_, ContextPtr context_);

    String getName() const override { return "CalculateWatermarks"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    QueryPlanStepPtr clone() const override;

private:
    const WatermarkSettingsPtr watermark;
    const ContextPtr context;
};

}
