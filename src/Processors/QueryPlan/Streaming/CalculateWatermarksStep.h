#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

#include <Interpreters/Context_fwd.h>

#include <Core/Field.h>
#include <Core/Streaming/Settings.h>

namespace DB
{

/// Evaluates the watermark expression, appends the time-attribute and watermark columns.
class CalculateWatermarksStep : public ITransformingStep
{
    void updateOutputHeader() override;

public:
    CalculateWatermarksStep(SharedHeader input_header_, WatermarkSettingsPtr watermark_settings_, Field initial_watermark_, ContextPtr context_);

    String getName() const override { return "CalculateWatermarks"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    QueryPlanStepPtr clone() const override;

private:
    const WatermarkSettingsPtr watermark_settings;
    const Field initial_watermark;
    const ContextPtr context;
};

}
