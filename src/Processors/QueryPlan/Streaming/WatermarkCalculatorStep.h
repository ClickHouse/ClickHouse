#pragma once

#include <Analyzer/TableExpressionModifiers.h>

#include <Interpreters/Context_fwd.h>

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Query-plan step that resolves the watermark expression of the wrapped reading plan.
class WatermarkCalculatorStep : public ITransformingStep
{
public:
    WatermarkCalculatorStep(SharedHeader input_header_, WatermarkSettingsPtr watermark_, ContextPtr context_);

    String getName() const override { return "WatermarkCalculator"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    QueryPlanStepPtr clone() const override;

private:
    void updateOutputHeader() override;

    WatermarkSettingsPtr watermark;
    ContextPtr context;
};

}
