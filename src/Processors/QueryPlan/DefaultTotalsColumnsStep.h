#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Overwrites the given output column positions with their type default on the WITH TOTALS stream only
/// (identity on the main stream). Inserted right after the projection step when an injective GROUP BY key
/// was unwrapped under a plain WITH TOTALS, so the grand-total row emits defaultOf(typeOf(f(g))) instead of
/// f(default). See OptimizeGroupByInjectiveFunctionsPass and #110715.
class DefaultTotalsColumnsStep : public ITransformingStep
{
public:
    DefaultTotalsColumnsStep(const SharedHeader & input_header_, std::vector<size_t> positions_);

    String getName() const override { return "DefaultTotalsColumns"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    QueryPlanStepPtr clone() const override;

private:
    void updateOutputHeader() override;

    std::vector<size_t> positions;
};

}
