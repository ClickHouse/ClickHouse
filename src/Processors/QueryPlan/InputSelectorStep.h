#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

/// Takes 3 children: signal, true branch, false branch.
/// Resizes each pipeline to a single stream and adds InputSelectorTransform on top.
class InputSelectorStep : public IQueryPlanStep
{
public:
    InputSelectorStep(SharedHeader signal_header, SharedHeader data_header);

    String getName() const override { return "InputSelector"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;
    void describePipeline(FormatSettings & settings) const override;
    bool hasCorrelatedExpressions() const override { return false; }

private:
    void updateOutputHeader() override;
};

}
