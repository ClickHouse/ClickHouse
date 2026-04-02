#pragma once

#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class SetReadinessSignalStep : public ITransformingStep
{
public:
    SetReadinessSignalStep(SharedHeader input_header_, FutureSetPtr future_set_);

    String getName() const override { return "SetReadinessSignal"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    void updateOutputHeader() override {}

    FutureSetPtr future_set;
};

}
