#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SortingStep.h>


namespace DB
{

class TopNStep final : public ITransformingStep
{
public:
    /// Only work on query coordination
    enum Phase
    {
        Final,
        Preliminary,
        Unknown,
    };

    TopNStep(
        QueryPlanStepPtr sorting_step_, QueryPlanStepPtr limit_step_);

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void updateOutputStream() override;

    String getName() const override { return "TopN"; }

    Phase getPhase() const { return phase; }

    void setPhase(Phase phase_) { phase = phase_; }

    size_t getLimit() const
    {
        auto * limit = typeid_cast<LimitStep *>(limit_step.get());
        assert(limit != nullptr);
        return limit->getLimit();
    }

    std::shared_ptr<TopNStep> makePreliminary(bool exact_rows_before_limit);

    std::shared_ptr<TopNStep> makeFinal(size_t max_block_size, bool exact_rows_before_limit);

    StepType stepType() const override
    {
        return TopN;
    }

private:
    QueryPlanStepPtr sorting_step;
    QueryPlanStepPtr limit_step;

    Phase phase = Phase::Unknown;
};

}
