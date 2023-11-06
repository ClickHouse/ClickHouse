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

    size_t getLimitForSorting() const
    {
        auto * limit = typeid_cast<LimitStep *>(limit_step.get());
        assert(limit != nullptr);
        if (limit->getLimit() > std::numeric_limits<UInt64>::max() - limit->getOffset())
            return 0;

        return limit->getLimit() + limit->getOffset();
    }

    std::shared_ptr<TopNStep> makePreliminary(bool exact_rows_before_limit);

    std::shared_ptr<TopNStep> makeFinal(const DataStream & input_stream, size_t max_block_size, bool exact_rows_before_limit);

    StepType stepType() const override
    {
        return TopN;
    }

    SortingStep::Type sortType() const
    {
        auto * sorting = typeid_cast<SortingStep *>(sorting_step.get());
        assert(sorting != nullptr);
        return sorting->getType();
    }

    const SortDescription & getPrefixDescription() const
    {
        auto * sorting = typeid_cast<SortingStep *>(sorting_step.get());
        assert(sorting != nullptr);
        return sorting->getPrefixDescription();
    }

    const SortDescription & getSortDescription() const
    {
        auto * sorting = typeid_cast<SortingStep *>(sorting_step.get());
        assert(sorting != nullptr);
        return sorting->getSortDescription();
    }

private:
    QueryPlanStepPtr sorting_step;
    QueryPlanStepPtr limit_step;

    Phase phase = Phase::Unknown;
};

}
