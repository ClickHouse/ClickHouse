#pragma once

#include <Processors/QueryPlan/QueryPlan.h>


namespace DB
{

class StepTree;
using StepTreePtr = std::shared_ptr<StepTree>;

/**
 * Represent part or whole of a QueryPlan, used by CBO optimizer.
 * The only difference between StepTree and QueryPlan is that the leaf nodes of QueryPlan must have no input.
 */
class StepTree : public QueryPlan
{
public:
    using Base = QueryPlan;

    StepTree() : Base() { }
    StepTree(StepTree &&) noexcept = default;
    ~StepTree() override = default;
    StepTree & operator=(StepTree &&) noexcept = default;

    /// Add single step to sub query plan
    void addStep(QueryPlanStepPtr step) override;

    /// Unite with other plans, used by queries like union, join etc.
    void unitePlans(QueryPlanStepPtr step, std::vector<StepTreePtr> plans);
};

}
