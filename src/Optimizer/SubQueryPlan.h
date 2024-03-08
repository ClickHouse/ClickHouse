#pragma once

#include <Processors/QueryPlan/QueryPlan.h>


namespace DB
{

class SubQueryPlan;
using SubQueryPlanPtr = std::shared_ptr<SubQueryPlan>;

/**
 * Represent part or whole of a QueryPlan, used by CBO optimizer.
 * The only difference between SubQueryPlan and QueryPlan is that the leaf nodes of QueryPlan must have no input.
 */
class SubQueryPlan : public QueryPlan
{
public:
    using Base = QueryPlan;

    SubQueryPlan() : Base() { }
    SubQueryPlan(SubQueryPlan &&) noexcept = default;
    ~SubQueryPlan() override = default;
    SubQueryPlan & operator=(SubQueryPlan &&) noexcept = default;

    /// Add single step to sub query plan
    void addStep(QueryPlanStepPtr step) override;

    /// Unite with other plans, used by queries like union, join etc.
    void unitePlans(QueryPlanStepPtr step, std::vector<SubQueryPlanPtr> plans);
};

}
