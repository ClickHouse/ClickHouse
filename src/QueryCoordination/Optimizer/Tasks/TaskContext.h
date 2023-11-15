#pragma once

#include <QueryCoordination/Optimizer/Cost/Cost.h>
#include <QueryCoordination/Optimizer/Cost/CostSettings.h>
#include <QueryCoordination/Optimizer/PhysicalProperties.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeContext.h>

namespace DB
{

class Group;
class OptimizeTask;
using OptimizeTaskPtr = std::unique_ptr<OptimizeTask>;

class TaskContext
{
public:
    TaskContext(
        Group & group_,
        const PhysicalProperties & required_properties_,
        OptimizeContextPtr optimize_context_);

    TaskContext(
        Group & group_,
        const PhysicalProperties & required_properties_,
        OptimizeContextPtr optimize_context_,
        Cost upper_bound_cost_);

    Group & getCurrentGroup();

    const PhysicalProperties & getRequiredProp() const;

    OptimizeContextPtr getOptimizeContext();

    ContextPtr getQueryContext() const;

    Memo & getMemo();

    Cost getUpperBoundCost() const;

    void setUpperBoundCost(const Cost & upper_bound_cost_);

    void pushTask(OptimizeTaskPtr task);

private:
    Group & group;

    PhysicalProperties required_properties;

    Cost upper_bound_cost;

    OptimizeContextPtr optimize_context;
};

using TaskContextPtr = std::shared_ptr<TaskContext>;

}
