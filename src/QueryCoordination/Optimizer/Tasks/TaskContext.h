#pragma once

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
        OptimizeContextPtr optimize_context_,
        Float64 upper_bound_cost_ = std::numeric_limits<Float64>::max());

    Group & getCurrentGroup();

    const PhysicalProperties & getRequiredProp() const;

    OptimizeContextPtr getOptimizeContext();

    ContextPtr getQueryContext() const;

    Memo & getMemo();

    Float64 getUpperBoundCost() const;

    void setUpperBoundCost(Float64 upper_bound_cost_);

    void pushTask(OptimizeTaskPtr task);

private:
    Group & group;

    PhysicalProperties required_properties;

    Float64 upper_bound_cost;

    OptimizeContextPtr optimize_context;
};

using TaskContextPtr = std::shared_ptr<TaskContext>;

}
