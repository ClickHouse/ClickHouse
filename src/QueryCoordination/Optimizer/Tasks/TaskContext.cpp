#include <QueryCoordination/Optimizer/Group.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeTask.h>
#include <QueryCoordination/Optimizer/Tasks/TaskContext.h>

namespace DB
{

TaskContext::TaskContext(Group & group_, const PhysicalProperties & required_properties_, OptimizeContextPtr optimize_context_)
    : group(group_), required_properties(required_properties_), optimize_context(optimize_context_)
{
    upper_bound_cost = Cost::infinite(CostSettings::fromContext(optimize_context_->getQueryContext()).getCostWeight());
}

TaskContext::TaskContext(
    Group & group_, const PhysicalProperties & required_properties_, OptimizeContextPtr optimize_context_, Cost upper_bound_cost_)
    : group(group_), required_properties(required_properties_), upper_bound_cost(upper_bound_cost_), optimize_context(optimize_context_)
{
}

Group & TaskContext::getCurrentGroup()
{
    return group;
}

const PhysicalProperties & TaskContext::getRequiredProp() const
{
    return required_properties;
}

OptimizeContextPtr TaskContext::getOptimizeContext()
{
    return optimize_context;
}

ContextPtr TaskContext::getQueryContext() const
{
    return optimize_context->getQueryContext();
}

Memo & TaskContext::getMemo()
{
    return optimize_context->getMemo();
}

void TaskContext::pushTask(OptimizeTaskPtr task)
{
    optimize_context->pushTask(std::move(task));
}

Cost TaskContext::getUpperBoundCost() const
{
    return upper_bound_cost;
}

void TaskContext::setUpperBoundCost(const Cost & upper_bound_cost_)
{
    upper_bound_cost = upper_bound_cost_;
}

}
