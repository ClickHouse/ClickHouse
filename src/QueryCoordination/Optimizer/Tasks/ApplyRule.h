#pragma once

#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeTask.h>
#include <QueryCoordination/Optimizer/Transform/Transformation.h>

namespace DB
{

class ApplyRule final : public OptimizeTask
{
public:
    ApplyRule(GroupNode & group_node_, const Optimizer::Transformation & transform_rule_, TaskContextPtr task_context_);

    void execute() override;

    String getDescription() override;

private:
    GroupNode & group_node;

    const Optimizer::Transformation & transform_rule;
};

}
