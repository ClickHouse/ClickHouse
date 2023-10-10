#pragma once

#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/Rule/Rule.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeTask.h>

namespace DB
{

class ApplyRule final : public OptimizeTask
{
public:
    ApplyRule(GroupNodePtr group_node_, RulePtr rule_, TaskContextPtr task_context_);

    void execute() override;

    String getDescription() override;

private:
    GroupNodePtr group_node;

    RulePtr rule;
};

}
