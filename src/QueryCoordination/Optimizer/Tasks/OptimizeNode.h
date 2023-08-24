#pragma once

#include <QueryCoordination/Optimizer/Tasks/OptimizeTask.h>

namespace DB
{

class GroupNode;

class OptimizeNode final : public OptimizeTask
{
public:
    OptimizeNode(GroupNode & group_node_, TaskContextPtr task_context_);

    void execute() override;

    String getDescription() override;

private:
    GroupNode & group_node;
};

}
