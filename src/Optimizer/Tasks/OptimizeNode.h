#pragma once

#include <Optimizer/Tasks/OptimizeTask.h>

namespace DB
{

class GroupNode;
using GroupNodePtr = std::shared_ptr<GroupNode>;

class OptimizeNode final : public OptimizeTask
{
public:
    OptimizeNode(GroupNodePtr group_node_, TaskContextPtr task_context_);

    void execute() override;

    String getDescription() override;

private:
    GroupNodePtr group_node;
};

}
