#pragma once

#include <Optimizer/Tasks/OptimizeTask.h>

namespace DB
{

class GroupNode;
using GroupNodePtr = std::shared_ptr<GroupNode>;

class DeriveStats final : public OptimizeTask
{
public:
    DeriveStats(GroupNodePtr group_node_, bool need_derive_child_, TaskContextPtr task_context_);

    void execute() override;

    String getDescription() override;

private:
    void deriveStats();

    OptimizeTaskPtr clone(bool need_derive_child_);

    GroupNodePtr group_node;

    bool need_derive_child;
};

}
