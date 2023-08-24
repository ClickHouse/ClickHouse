#pragma once

#include <QueryCoordination/Optimizer/Tasks/OptimizeTask.h>

namespace DB
{

class GroupNode;

class DeriveStats final : public OptimizeTask
{
public:
    DeriveStats(GroupNode & group_node_, bool need_derive_child_, TaskContextPtr task_context_);

    void execute() override;

    String getDescription() override;

private:
    void deriveStats();

    OptimizeTaskPtr clone(bool need_derive_child_);

    GroupNode & group_node;

    bool need_derive_child;
};

}
