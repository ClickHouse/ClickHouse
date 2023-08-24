#pragma once

#include <QueryCoordination/Optimizer/Task/Task.h>
#include <QueryCoordination/Optimizer/Group.h>
#include <QueryCoordination/Optimizer/GroupNode.h>

namespace DB
{

class DeriveStatNode final : public Task
{
public:
    DeriveStatNode(Memo & memo_, std::stack<std::unique_ptr<Task>> & stack_, ContextPtr context_, Group & group_, GroupNode & group_node_)
        : Task(memo_, stack_, context_), group(group_), group_node(group_node_)
    {
    }

    void execute() override;

    std::unique_ptr<DeriveStatNode> clone()
    {
        return std::make_unique<DeriveStatNode>(memo, stack, context, group, group_node);
    }

private:
    Group & group;
    GroupNode & group_node;
};

}
