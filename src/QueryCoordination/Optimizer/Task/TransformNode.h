#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <QueryCoordination/Optimizer/Task/Task.h>

namespace DB
{

class Memo;
class Group;
class GroupNode;

class TransformNode final : public Task
{
public:
    TransformNode(Memo & memo_, std::stack<std::unique_ptr<Task>> & stack_, ContextPtr context_, Group & group_, GroupNode & group_node_)
        : Task(memo_, stack_, context_), group(group_), group_node(group_node_)
    {
    }

    void execute() override;

private:
    Group & group;
    GroupNode & group_node;
};

}
