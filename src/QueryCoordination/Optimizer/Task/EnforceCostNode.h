#pragma once

#include <QueryCoordination/Optimizer/Task/Task.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class Memo;
class Group;
class GroupNode;
class PhysicalProperties;

class EnforceCostNode final : public Task
{
public:
    EnforceCostNode(Memo & memo_, std::stack<std::unique_ptr<Task>> & stack_, ContextPtr context_, Group & group_, GroupNode & group_node_, const PhysicalProperties & required_prop_)
        : Task(memo_, stack_, context_), group(group_), group_node(group_node_), required_prop(required_prop_)
    {
    }

    void execute() override;

    std::unique_ptr<EnforceCostNode> clone();

private:
    void enforceGroupNode(
        const PhysicalProperties & output_prop,
        std::vector<std::pair<GroupNode, PhysicalProperties>> & collection);

    Group & group;
    GroupNode & group_node;
    const PhysicalProperties & required_prop;
};

}
