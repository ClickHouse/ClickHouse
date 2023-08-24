#pragma once


namespace DB
{

class Group;
class GroupNode;
class PhysicalProperties;

class OptimizeGroupNode
{
public:
    OptimizeGroupNode(Group & group_, GroupNode & group_node_, const PhysicalProperties & required_prop_) : group(group_), group_node(group_node_), required_prop(required_prop_) {}

    void execute();

private:
    Group & group;
    GroupNode & group_node;
    const PhysicalProperties & required_prop;
};

}
