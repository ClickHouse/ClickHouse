#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class Group;
class GroupNode;

class TransformGroupNode
{
public:
    TransformGroupNode(Group & group_, GroupNode & group_node_, ContextPtr query_context_) : group(group_), group_node(group_node_), query_context(query_context_) {}

    void execute();

private:
    Group & group;
    GroupNode & group_node;

    ContextPtr query_context;
};

}
