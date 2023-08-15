#include <QueryCoordination/NewOptimizer/Group.h>

namespace DB
{

GroupNode & Group::addGroupNode(GroupNode & group_plan_node)
{
    group_nodes.emplace_back(std::move(group_plan_node));
    return group_nodes.back();
}

}
