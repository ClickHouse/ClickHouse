#include <QueryCoordination/NewOptimizer/Group.h>

namespace DB
{

const GroupNode & Group::addGroupNode(const GroupNode & group_plan_node)
{
    group_nodes.emplace_back(group_plan_node);
    return group_nodes.back();
}

}
