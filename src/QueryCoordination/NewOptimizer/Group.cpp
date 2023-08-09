#include <QueryCoordination/NewOptimizer/Group.h>

namespace DB
{

void Group::addGroupNode(const GroupNode & group_plan_node)
{
    group_nodes.emplace_back(group_plan_node);
}

}
