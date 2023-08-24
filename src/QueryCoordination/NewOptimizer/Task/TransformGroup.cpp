#include <QueryCoordination/NewOptimizer/Task/TransformGroup.h>
#include <QueryCoordination/NewOptimizer/Group.h>

namespace DB
{

void TransformGroup::execute()
{
    const auto & group_nodes = group.getGroupNodes();

    for (const auto & group_node : group_nodes)
    {
        /// push Task TransformGroupNode
    }
}

}

