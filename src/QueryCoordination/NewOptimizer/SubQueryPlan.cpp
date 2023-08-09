#include <QueryCoordination/NewOptimizer/SubQueryPlan.h>


namespace DB
{

void SubQueryPlan::addStep(QueryPlanStepPtr step)
{
    if (root)
    {
        const auto & root_header = root->step->getOutputStream().header;
        const auto & step_header = step->getInputStreams().front().header;
        if (!blocksHaveEqualStructure(root_header, step_header))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot add step {} to QueryPlan because it has incompatible header with root step {} root header: {} step header: {}",
                step->getName(),
                root->step->getName(),
                root_header.dumpStructure(),
                step_header.dumpStructure());

        nodes.emplace_back(Node{.step = std::move(step), .children = {root}});
        root = &nodes.back();
        return;
    }
    else
    {
        nodes.emplace_back(Node{.step = std::move(step)});
        root = &nodes.back();
        return;
    }
}

}
