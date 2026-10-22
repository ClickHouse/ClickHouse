#include <Processors/QueryPlan/Profiling/Analysis/PlanAnalysisSnapshot.h>

#include <Processors/QueryPlan/JoinStep.h>
#include <Common/typeid_cast.h>

namespace DB
{

PlanAnalysisSnapshot PlanAnalysisSnapshot::capture(const PlanWalk & walk)
{
    PlanAnalysisSnapshot snapshot;
    snapshot.root_id = walk.root_id;
    snapshot.shape.reserve(walk.steps.size());
    snapshot.analyzers.reserve(walk.steps.size());

    for (const auto & visited : walk.steps)
    {
        PlanShapeNode node;
        node.id = visited.id;
        node.children = visited.children;

        if (const auto * join_step = typeid_cast<const JoinStep *>(visited.step))
        {
            node.is_join = true;
            node.cluster_id = join_step->getClusterId();
        }

        snapshot.shape.push_back(std::move(node));
        snapshot.analyzers.emplace(visited.id, visited.step->makeAnalyzer());
    }

    return snapshot;
}

}
