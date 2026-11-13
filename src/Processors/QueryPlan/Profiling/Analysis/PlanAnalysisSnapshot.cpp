#include <Processors/QueryPlan/Profiling/Analysis/PlanAnalysisSnapshot.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>

#include <algorithm>

namespace DB
{

PlanAnalysisSnapshot PlanAnalysisSnapshot::capture(const PlanWalk & walk)
{
    PlanAnalysisSnapshot snapshot;
    snapshot.root_id = walk.root_id;
    snapshot.analyzers.reserve(walk.steps.size());

    for (const auto & visited : walk.steps)
    {
        StepAnalyzer analyzer = visited.step->makeAnalyzer();
        analyzer.id = visited.id;
        analyzer.children = visited.children;
        snapshot.analyzers.emplace(visited.id, std::move(analyzer));
    }

    return snapshot;
}

const StepAnalyzer * PlanAnalysisSnapshot::find(const String & step_id) const
{
    const auto it = analyzers.find(step_id);
    return it != analyzers.end() ? &it->second : nullptr;
}

std::vector<const StepAnalyzer *> PlanAnalysisSnapshot::stepsInPostOrder() const
{
    const auto * root = find(root_id);
    if (!root)
        return {};

    std::vector<const StepAnalyzer *> post_order;
    std::vector<const StepAnalyzer *> to_visit{root};

    while (!to_visit.empty())
    {
        const auto * analyzer = to_visit.back();
        to_visit.pop_back();
        post_order.push_back(analyzer);

        for (const auto & child_id : analyzer->children)
            if (const auto * child = find(child_id))
                to_visit.push_back(child);
    }

    std::reverse(post_order.begin(), post_order.end());
    return post_order;
}

}
