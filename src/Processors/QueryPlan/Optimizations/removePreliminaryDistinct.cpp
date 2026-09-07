#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

namespace DB::QueryPlanOptimizations
{

/// A final DISTINCT that hash-partitions its input, or keeps disjoint streams, deduplicates every stream completely
/// on its own, so a preliminary DISTINCT before it only hashes every row once more. It pays off only when the
/// final DISTINCT merges the streams into one (sorted input, global size limits, or a single thread), or when it
/// deduplicates sorted streams without a hash table.
void removePreliminaryDistinct(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root)
{
    if (!optimization_settings.allow_parallel_final_distinct || optimization_settings.max_threads <= 1)
        return;

    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        if (frame.next_child < frame.node->children.size())
        {
            stack.push_back({.node = frame.node->children[frame.next_child++]});
            continue;
        }

        auto * node = frame.node;
        stack.pop_back();

        const auto * distinct = typeid_cast<const DistinctStep *>(node->step.get());
        if (!distinct || distinct->isPreliminary() || !distinct->getSortDescription().empty() || distinct->getSetSizeLimits().hasLimits()
            || node->children.size() != 1)
            continue;

        auto * child = node->children.front();
        const auto * preliminary_distinct = typeid_cast<const DistinctStep *>(child->step.get());
        if (!preliminary_distinct || !preliminary_distinct->isPreliminary() || !preliminary_distinct->getSortDescription().empty()
            || preliminary_distinct->getColumnNames() != distinct->getColumnNames() || child->children.size() != 1)
            continue;

        node->children = child->children;
    }
}

}
