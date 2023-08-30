#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Common/Exception.h>
#include <stack>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_QUERY_PLAN_OPTIMIZATIONS;
}

namespace QueryPlanOptimizations
{

void optimizeTree(const QueryPlanOptimizationSettings & settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes)
{
    if (!settings.optimize_plan)
        return;

    const auto & optimizations = getOptimizations();

    struct Frame
    {
        QueryPlan::Node * node = nullptr;

        /// If not zero, traverse only depth_limit layers of tree (if no other optimizations happen).
        /// Otherwise, traverse all children.
        size_t depth_limit = 0;

        /// Next child to process.
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push({.node = &root});

    size_t max_optimizations_to_apply = settings.max_optimizations_to_apply;
    size_t total_applied_optimizations = 0;

    while (!stack.empty())
    {
        auto & frame = stack.top();

        /// If traverse_depth_limit == 0, then traverse without limit (first entrance)
        /// If traverse_depth_limit > 1, then traverse with (limit - 1)
        if (frame.depth_limit != 1)
        {
            /// Traverse all children first.
            if (frame.next_child < frame.node->children.size())
            {
                stack.push(
                {
                    .node = frame.node->children[frame.next_child],
                    .depth_limit = frame.depth_limit ? (frame.depth_limit - 1) : 0,
                });

                ++frame.next_child;
                continue;
            }
        }

        size_t max_update_depth = 0;

        /// Apply all optimizations.
        for (const auto & optimization : optimizations)
        {
            if (!(settings.*(optimization.is_enabled)))
                continue;

            /// Just in case, skip optimization if it is not initialized.
            if (!optimization.apply)
                continue;

            if (max_optimizations_to_apply && max_optimizations_to_apply < total_applied_optimizations)
                throw Exception(ErrorCodes::TOO_MANY_QUERY_PLAN_OPTIMIZATIONS,
                                "Too many optimizations applied to query plan. Current limit {}",
                                max_optimizations_to_apply);

            /// Try to apply optimization.
            auto update_depth = optimization.apply(frame.node, nodes);
            if (update_depth)
                ++total_applied_optimizations;
            max_update_depth = std::max<size_t>(max_update_depth, update_depth);
        }

        /// Traverse `max_update_depth` layers of tree again.
        if (max_update_depth)
        {
            frame.depth_limit = max_update_depth;
            frame.next_child = 0;
            continue;
        }

        /// Nothing was applied.
        stack.pop();
    }
}

}
}
