#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

void tryRemoveRedundantOrderBy(QueryPlan::Node * root)
{
    // do top down find first order by or group by
    struct Frame
    {
        QueryPlan::Node * node = nullptr;
        QueryPlan::Node * parent_node = nullptr;
    };

    std::vector<Frame> stack;
    stack.push_back({.node = root});

    std::vector<IQueryPlanStep *> steps_affect_order;

    while (!stack.empty())
    {
        auto frame = stack.back();
        stack.pop_back();

        QueryPlan::Node * current_node = frame.node;
        IQueryPlanStep * current_step = frame.node->step.get();
        if (!steps_affect_order.empty())
        {
            if (SortingStep * ss = typeid_cast<SortingStep *>(current_step); ss)
            {
                auto try_to_remove_sorting_step = [&]() -> bool
                {
                    /// if there are LIMITs on top of ORDER BY, the ORDER BY is non-removable
                    /// if ORDER BY is with FILL WITH, it is non-removable
                    if (typeid_cast<LimitStep *>(steps_affect_order.back()) || typeid_cast<LimitByStep *>(steps_affect_order.back())
                        || typeid_cast<FillingStep *>(steps_affect_order.back()))
                        return false;

                    bool remove_sorting = false;
                    /// (1) aggregation
                    if (const AggregatingStep * parent_aggr = typeid_cast<AggregatingStep *>(steps_affect_order.back()); parent_aggr)
                    {
                        /// check if it contains aggregation functions which depends on order
                    }
                    /// (2) sorting
                    else if (SortingStep * parent_sorting = typeid_cast<SortingStep *>(steps_affect_order.back()); parent_sorting)
                    {
                        remove_sorting = true;
                    }

                    if (remove_sorting)
                    {
                        /// need to remove sorting and its expression from plan
                        QueryPlan::Node * parent = frame.parent_node;

                        QueryPlan::Node * next_node = !current_node->children.empty() ? current_node->children.front() : nullptr;
                        if (next_node && typeid_cast<ExpressionStep *>(next_node->step.get()))
                            next_node = !current_node->children.empty() ? current_node->children.front() : nullptr;

                        if (next_node)
                            parent->children[0] = next_node;
                    }
                    return remove_sorting;
                };
                if (try_to_remove_sorting_step())
                {
                    /// current step was removed from plan, its parent has new children, need to visit them
                    for (auto * child : frame.parent_node->children)
                        stack.push_back({.node = child, .parent_node = frame.parent_node});

                    continue;
                }
            }
        }

        if (typeid_cast<LimitStep *>(current_step)
            || typeid_cast<LimitByStep *>(current_step) /// (1) if there are LIMITs on top of ORDER BY, the ORDER BY is non-removable
            || typeid_cast<FillingStep *>(current_step) /// (2) if ORDER BY is with FILL WITH, it is non-removable
            || typeid_cast<SortingStep *>(current_step) /// (3) ORDER BY will change order of previous sorting
            || typeid_cast<AggregatingStep *>(current_step)) /// (4) aggregation change order
            steps_affect_order.push_back(current_step);

        /// visit children
        for (auto * child : current_node->children)
            stack.push_back({.node = child, .parent_node = current_node});

        /// if all children of a particular parent are visited
        /// then the parent need to be removed from nodes which affects order, if it's such node
        if (!steps_affect_order.empty() && frame.parent_node)
        {
            Frame next_frame;
            if (!stack.empty())
                next_frame = stack.back();

            /// if next frame node is not child of current node,
            /// and it doesn't have the same parent as current frame
            /// then we are visiting last child of the parent.
            /// So, remove the parent if it's on top of stack with affecting order nodes
            if (next_frame.parent_node != frame.node && next_frame.parent_node != frame.parent_node
                && steps_affect_order.back() == frame.parent_node->step.get())
                steps_affect_order.pop_back();
        }
    }
}

}
