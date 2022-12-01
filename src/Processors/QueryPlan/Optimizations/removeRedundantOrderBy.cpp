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
    Stack stack;
    stack.push_back({.node = root});

    std::vector<IQueryPlanStep *> steps_affect_order; /// aggregation or sorting

    while (!stack.empty())
    {
        auto & frame = stack.back();

        QueryPlan::Node * current_node = frame.node;
        IQueryPlanStep * current_step = frame.node->step.get();
        if (!steps_affect_order.empty())
        {
            while (true)
            {
                if (SortingStep * ss = typeid_cast<SortingStep *>(current_step); ss)
                {
                    /// if there are LIMITs on top of ORDER BY, the ORDER BY is non-removable
                    /// if ORDER BY is with FILL WITH, it is non-removable
                    if (typeid_cast<LimitStep *>(steps_affect_order.back()) || typeid_cast<LimitByStep *>(steps_affect_order.back())
                        || typeid_cast<FillingStep *>(steps_affect_order.back()))
                        break;

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
                        QueryPlan::Node * parent = stack.back().node;
                        chassert(parent->children.front() == current_node);

                        QueryPlan::Node * next_node = !current_node->children.empty() ? current_node->children.front() : nullptr;
                        if (next_node && typeid_cast<ExpressionStep *>(next_node->step.get()))
                            next_node = !current_node->children.empty() ? current_node->children.front() : nullptr;

                        if (next_node)
                            parent->children[0] = next_node;
                    }
                }
            }
        }

        if (typeid_cast<LimitStep *>(current_step)
            || typeid_cast<LimitByStep *>(current_step) /// if there are LIMITs on top of ORDER BY, the ORDER BY is non-removable
            || typeid_cast<FillingStep *>(current_step) /// if ORDER BY is with FILL WITH, it is non-removable
            || typeid_cast<SortingStep *>(current_step) /// ORDER BY will change order of previous sorting
            || typeid_cast<AggregatingStep *>(current_step)) /// aggregation change order
            steps_affect_order.push_back(current_step);

        /// visit children if there are non-visited
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
        }
        /// all children are visited
        else
        {
            if (!steps_affect_order.empty() && current_step == steps_affect_order.back())
                steps_affect_order.pop_back();

            stack.pop_back();
        }
    }
}

}
