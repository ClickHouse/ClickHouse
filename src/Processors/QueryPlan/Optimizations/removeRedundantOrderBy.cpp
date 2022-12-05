#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{
const char * stepName(const QueryPlan::Node * node)
{
    IQueryPlanStep * current_step = node->step.get();
    return typeid(*current_step).name();
}

void printStepName(const char * prefix, const QueryPlan::Node * node)
{
    LOG_DEBUG(&Poco::Logger::get("RedundantOrderBy"), "{}: {}: {}", prefix, stepName(node), reinterpret_cast<void *>(node->step.get()));
}

void tryRemoveRedundantOrderBy(QueryPlan::Node * root)
{
    // do top down find first order by or group by
    struct Frame
    {
        QueryPlan::Node * node = nullptr;
        QueryPlan::Node * parent_node = nullptr;
        size_t next_child = 0;
    };

    std::vector<Frame> stack;
    stack.push_back({.node = root});

    std::vector<IQueryPlanStep *> steps_affect_order;

    while (!stack.empty())
    {
        auto & frame = stack.back();

        QueryPlan::Node * current_node = frame.node;
        QueryPlan::Node * parent_node = frame.parent_node;
        IQueryPlanStep * current_step = frame.node->step.get();
        printStepName("back", current_node);

        /// top-down visit
        if (0 == frame.next_child)
        {
            printStepName("visit", current_node);
            /// if there is parent node which can affect order and current step is sorting
            /// then check if we can remove the sorting step (and corresponding expression step)
            if (!steps_affect_order.empty() && typeid_cast<SortingStep *>(current_step))
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
                        /// TODO: check if it contains aggregation functions which depends on order
                        remove_sorting = true;
                    }
                    /// (2) sorting
                    else if (SortingStep * parent_sorting = typeid_cast<SortingStep *>(steps_affect_order.back()); parent_sorting)
                    {
                        remove_sorting = true;
                    }

                    if (remove_sorting)
                    {
                        chassert(typeid_cast<ExpressionStep *>(current_node->children.front()->step.get()));
                        chassert(!current_node->children.front()->children.empty());

                        /// need to remove sorting and its expression from plan
                        parent_node->children.front() = current_node->children.front()->children.front();
                    }
                    return remove_sorting;
                };
                if (try_to_remove_sorting_step())
                {
                    LOG_DEBUG(&Poco::Logger::get("RedundantOrderBy"), "Sorting removed");

                    /// mark removed node as visited
                    frame.next_child = frame.node->children.size();

                    /// current sorting step has been removed from plan, its parent has new children, need to visit them
                    auto next_frame = Frame{.node = parent_node->children[0], .parent_node = parent_node};
                    ++frame.next_child;
                    printStepName("push", next_frame.node);
                    stack.push_back(next_frame);
                    continue;
                }
            }

            if (typeid_cast<LimitStep *>(current_step)
                || typeid_cast<LimitByStep *>(current_step) /// (1) if there are LIMITs on top of ORDER BY, the ORDER BY is non-removable
                || typeid_cast<FillingStep *>(current_step) /// (2) if ORDER BY is with FILL WITH, it is non-removable
                || typeid_cast<SortingStep *>(current_step) /// (3) ORDER BY will change order of previous sorting
                || typeid_cast<AggregatingStep *>(current_step)) /// (4) aggregation change order
            {
                printStepName("steps_affect_order/push", current_node);
                steps_affect_order.push_back(current_step);
            }
        }

        /// Traverse all children
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = current_node->children[frame.next_child], .parent_node = current_node};
            ++frame.next_child;
            printStepName("push", next_frame.node);
            stack.push_back(next_frame);
            continue;
        }

        /// bottom-up visit
        if (!steps_affect_order.empty() && steps_affect_order.back() == current_node->step.get())
        {
            printStepName("steps_affect_order/pop", current_node);
            steps_affect_order.pop_back();
        }

        printStepName("pop", current_node);
        stack.pop_back();
    }
}
}
