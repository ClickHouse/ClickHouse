#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/StorageMerge.h>
#include <Interpreters/ActionsDAG.h>
#include <deque>

namespace DB::QueryPlanOptimizations
{

void optimizePrimaryKeyCondition(QueryPlan::Node & root)
{
    struct Frame
    {
        QueryPlan::Node * node = nullptr;
        size_t next_child = 0;
    };

    std::deque<Frame> stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            stack.push_back({.node = frame.node->children[frame.next_child]});

            ++frame.next_child;
            continue;
        }

        auto add_filter = [&](auto & storage)
        {
            for (auto iter=stack.rbegin() + 1; iter!=stack.rend(); ++iter)
            {
                if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
                    storage.addFilter(filter_step->getExpression(), filter_step->getFilterColumnName());
                else if (typeid_cast<ExpressionStep *>(iter->node->step.get()))
                    ;
                else
                    break;
            }
        };

        if (auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(frame.node->step.get()))
            add_filter(*read_from_merge_tree);
        else if (auto * read_from_merge = typeid_cast<ReadFromMerge *>(frame.node->step.get()))
            add_filter(*read_from_merge);

        stack.pop_back();
    }
}

}
