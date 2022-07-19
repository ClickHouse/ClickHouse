#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Interpreters/ActionsDAG.h>
#include <stack>

namespace DB::QueryPlanOptimizations
{

void optimizePrimaryKeyCondition(QueryPlan::Node & root)
{
    struct Frame
    {
        QueryPlan::Node * node = nullptr;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            stack.push({.node = frame.node->children[frame.next_child]});

            ++frame.next_child;
            continue;
        }

        if (auto * filter_step = typeid_cast<FilterStep *>(frame.node->step.get()))
        {
            auto * child = frame.node->children.at(0);
            if (auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(child->step.get()))
                read_from_merge_tree->addFilter(filter_step->getExpression(), filter_step->getFilterColumnName());

        }

        stack.pop();
    }
}

}
