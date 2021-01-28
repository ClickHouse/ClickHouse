#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <stack>

namespace DB::QueryPlanOptimizations
{

void optimizeTree(QueryPlan::Node & root, QueryPlan::Nodes & nodes)
{
    const auto & optimizations = getOptimizations();

    struct Frame
    {
        QueryPlan::Node * node;

        /// If not zero, traverse only depth_limit layers of tree (if no other optimizations happen).
        /// Otherwise, traverse all children.
        size_t depth_limit = 0;

        /// Next child to process.
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = &root});

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
                stack.push(Frame
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
            /// Just in case, skip optimization if it is not initialized.
            if (!optimization.run)
                continue;

            /// Try to apply optimization.
            if (optimization.run(frame.node, nodes))
                max_update_depth = std::max<size_t>(max_update_depth, optimization.update_depth);
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
