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
        Frame * parent = nullptr;

        /// If not zero, traverse only traverse_depth_limit layers of tree (if no other optimizations happen).
        /// Otherwise, traverse all children.
        size_t traverse_depth_limit = 0;

        /// Next child to process.
        size_t next_child = 0;

        /// If not zero, optimizations to current node again.
        /// Skip optimizations which read less then `read_depth_limit` layers of tree.
        size_t read_depth_limit = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        /// If traverse_depth_limit == 0, then traverse without limit (first entrance)
        /// If traverse_depth_limit > 1, then traverse with (limit - 1)
        if (frame.traverse_depth_limit != 1)
        {
            /// Traverse all children first.
            if (frame.next_child < frame.node->children.size())
            {
                stack.push(Frame
                {
                       .node = frame.node->children[frame.next_child],
                       .parent = &frame,
                       .traverse_depth_limit = frame.traverse_depth_limit ? (frame.traverse_depth_limit - 1) : 0,
                });

                ++frame.next_child;
                continue;
            }
        }

        /// If frame.traverse_depth_limit == 0, apply optimizations on first entrance.
        /// If frame.read_depth_limit, then one of children was updated, and we may need to repeat some optimizations.
        if (frame.traverse_depth_limit == 0 || frame.read_depth_limit)
        {
            size_t max_update_depth = 0;

            /// Apply all optimizations.
            for (const auto & optimization : optimizations)
            {
                /// Just in case, skip optimization if it is not initialized.
                if (!optimization.run)
                    continue;

                /// Skip optimization if read_depth_limit is applied.
                if (frame.read_depth_limit && optimization.read_depth <= frame.read_depth_limit)
                    continue;

                /// Try to apply optimization.
                if (optimization.run(frame.node, nodes))
                    max_update_depth = std::max<size_t>(max_update_depth, optimization.update_depth);
            }

            /// Nothing was applied.
            if (max_update_depth == 0)
            {
                stack.pop();
                continue;
            }

            /// Traverse `max_update_depth` layers of tree again.
            frame.traverse_depth_limit = max_update_depth;
            frame.next_child = 0;

            /// Also go to parents and tell them to apply some optimizations again.
            /// Check: for our parent we set read_depth_limit = 1, which means it can skip optimizations
            ///        which use ony 1 layer of tree (not read current node).
            /// Note that frame.read_depth_limit will be zeroed.
            Frame * cur_frame = &frame;
            for (size_t cur_depth = 0; cur_frame && cur_frame->traverse_depth_limit; ++cur_depth)
            {
                /// If cur_frame is traversed first time, all optimizations will apply anyway.
                if (cur_frame->traverse_depth_limit == 0)
                    break;

                /// Stop if limit is applied and stricter then current.
                if (cur_frame->read_depth_limit && cur_frame->read_depth_limit <= cur_depth)
                    break;

                cur_frame->read_depth_limit = cur_depth;
                cur_frame = cur_frame->parent;
            }

            continue;
        }

        stack.pop();
    }
}

}
