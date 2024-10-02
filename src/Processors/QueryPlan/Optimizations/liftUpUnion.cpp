#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB::QueryPlanOptimizations
{

size_t tryLiftUpUnion(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes)
{
    if (parent_node->children.empty())
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();
    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * union_step = typeid_cast<UnionStep *>(child.get());
    if (!union_step)
        return 0;

    if (auto * expression = typeid_cast<ExpressionStep *>(parent.get()))
    {
        /// Union does not change header.
        /// We can push down expression and update header.
        auto union_input_streams = child->getInputStreams();
        for (auto & input_stream : union_input_streams)
            input_stream.header = expression->getOutputStream().header;

        ///                    - Something
        /// Expression - Union - Something
        ///                    - Something

        child = std::make_unique<UnionStep>(union_input_streams, union_step->getMaxThreads());

        std::swap(parent, child);
        std::swap(parent_node->children, child_node->children);
        std::swap(parent_node->children.front(), child_node->children.front());

        ///       - Expression - Something
        /// Union - Something
        ///       - Something

        for (size_t i = 1; i < parent_node->children.size(); ++i)
        {
            auto & expr_node = nodes.emplace_back();
            expr_node.children.push_back(parent_node->children[i]);
            parent_node->children[i] = &expr_node;

            expr_node.step = std::make_unique<ExpressionStep>(
                expr_node.children.front()->step->getOutputStream(),
                expression->getExpression().clone());
        }

        ///       - Expression - Something
        /// Union - Expression - Something
        ///       - Expression - Something

        return 3;
    }

    return 0;
}

}
