#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/DistinctStep.h>

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
        auto union_input_headers = child->getInputHeaders();
        for (auto & input_header : union_input_headers)
            input_header = expression->getOutputHeader();

        ///                    - Something
        /// Expression - Union - Something
        ///                    - Something

        child = std::make_unique<UnionStep>(union_input_headers, union_step->getMaxThreads());

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
                expr_node.children.front()->step->getOutputHeader(),
                expression->getExpression().clone());
            expr_node.step->setStepDescription(expression->getStepDescription());
        }

        ///       - Expression - Something
        /// Union - Expression - Something
        ///       - Expression - Something

        return 3;
    }

    if (auto * distinct = typeid_cast<DistinctStep *>(parent.get()); distinct && distinct->isPreliminary())
    {
        /// Union does not change header. Distinct as well.

        ///                  - Something
        /// Distinct - Union - Something
        ///                  - Something

        std::swap(parent, child);
        std::swap(parent_node->children, child_node->children);
        std::swap(parent_node->children.front(), child_node->children.front());

        ///       - Distinct - Something
        /// Union - Something
        ///       - Something

        for (size_t i = 1; i < parent_node->children.size(); ++i)
        {
            auto & distinct_node = nodes.emplace_back();
            distinct_node.children.push_back(parent_node->children[i]);
            parent_node->children[i] = &distinct_node;

            distinct_node.step = std::make_unique<DistinctStep>(
                distinct_node.children.front()->step->getOutputHeader(),
                distinct->getSetSizeLimits(),
                distinct->getLimitHint(),
                distinct->getColumnNames(),
                distinct->isPreliminary());
        }

        ///       - Distinct - Something
        /// Union - Distinct - Something
        ///       - Distinct - Something

        return 3;
    }

    return 0;
}

}
