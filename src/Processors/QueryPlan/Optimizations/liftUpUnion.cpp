#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Core/Block.h>

namespace DB::QueryPlanOptimizations
{

bool canPushStepThroughUnion(const UnionStep & union_step)
{
    const auto & union_output = *union_step.getOutputHeader();
    for (const auto & input_header : union_step.getInputHeaders())
    {
        /// Reject a branch whose physical structure diverges from the union output, e.g. a
        /// branch that constant-folds a column to Const (or diverges in Sparse/Replicated
        /// representation). The union normalizes such a branch at runtime; pushing a step down
        /// would give it a full input header and move the mismatch into the optimizer's own
        /// header validation. blocksHaveEqualStructure compares column count, names, types and
        /// physical column structure.
        if (!blocksHaveEqualStructure(*input_header, union_output))
            return false;

        /// blocksHaveEqualStructure compares types via IDataType::equals, which is tolerant of
        /// two AggregateFunction types sharing a state representation but carrying different type
        /// names (quantileExactTuple vs quantilesExactTuple(0.9)). The union coerces such a branch
        /// at runtime, so also reject any column whose type name differs from the union output.
        for (size_t col = 0; col < input_header->columns(); ++col)
            if (input_header->getByPosition(col).type->getName() != union_output.getByPosition(col).type->getName())
                return false;
    }
    return true;
}

size_t tryLiftUpUnion(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & /*settings*/)
{
    if (parent_node->children.empty())
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();
    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * union_step = typeid_cast<UnionStep *>(child.get());
    if (!union_step)
        return 0;

    /// The rewrites below push the parent into every branch, assuming the union forwards each
    /// branch unchanged. Bail out when a branch diverges from the union output either physically
    /// (e.g. it drops a Const that diverged across branches) or only loosely by type name (same
    /// state representation, different type name) -- see canPushStepThroughUnion.
    if (!canPushStepThroughUnion(*union_step))
        return 0;

    if (auto * expression = typeid_cast<ExpressionStep *>(parent.get()))
    {
        /// Union does not change header.
        /// We can push down expression and update header.
        auto union_input_headers = child->getInputHeaders();
        auto expected_output = expression->getOutputHeader();

        for (auto & input_header : union_input_headers)
            input_header = expected_output;

        ///                    - Something
        /// Expression - Union - Something
        ///                    - Something

        child = std::make_unique<UnionStep>(union_input_headers, union_step->getMaxThreads(), union_step->isNarrowingAllowed());

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
            expr_node.step->setStepDescription(*expression);
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

            distinct_node.step->setStepDescription(*distinct);
        }

        ///       - Distinct - Something
        /// Union - Distinct - Something
        ///       - Distinct - Something

        return 3;
    }

    return 0;
}

}
