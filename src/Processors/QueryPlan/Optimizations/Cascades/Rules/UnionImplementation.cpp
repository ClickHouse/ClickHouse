#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// `UNION ALL` concatenates its inputs, so a union of each node's share of every input is that
/// node's share of the result. Running it on every node keeps the work above it (for example
/// the partial half of a two-stage aggregation) distributed instead of gathering all inputs.
///
/// Only a union the query asks for (`isNarrowingAllowed`) runs on several nodes: the consumer
/// of any other union reads each input as its own stream (for example sequences of aggregation
/// buckets), and an exchange would mix those streams. Inputs with requirements of their own
/// (a sorting stripped below the union) stay on one node.
class UnionImplementation : public IOptimizationRule
{
public:
    [[nodiscard]] String getName() const override { return "Union"; }

    [[nodiscard]] bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const override
    {
        return typeid_cast<const UnionStep *>(expression->getQueryPlanStep()) != nullptr;
    }

    [[nodiscard]] Promise getPromise() const override { return 1; }
    [[nodiscard]] bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override
    {
        std::vector<GroupExpressionPtr> result;
        addPhysicalToMemo(std::make_shared<GroupExpression>(*expression), required_properties, memo, result);

        if (!typeid_cast<const UnionStep &>(*expression->getQueryPlanStep()).isNarrowingAllowed())
            return result;
        for (const auto & input : expression->inputs)
            if (!(input.required_properties == ExpressionProperties{}))
                return result;

        auto node_counts = getCandidateNodeCounts(memo.getContext().cluster_node_count);
        if (required_properties.distribution.node_count > 1)
            node_counts.push_back(required_properties.distribution.node_count);

        for (size_t node_count : node_counts)
        {
            auto implementation = std::make_shared<GroupExpression>(*expression);
            for (auto & input : implementation->inputs)
                input.required_properties.distribution.node_count = node_count;
            implementation->properties.distribution.node_count = node_count;
            addPhysicalToMemo(implementation, required_properties, memo, result);
        }

        return result;
    }
};

OptimizationRulePtr createUnionImplementation() { return std::make_shared<UnionImplementation>(); }

}
