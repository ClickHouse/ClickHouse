#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Common/Exception.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Pushes a partial aggregation below a join (eager aggregation) as a cost-based alternative,
/// gated by `cascades_aggregation_pushdown`.
class AggregationPushdown : public IOptimizationRule
{
public:
    String getName() const override { return "AggregationPushdown"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 2000; }
    bool isTransformation() const override { return true; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool AggregationPushdown::checkPattern(GroupExpressionPtr /*expression*/, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    /// The matching logic is not implemented yet.
    return false;
}

std::vector<GroupExpressionPtr> AggregationPushdown::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & /*memo*/) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "AggregationPushdown::applyImpl called, but checkPattern always returns false for expression '{}'",
        expression->getDescription());
}

OptimizationRulePtr createAggregationPushdown() { return std::make_shared<AggregationPushdown>(); }

}
