#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Unsorted single-node read: fallback for `ReadFromMergeTree` at {1 node}.
/// `ReadFromMergeTree` is excluded from `DefaultImplementation` so that specialized
/// read rules (`ParallelRead`, `ReplicatedRead`) handle it.
class LocalReadImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "LocalRead"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const override
    {
        return typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep()) != nullptr;
    }
    Promise getPromise() const override { return 1; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override
    {
        auto implementation_expression = std::make_shared<GroupExpression>(*expression);
        /// No distribution propagation: output stays at default {1 node}.
        return addPhysicalToMemo(implementation_expression, required_properties, memo);
    }
};

OptimizationRulePtr createLocalReadImplementation() { return std::make_shared<LocalReadImplementation>(); }

}
