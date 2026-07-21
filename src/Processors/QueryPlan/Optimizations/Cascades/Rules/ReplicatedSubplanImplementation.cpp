#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Whether this step gives the same result on every node when run over identical inputs.
/// Steps not proven safe return false.  Not listed on purpose: reads have their own
/// rule (`ReplicatedRead`), per-row steps are covered by `DistributionPassthrough`, top-N
/// and LIMIT pick different rows on different nodes, and aggregations can diverge
/// (order-dependent functions, floating-point accumulation) until a per-function gate
/// exists.
static bool isReplicationSafe(const IQueryPlanStep & step)
{
    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(&step))
        /// A hash join is deterministic; only a non-deterministic function in the join
        /// condition or residual filter could make per-node results differ.
        return !QueryPlanOptimizations::dagContainsNonDeterministicFunction(join_step->getActionsDAG());
    return false;
}

/// Recomputes a step on every node instead of computing it once and broadcasting the
/// result: all inputs are required replicated, the output is replicated.  Through the
/// input groups this extends to whole subtrees of safe steps, replicated reads, and
/// broadcast results.  Only fires when the parent requires a replicated result (like
/// `ReplicatedRead`): the variant satisfies nothing else.
class ReplicatedSubplanImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "ReplicatedSubplan"; }

    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const override
    {
        return required_properties.distribution.is_replicated
            && required_properties.distribution.node_count > 1
            && required_properties.distribution.columns.empty()  /// replicated requirements never carry columns
            && expression->strategy == nullptr                   /// logical expressions only
            && isReplicationSafe(*expression->getQueryPlanStep());
    }

    Promise getPromise() const override { return 5000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override
    {
        DistributionDescription replicated_distribution;
        replicated_distribution.node_count = required_properties.distribution.node_count;
        replicated_distribution.is_replicated = true;

        auto replicated_expression = std::make_shared<GroupExpression>(*expression);
        replicated_expression->strategy = std::make_shared<ReplicatedSubplanStrategy>();
        for (auto & input : replicated_expression->inputs)
            input.required_properties.distribution = replicated_distribution;
        replicated_expression->properties.distribution = replicated_distribution;

        std::vector<GroupExpressionPtr> result;
        addPhysicalToMemo(replicated_expression, required_properties, memo, result);
        return result;
    }
};

OptimizationRulePtr createReplicatedSubplanImplementation();
OptimizationRulePtr createReplicatedSubplanImplementation() { return std::make_shared<ReplicatedSubplanImplementation>(); }

}
