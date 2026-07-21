#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/DagNameTranslation.h>
#include <Common/logger_useful.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Core/SortDescription.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Get the `ActionsDAG` from an `ExpressionStep` or `FilterStep`, or nullptr.
static const ActionsDAG * tryGetActionsDAG(const IQueryPlanStep * step)
{
    if (const auto * expr_step = typeid_cast<const ExpressionStep *>(step))
        return &expr_step->getExpression();
    if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
        return &filter_step->getExpression();
    return nullptr;
}

/// Implementation rule for stateless per-row steps. Propagates distribution to the
/// input and creates speculative multi-node variants at each candidate node count.
/// Also creates sorted passthrough variants that delegate sorting to the child group,
/// so the sort can be placed below the passthrough step when that is cheaper.
class DistributionPassthrough : public IOptimizationRule
{
public:
    String getName() const override { return "DistributionPassthrough"; }

    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const override
    {
        return isDistributionPassthroughStep(*expression->getQueryPlanStep());
    }

    Promise getPromise() const override { return 1; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override
    {
        std::vector<GroupExpressionPtr> result;

        /// Per-block and non-deterministic functions (`rowNumberInAllBlocks`, `blockNumber`,
        /// `nowInBlock`, `rand`, ...) produce different values when the stream is split across
        /// nodes or recomputed per replica, so such a step runs on a single node only.
        /// The rule-based planner applies the same rule when moving gathers across steps.
        if (const ActionsDAG * step_dag = tryGetActionsDAG(expression->getQueryPlanStep());
            step_dag && QueryPlanOptimizations::dagContainsNonDeterministicFunction(*step_dag))
        {
            DistributionDescription single_node;    /// node_count=1, not replicated (default)
            if (auto implementation_expression = createAtDistribution(expression, single_node))
                addPhysicalToMemo(implementation_expression, required_properties, memo, result);
            return result;
        }

        /// Implementation at the parent's required distribution.
        if (auto implementation_expression = createAtDistribution(expression, required_properties.distribution))
            addPhysicalToMemo(implementation_expression, required_properties, memo, result);

        /// Speculative implementations at each candidate node count.
        auto candidates = getCandidateNodeCounts(memo.getEnvironment().cluster_node_count);
        for (size_t candidate : candidates)
        {
            if (candidate == required_properties.distribution.node_count)
                continue;

            DistributionDescription dist;
            dist.node_count = candidate;

            if (auto implementation_expression = createAtDistribution(expression, dist))
                addPhysicalToMemo(implementation_expression, required_properties, memo, result);
        }

        /// Sorted passthrough: delegate sorting to the child (column names translated
        /// through the DAG).  Competes with unsorted variant + `SortingEnforcer`.
        if (!required_properties.sorting.empty())
        {
            const ActionsDAG * dag = tryGetActionsDAG(expression->getQueryPlanStep());
            SortDescription input_sorting = required_properties.sorting;
            bool can_translate = !dag || translateSortDescription(*dag, input_sorting);

            if (can_translate)
            {
                auto create_sorted_variant = [&](const DistributionDescription & dist)
                {
                    auto sorted_impl = std::make_shared<GroupExpression>(*expression);

                    chassert(sorted_impl->inputs.size() == 1);
                    auto & sorted_input_props = sorted_impl->inputs[0].required_properties;

                    sorted_input_props.distribution = dist;
                    sorted_input_props.sorting = input_sorting;

                    sorted_impl->properties.distribution = dist;
                    sorted_impl->properties.sorting = required_properties.sorting;

                    addPhysicalToMemo(sorted_impl, required_properties, memo, result);
                };

                for (size_t candidate : candidates)
                {
                    DistributionDescription dist;
                    dist.node_count = candidate;
                    create_sorted_variant(dist);
                }

                create_sorted_variant({});
            }
        }

        return result;
    }

private:
    GroupExpressionPtr createAtDistribution(
        const GroupExpressionPtr & expression,
        const DistributionDescription & distribution) const
    {
        auto implementation_expression = std::make_shared<GroupExpression>(*expression);

        chassert(implementation_expression->inputs.size() == 1);
        auto & input_props = implementation_expression->inputs[0].required_properties;

        /// Construction-time sorting conflicts with multi-node distribution.
        if (!input_props.sorting.empty() && distribution.node_count > 1)
            return nullptr;

        if (input_props.sorting.empty())
        {
            input_props.distribution = distribution;

            /// Translate distribution column names through the step's DAG.
            if (!input_props.distribution.columns.empty())
            {
                const ActionsDAG * dag = tryGetActionsDAG(implementation_expression->plan_step.get());
                if (dag && !translateDistributionColumns(*dag, input_props.distribution.columns))
                {
                    LOG_TEST(getLogger("DistributionPassthrough"), "No passthrough for '{}': a distribution column does not map through the step",
                        implementation_expression->getName());
                    return nullptr;
                }
            }

            implementation_expression->properties.distribution = distribution;
        }

        return implementation_expression;
    }
};


OptimizationRulePtr createDistributionPassthrough();
OptimizationRulePtr createDistributionPassthrough() { return std::make_shared<DistributionPassthrough>(); }

}
