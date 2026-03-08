#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Trace a column name back through the DAG to its original input name.
/// For ALIAS chains (e.g., __table3.l_orderkey -> l_orderkey), returns the input name.
/// For computed columns (FUNCTION nodes), returns empty - can't be translated.
static String traceColumnToInput(const ActionsDAG & dag, const String & output_name)
{
    const ActionsDAG::Node * node = dag.tryFindInOutputs(output_name);
    if (!node)
        return {};
    while (node->type == ActionsDAG::ActionType::ALIAS)
    {
        chassert(node->children.size() == 1);
        node = node->children.front();
    }
    if (node->type == ActionsDAG::ActionType::INPUT)
        return node->result_name;
    return {};
}

/// Translate distribution column names through an ActionsDAG.
/// Returns false if any column set becomes empty (all names are computed by this step),
/// signaling that the passthrough implementation should be rejected so that
/// DistributionEnforcer fires on this step's output where the column exists.
static bool translateDistributionColumns(const ActionsDAG & dag, std::vector<NameSet> & columns)
{
    for (auto & column_set : columns)
    {
        NameSet translated;
        for (const auto & name : column_set)
        {
            String input_name = traceColumnToInput(dag, name);
            if (!input_name.empty())
            {
                translated.insert(input_name);
            }
            else if (!dag.tryFindInOutputs(name))
            {
                /// Not in DAG outputs — may be a passthrough input column. Keep it.
                translated.insert(name);
            }
            /// else: computed by this step (FUNCTION) — not present in input. Drop it.
        }
        if (translated.empty())
            return false;
        column_set = std::move(translated);
    }
    return true;
}

/// Stateless per-row steps that can safely run on any subset of the data independently.
/// This whitelist is safe by default: a step type not listed here falls through to
/// `DefaultImplementation` at {1 node}. The failure mode is a missed optimization,
/// never incorrect results.
static bool isDistributionPassthrough(const IQueryPlanStep * step)
{
    return typeid_cast<const ExpressionStep *>(step) != nullptr
        || typeid_cast<const FilterStep *>(step) != nullptr
        || typeid_cast<const BuildRuntimeFilterStep *>(step) != nullptr;
}


/// Implementation rule for stateless per-row steps (`ExpressionStep`, `FilterStep`,
/// `BuildRuntimeFilterStep`). These steps can run on any data partition independently,
/// so it is safe to propagate the parent's distribution to the input and to create
/// speculative multi-node variants at each candidate node count.
///
/// The multi-node variants give `DistributionEnforcer` something to attach GatherExchanges
/// to, enabling plans like:
///   Agg(1 node) -> GatherExchange -> Expression(N nodes) -> Join(N nodes)
class DistributionPassthrough : public IOptimizationRule
{
public:
    String getName() const override { return "DistributionPassthrough"; }

    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const override
    {
        return isDistributionPassthrough(expression->getQueryPlanStep());
    }

    Promise getPromise() const override { return 1; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override
    {
        std::vector<GroupExpressionPtr> result;

        /// Implementation at the parent's required distribution.
        if (auto implementation_expression = createAtDistribution(expression, required_properties, required_properties.distribution))
        {
            memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
            result.push_back(implementation_expression);
        }

        /// Speculative implementations at each candidate node count.
        for (size_t candidate : getCandidateNodeCounts(memo.getClusterNodeCount()))
        {
            if (candidate == required_properties.distribution.node_count)
                continue;

            DistributionDescription dist;
            dist.node_count = candidate;

            if (auto implementation_expression = createAtDistribution(expression, required_properties, dist))
            {
                memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
                result.push_back(implementation_expression);
            }
        }

        return result;
    }

private:
    GroupExpressionPtr createAtDistribution(
        const GroupExpressionPtr & expression,
        const ExpressionProperties & required_properties,
        const DistributionDescription & distribution) const
    {
        auto implementation_expression = std::make_shared<GroupExpression>(*expression);
        implementation_expression->plan_step->setStepDescription(*expression->plan_step);
        implementation_expression->setApplied(*this, required_properties);

        chassert(implementation_expression->inputs.size() == 1);
        auto & input_props = implementation_expression->inputs[0].required_properties;

        /// Construction-time sorting on the input conflicts with multi-node distribution.
        /// Both input and output stay at {1 node}; `DistributionEnforcer` adds
        /// an exchange on this step's output if the parent needs multi-node.
        if (!input_props.sorting.empty() && distribution.node_count > 1)
            return nullptr;

        if (input_props.sorting.empty())
        {
            input_props.distribution = distribution;

            /// Translate distribution column names through the step's DAG.
            if (!input_props.distribution.columns.empty())
            {
                const ActionsDAG * dag = nullptr;
                if (auto * expr_step = typeid_cast<ExpressionStep *>(implementation_expression->plan_step.get()))
                    dag = &expr_step->getExpression();
                else if (auto * filter_step = typeid_cast<FilterStep *>(implementation_expression->plan_step.get()))
                    dag = &filter_step->getExpression();

                if (dag && !translateDistributionColumns(*dag, input_props.distribution.columns))
                    return nullptr;
            }

            implementation_expression->properties.distribution = distribution;
        }

        return implementation_expression;
    }
};


OptimizationRulePtr createDistributionPassthrough() { return std::make_shared<DistributionPassthrough>(); }

}
