#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
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
static void translateDistributionColumns(const ActionsDAG & dag, std::vector<NameSet> & columns)
{
    for (auto & column_set : columns)
    {
        NameSet translated;
        for (const auto & name : column_set)
        {
            String input_name = traceColumnToInput(dag, name);
            if (!input_name.empty())
                translated.insert(input_name);
            else
                translated.insert(name);
        }
        column_set = std::move(translated);
    }
}

/// Moves the QueryPlan node to implementation as is
class DefaultImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "DefaultImplementation"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 1; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool DefaultImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    /// `AggregatingStep` is handled exclusively by `AggregationImplementation`.
    /// `JoinStepLogical` is handled exclusively by `HashJoinImplementation`.
    auto * step = expression->getQueryPlanStep();
    return typeid_cast<AggregatingStep *>(step) == nullptr
        && typeid_cast<JoinStepLogical *>(step) == nullptr;
}

/// Create a single implementation expression at the given distribution.
/// Returns the expression, or nullptr if the step has construction-time sorting
/// that conflicts with multi-node distribution.
static GroupExpressionPtr createImplementationAtDistribution(
    const GroupExpressionPtr & expression,
    const ExpressionProperties & required_properties,
    const DistributionDescription & distribution,
    const IOptimizationRule & rule)
{
    auto implementation_expression = std::make_shared<GroupExpression>(*expression);
    implementation_expression->plan_step->setStepDescription(*expression->plan_step);
    implementation_expression->setApplied(rule, required_properties);

    bool propagate_distribution = true;

    if (implementation_expression->inputs.size() == 1)
    {
        auto & input_props = implementation_expression->inputs[0].required_properties;
        const bool has_construction_time_sorting = !input_props.sorting.empty();

        if (!has_construction_time_sorting)
        {
            input_props.distribution = distribution;

            if (!input_props.distribution.columns.empty())
            {
                const ActionsDAG * dag = nullptr;
                if (auto * expression_step = typeid_cast<ExpressionStep *>(implementation_expression->plan_step.get()))
                    dag = &expression_step->getExpression();
                else if (auto * filter_step = typeid_cast<FilterStep *>(implementation_expression->plan_step.get()))
                    dag = &filter_step->getExpression();

                if (dag)
                    translateDistributionColumns(*dag, input_props.distribution.columns);
            }
        }
        else
        {
            /// Construction-time sorting conflicts with multi-node distribution.
            if (distribution.node_count > 1)
                return nullptr;
            propagate_distribution = false;
        }
    }
    else if (implementation_expression->inputs.empty())
    {
        propagate_distribution = false;
    }

    if (propagate_distribution)
        implementation_expression->properties.distribution = distribution;

    return implementation_expression;
}

std::vector<GroupExpressionPtr> DefaultImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    std::vector<GroupExpressionPtr> result;

    /// Primary implementation at the parent's required distribution.
    auto primary = createImplementationAtDistribution(expression, required_properties, required_properties.distribution, *this);
    if (primary)
    {
        memo.getGroup(expression->group_id)->addPhysicalExpression(primary);
        result.push_back(primary);
    }

    /// For single-input steps, also create implementations at each candidate node count.
    /// This gives `DistributionEnforcer` multi-node expressions to work with, enabling
    /// plans like: Agg(1 node) -> GatherExchange -> Expression(N nodes) -> Join(N nodes).
    /// Without these, groups above joins would only have {1 node} implementations,
    /// preventing parallel reads in the subtree.
    if (expression->inputs.size() == 1)
    {
        const auto candidate_node_counts = getCandidateNodeCounts(memo.getClusterNodeCount());
        for (size_t candidate : candidate_node_counts)
        {
            if (candidate == required_properties.distribution.node_count)
                continue;

            DistributionDescription dist;
            dist.node_count = candidate;

            auto implementation_expression = createImplementationAtDistribution(expression, required_properties, dist, *this);
            if (implementation_expression)
            {
                memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
                result.push_back(implementation_expression);
            }
        }
    }

    return result;
}

OptimizationRulePtr createDefaultImplementation() { return std::make_shared<DefaultImplementation>(); }

}
