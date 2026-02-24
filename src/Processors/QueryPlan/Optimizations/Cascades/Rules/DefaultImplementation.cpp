#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
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

std::vector<GroupExpressionPtr> DefaultImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto implementation_expression = std::make_shared<GroupExpression>(*expression);
    implementation_expression->plan_step->setStepDescription(*expression->plan_step);
    implementation_expression->setApplied(*this, required_properties);

    bool propagate_distribution = true;

    if (implementation_expression->inputs.size() == 1)
    {
        auto & input_props = implementation_expression->inputs[0].required_properties;
        const bool has_construction_time_sorting = !input_props.sorting.empty();

        /// Don't propagate the parent's sorting to the child - `SortingEnforcer`
        /// handles it on this group's output. Propagating would cause double sorting.
        /// Construction-time sorting (already on `input_props`) is kept as-is.

        /// Don't propagate distribution when the input has construction-time sorting:
        /// the combined {sorting + multi-node} requirement is unsatisfiable.
        /// Both input and output stay at {1 node}; `DistributionEnforcer` adds
        /// an exchange on this step's output if the parent needs multi-node.
        if (!has_construction_time_sorting)
        {
            input_props.distribution = required_properties.distribution;

            /// Translate distribution column names through the Expression/Filter DAG.
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
            propagate_distribution = false;
    }
    else if (implementation_expression->inputs.empty())
    {
        /// Leaf steps (e.g., `ReadFromMergeTree`) produce what the storage provides,
        /// not what the parent requires. `DistributionEnforcer` bridges any gap.
        propagate_distribution = false;
    }

    /// Output distribution matches what was propagated to the input.
    /// Without this, the output stays at the default {1 node}, which allows
    /// a parent requiring {1 node} to match an implementation whose input
    /// is actually on N nodes - getting IO reduction without exchange cost.
    if (propagate_distribution)
        implementation_expression->properties.distribution = required_properties.distribution;

    memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
    return {implementation_expression};
}

OptimizationRulePtr createDefaultImplementation() { return std::make_shared<DefaultImplementation>(); }

}
