#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

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
    implementation_expression->plan_step->setStepDescription(fmt::format("IMPL: {}", expression->plan_step->getStepDescription()), 200);
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
            input_props.distribution = required_properties.distribution;
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
