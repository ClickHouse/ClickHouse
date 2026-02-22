#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
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

bool DefaultImplementation::checkPattern(GroupExpressionPtr /*expression*/, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    return true;
}

std::vector<GroupExpressionPtr> DefaultImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto implementation_expression = std::make_shared<GroupExpression>(*expression);
    implementation_expression->plan_step->setStepDescription(fmt::format("IMPL: {}", expression->plan_step->getStepDescription()), 200);
    implementation_expression->setApplied(*this, required_properties);

    if (implementation_expression->inputs.size() == 1)
    {
        auto & input_props = implementation_expression->inputs[0].required_properties;
        /// Check before modifying whether the input already has a sorting requirement
        /// from construction time (i.e. a SortingStep was stripped between this step and
        /// its child). Sorting requires a single stream on one node, so this distribution
        /// must remain single-node; the parent's distribution requirement applies to this
        /// step's output and will be enforced by DistributionEnforcer.
        const bool has_construction_time_sorting = !input_props.sorting.empty();
        /// Keep construction-time sorting if set, otherwise propagate from parent.
        if (input_props.sorting.empty())
            input_props.sorting = required_properties.sorting;
        /// Keep construction-time sort_limit if set, otherwise propagate from parent.
        if (input_props.sort_limit == 0)
            input_props.sort_limit = required_properties.sort_limit;
        /// Propagate the parent's distribution to the input only when there is no
        /// construction-time sorting requirement. When there is one, forcing the input
        /// into a multi-node distribution alongside a sort requirement would produce
        /// an unsatisfiable combined requirement (no enforcer can sort and scatter
        /// simultaneously). Leave the input single-node; DistributionEnforcer will
        /// add the necessary exchange on top of this step's output instead.
        if (!has_construction_time_sorting)
            input_props.distribution = required_properties.distribution;
    }

    memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
    return {implementation_expression};
}

OptimizationRulePtr createDefaultImplementation() { return std::make_shared<DefaultImplementation>(); }

}
