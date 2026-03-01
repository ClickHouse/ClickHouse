#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Fallback implementation rule for steps not handled by specialized rules.
/// Creates a single implementation at {1 node} without distribution propagation.
/// Distributed execution of these steps is handled by dedicated rules
/// (`AggregationImplementation`, `SortingEnforcer`, etc.).
///
/// Stateless per-row steps (`ExpressionStep`, `FilterStep`, `BuildRuntimeFilterStep`)
/// are excluded and handled by `DistributionPassthrough` instead.
class DefaultImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "DefaultImplementation"; }

    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const override
    {
        auto * step = expression->getQueryPlanStep();
        /// Steps with specialized implementation rules.
        if (typeid_cast<AggregatingStep *>(step) != nullptr
            || typeid_cast<JoinStepLogical *>(step) != nullptr)
            return false;
        /// Distribution-passthrough steps handled by `DistributionPassthrough`.
        if (typeid_cast<ExpressionStep *>(step) != nullptr
            || typeid_cast<FilterStep *>(step) != nullptr
            || typeid_cast<BuildRuntimeFilterStep *>(step) != nullptr)
            return false;
        return true;
    }

    Promise getPromise() const override { return 1; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override
    {
        auto implementation_expression = std::make_shared<GroupExpression>(*expression);
        implementation_expression->plan_step->setStepDescription(*expression->plan_step);
        implementation_expression->setApplied(*this, required_properties);
        /// No distribution propagation: output stays at default {1 node}.

        memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
        return {implementation_expression};
    }
};


OptimizationRulePtr createDefaultImplementation() { return std::make_shared<DefaultImplementation>(); }

}
