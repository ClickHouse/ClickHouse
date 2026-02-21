#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

class ParallelReadImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "ParallelRead"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 5000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const override;
};

bool ParallelReadImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return typeid_cast<ReadFromMergeTree *>(expression->getQueryPlanStep()) != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:") &&
        required_properties.distribution.node_count > 1 &&
        !required_properties.distribution.is_replicated;
}

std::vector<GroupExpressionPtr> ParallelReadImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto * read_step = typeid_cast<ReadFromMergeTree *>(expression->getQueryPlanStep());

    auto new_read_step = read_step->clone();
    new_read_step->setStepDescription(fmt::format("ParallelRead IMPL: {}", read_step->getStepDescription()), 200);

    GroupExpressionPtr read_expression = std::make_shared<GroupExpression>(*expression);
    read_expression->plan_step = std::move(new_read_step);
    read_expression->properties = required_properties;

    memo.getGroup(expression->group_id)->addPhysicalExpression(read_expression);
    read_expression->setApplied(*this, required_properties);
    return {read_expression};
}

OptimizationRulePtr createParallelReadImplementation() { return std::make_shared<ParallelReadImplementation>(); }

}
