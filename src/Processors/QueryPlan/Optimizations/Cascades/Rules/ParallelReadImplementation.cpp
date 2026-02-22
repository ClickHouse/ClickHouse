#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ParallelReadImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "ParallelRead"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 5000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool ParallelReadImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return typeid_cast<ReadFromMergeTree *>(expression->getQueryPlanStep()) != nullptr &&
        required_properties.distribution.node_count > 1 &&
        !required_properties.distribution.is_replicated;
}

std::vector<GroupExpressionPtr> ParallelReadImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto * read_step = typeid_cast<ReadFromMergeTree *>(expression->getQueryPlanStep());
    const size_t node_count = required_properties.distribution.node_count;

    /// Produce a distributed read that splits work uniformly across all nodes.
    /// DefaultImplementation handles the single-node (local) read.
    auto parallel_read_step_ptr = read_step->clone();
    auto * parallel_read_step = typeid_cast<ReadFromMergeTree *>(parallel_read_step_ptr.get());
    if (!parallel_read_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ParallelReadImplementation: clone() of ReadFromMergeTree returned unexpected step type for expression '{}'",
            expression->getDescription());

    parallel_read_step->setDistributedRead(node_count);
    parallel_read_step->setStepDescription(fmt::format("ParallelRead IMPL: {}", read_step->getStepDescription()), 200);

    GroupExpressionPtr parallel_read_expression = std::make_shared<GroupExpression>(*expression);
    parallel_read_expression->plan_step = std::move(parallel_read_step_ptr);

    ExpressionProperties parallel_properties;
    parallel_properties.distribution.node_count = node_count;
    parallel_read_expression->properties = parallel_properties;

    parallel_read_expression->setApplied(*this, required_properties);
    memo.getGroup(expression->group_id)->addPhysicalExpression(parallel_read_expression);

    return {parallel_read_expression};
}

OptimizationRulePtr createParallelReadImplementation() { return std::make_shared<ParallelReadImplementation>(); }

}
