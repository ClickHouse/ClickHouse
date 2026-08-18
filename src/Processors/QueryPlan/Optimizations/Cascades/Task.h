#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <memory>
#include <fmt/format.h>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{

class OptimizerContext;

class IOptimizationRule;
using OptimizationRulePtr = std::shared_ptr<const IOptimizationRule>;

/// Rule priority: among the rules applicable to one expression, a higher promise fires first.
/// Candidates are sorted ascending and pushed on the LIFO task stack, so the largest promise
/// ends up on top.
using Promise = Int64;
class IOptimizationTask : boost::noncopyable
{
public:
    virtual ~IOptimizationTask() = default;
    virtual void execute(OptimizerContext & optimizer_context) = 0;
    /// One line for logs and the task-budget error: which task and on what.
    virtual String describe() const = 0;
};

using OptimizationTaskPtr = std::shared_ptr<IOptimizationTask>;

/// Optimization tasks as described in "Extensible Query Optimizers in Practice":
/// https://www.microsoft.com/en-us/research/wp-content/uploads/2024/12/Extensible-Query-Optimizers-in-Practice.pdf#section.2.3

class OptimizeGroupTask final : public IOptimizationTask
{
public:
    OptimizeGroupTask(GroupId group_id_, ExpressionProperties required_properties_)
        : group_id(group_id_)
        , required_properties(required_properties_)
    {}

    void execute(OptimizerContext & optimizer_context) override;
    String describe() const override { return fmt::format("optimize group #{} for {}", group_id, required_properties.dump()); }

private:
    /// Stage 3: run the enforcer rules to a fixed point over the group's physical expressions
    /// and return the expressions they inserted; satisfying expressions update the best plan.
    std::vector<GroupExpressionPtr> runEnforcementStage(OptimizerContext & optimizer_context, const GroupPtr & group) const;

    GroupId group_id;
    ExpressionProperties required_properties;
};


class ExploreGroupTask final : public IOptimizationTask
{
public:
    explicit ExploreGroupTask(GroupId group_id_)
        : group_id(group_id_)
    {}

    void execute(OptimizerContext & optimizer_context) override;
    String describe() const override { return fmt::format("explore group #{}", group_id); }

private:
    GroupId group_id;
};


class ExploreExpressionTask final : public IOptimizationTask
{
public:
    explicit ExploreExpressionTask(GroupExpressionPtr expression_)
        : expression(expression_)
    {}

    void execute(OptimizerContext & optimizer_context) override;
    String describe() const override;

private:
    GroupExpressionPtr expression;
};


class OptimizeExpressionTask final : public IOptimizationTask
{
public:
    OptimizeExpressionTask(GroupExpressionPtr expression_, ExpressionProperties required_properties_)
        : expression(expression_)
        , required_properties(required_properties_)
    {}

    void execute(OptimizerContext & optimizer_context) override;
    String describe() const override;

private:
    GroupExpressionPtr expression;
    ExpressionProperties required_properties;
};


class ApplyRuleTask final : public IOptimizationTask
{
public:
    ApplyRuleTask(GroupExpressionPtr expression_, ExpressionProperties required_properties_, OptimizationRulePtr rule_)
        : expression(expression_)
        , required_properties(required_properties_)
        , rule(rule_)
    {}

    void execute(OptimizerContext & optimizer_context) override;
    String describe() const override;

private:
    void updateMemo(const std::vector<GroupExpressionPtr> & new_expressions, OptimizerContext & optimizer_context);

    GroupExpressionPtr expression;
    ExpressionProperties required_properties;
    OptimizationRulePtr rule;
};


class OptimizeInputsTask final : public IOptimizationTask
{
public:
    OptimizeInputsTask(GroupExpressionPtr expression_, size_t input_index_to_optimize_)
        : expression(expression_)
        , input_index_to_optimize(input_index_to_optimize_)
    {}

    void execute(OptimizerContext & optimizer_context) override;
    String describe() const override;

private:
    GroupExpressionPtr expression;
    const size_t input_index_to_optimize;
};


}
