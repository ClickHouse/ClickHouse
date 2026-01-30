#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <memory>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{

class OptimizerContext;

class IOptimizationRule;
using OptimizationRulePtr = std::shared_ptr<const IOptimizationRule>;

using Promise = Int64;
using CostLimit = Cost;

class IOptimizationTask : boost::noncopyable
{
protected:
    explicit IOptimizationTask(CostLimit cost_limit_)
        : cost_limit(cost_limit_)
    {}

public:
    virtual ~IOptimizationTask() = default;
    virtual void execute(OptimizerContext & optimizer_context) = 0;

    const CostLimit cost_limit;
};

using OptimizationTaskPtr = std::shared_ptr<IOptimizationTask>;

/// Optimizations tasks as they are described in "Extensible Query Optimizers in Practice":
/// https://www.microsoft.com/en-us/research/wp-content/uploads/2024/12/Extensible-Query-Optimizers-in-Practice.pdf#section.2.3

class OptimizeGroupTask final : public IOptimizationTask
{
public:
    OptimizeGroupTask(GroupId group_id_, ExpressionProperties required_properties_, CostLimit cost_limit_)
        : IOptimizationTask(cost_limit_)
        , group_id(group_id_)
        , required_properties(required_properties_)
    {}

    void execute(OptimizerContext & optimizer_context) override;

private:
    GroupId group_id;
    ExpressionProperties required_properties;
};


class ExploreGroupTask final : public IOptimizationTask
{
public:
    ExploreGroupTask(GroupId group_id_, CostLimit cost_limit_)
        : IOptimizationTask(cost_limit_)
        , group_id(group_id_)
    {}

    void execute(OptimizerContext & optimizer_context) override;

private:
    GroupId group_id;
};


class ExploreExpressionTask final : public IOptimizationTask
{
public:
    ExploreExpressionTask(GroupExpressionPtr expression_, CostLimit cost_limit_)
        : IOptimizationTask(cost_limit_)
        , expression(expression_)
    {}

    void execute(OptimizerContext & optimizer_context) override;

private:
    GroupExpressionPtr expression;
};


class OptimizeExpressionTask final : public IOptimizationTask
{
public:
    OptimizeExpressionTask(GroupExpressionPtr expression_, ExpressionProperties required_properties_, CostLimit cost_limit_)
        : IOptimizationTask(cost_limit_)
        , expression(expression_)
        , required_properties(required_properties_)
    {}

    void execute(OptimizerContext & optimizer_context) override;

private:
    GroupExpressionPtr expression;
    ExpressionProperties required_properties;
};


class ApplyRuleTask final : public IOptimizationTask
{
public:
    ApplyRuleTask(GroupExpressionPtr expression_, ExpressionProperties required_properties_, OptimizationRulePtr rule_, Promise promise_, CostLimit cost_limit_)
        : IOptimizationTask(cost_limit_)
        , expression(expression_)
        , required_properties(required_properties_)
        , rule(rule_)
        , promise(promise_)
    {}

    void execute(OptimizerContext & optimizer_context) override;

private:
    void updateMemo(const std::vector<GroupExpressionPtr> & new_expressions, OptimizerContext & optimizer_context);

    GroupExpressionPtr expression;
    ExpressionProperties required_properties;
    OptimizationRulePtr rule;
    Promise promise;
};


class OptimizeInputsTask final : public IOptimizationTask
{
public:
    OptimizeInputsTask(GroupExpressionPtr expression_, size_t input_index_to_optimize_, CostLimit cost_limit_)
        : IOptimizationTask(cost_limit_)
        , expression(expression_)
        , input_index_to_optimize(input_index_to_optimize_)
    {}

    void execute(OptimizerContext & optimizer_context) override;

private:
    GroupExpressionPtr expression;
    const size_t input_index_to_optimize;
};


}
