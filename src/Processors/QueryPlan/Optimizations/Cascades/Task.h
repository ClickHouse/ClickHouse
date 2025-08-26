#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <memory>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{

class OptimizerContext;

class IOptimizationRule;
using OptimizationRulePtr = std::shared_ptr<const IOptimizationRule>;

using Promise = Int64;
using CostLimit = Int64;

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


class OptimizeGroupTask final : public IOptimizationTask
{
public:
    OptimizeGroupTask(GroupId group_id_, CostLimit cost_limit_)
        : IOptimizationTask(cost_limit_)
        , group_id(group_id_)
    {}

    void execute(OptimizerContext & optimizer_context) override;

private:
    GroupId group_id;
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
    OptimizeExpressionTask(GroupExpressionPtr expression_, CostLimit cost_limit_)
        : IOptimizationTask(cost_limit_)
        , expression(expression_)
    {}

    void execute(OptimizerContext & optimizer_context) override;

private:
    GroupExpressionPtr expression;
};


class ApplyRuleTask final : public IOptimizationTask
{
public:
    ApplyRuleTask(GroupExpressionPtr expression_, OptimizationRulePtr rule_, Promise promise_, CostLimit cost_limit_)
        : IOptimizationTask(cost_limit_)
        , expression(expression_)
        , rule(rule_)
        , promise(promise_)
    {}

    void execute(OptimizerContext & optimizer_context) override;

private:
    void updateMemo(const std::vector<GroupExpressionPtr> & new_expressions, OptimizerContext & optimizer_context);

    GroupExpressionPtr expression;
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
