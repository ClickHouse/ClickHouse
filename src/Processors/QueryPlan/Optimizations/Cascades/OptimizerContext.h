#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Optimizer.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/Logger.h>
#include <stack>


namespace DB
{

class OptimizerContext
{
    friend class CascadesOptimizer;

public:
    explicit OptimizerContext(IOptimizerStatistics & statistics);

    GroupId addGroup(QueryPlan::Node & node);
    void pushTask(OptimizationTaskPtr task);
    GroupPtr getGroup(GroupId group_id);

    void updateBestPlan(GroupExpressionPtr expression);

    LoggerPtr log = getLogger("CascadesOptimizer");

    const std::vector<OptimizationRulePtr> & getTransformationRules() const { return transformation_rules; }
    const std::vector<OptimizationRulePtr> & getImplementationRules() const { return implementation_rules; }
    const std::vector<OptimizationRulePtr> & getEnforcerRules() const { return enforcer_rules; }

    Memo & getMemo() { return memo; }
    const Memo & getMemo() const { return memo; }

private:
    void addRule(OptimizationRulePtr rule);
    void addEnforcerRule(OptimizationRulePtr rule);

    std::vector<OptimizationRulePtr> transformation_rules;
    std::vector<OptimizationRulePtr> implementation_rules;
    std::vector<OptimizationRulePtr> enforcer_rules;

    Memo memo{log};
    std::stack<OptimizationTaskPtr> tasks;
    CostEstimator cost_estimator;
};

}
