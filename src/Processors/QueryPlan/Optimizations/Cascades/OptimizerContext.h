#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Optimizer.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StatisticsDerivation.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/Logger.h>
#include <stack>
#include <utility>


namespace DB
{

class OptimizerContext
{
    friend class CascadesOptimizer;

public:
    explicit OptimizerContext(IOptimizerStatistics & statistics, size_t cluster_node_count = 1, CostConfig cost_config = {});

    std::pair<GroupId, ExpressionProperties> addGroup(QueryPlan::Node & node);
    void pushTask(OptimizationTaskPtr task);
    GroupPtr getGroup(GroupId group_id);

    void updateBestPlan(GroupExpressionPtr expression);
    void deriveStatistics(GroupId group_id);

    CostEstimator & getCostEstimator() { return cost_estimator; }

    /// Compute the cost budget for optimizing a child input group.
    /// budget = parent_limit - sum(other children's best subtree costs)
    CostLimit computeChildCostLimit(GroupExpressionPtr parent_expression, size_t child_index, CostLimit parent_limit);

    /// Fast-path costing: if all inputs already have best implementations,
    /// compute the expression's cost directly and update the group's best plan.
    /// Returns true if the expression was handled (all inputs ready), false otherwise.
    bool tryUpdateBestPlanDirectly(GroupExpressionPtr expression);

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
    StatisticsDerivation statistics_derivation;
};

}
