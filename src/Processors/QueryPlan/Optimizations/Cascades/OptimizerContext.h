#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/Logger.h>
#include "Processors/QueryPlan/Optimizations/Cascades/Group.h"
#include "Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h"
#include "Processors/QueryPlan/Optimizations/Cascades/Optimizer.h"
#include <stack>


namespace DB
{

class OptimizerContext
{
    friend class CascadesOptimizer;

public:
    OptimizerContext();

    GroupId addGroup(QueryPlan::Node & node);
    void pushTask(OptimizationTaskPtr task);
    GroupPtr getGroup(GroupId group_id);
    void getBestPlan(GroupId group_id);

    void updateBestPlan(GroupExpressionPtr expression);

    LoggerPtr log = getLogger("CascadesOptimizer");

    const std::vector<OptimizationRulePtr> & getTransformationRules() const { return transformation_rules; }
    const std::vector<OptimizationRulePtr> & getImplementationRules() const { return implementation_rules; }

    Memo & getMemo() { return memo; }
    const Memo & getMemo() const { return memo; }

private:
    void addRule(OptimizationRulePtr rule);

    std::vector<OptimizationRulePtr> transformation_rules;
    std::vector<OptimizationRulePtr> implementation_rules;

    Memo memo{log};
    std::stack<OptimizationTaskPtr> tasks;
    CostEstimator cost_estimator;
};

}
