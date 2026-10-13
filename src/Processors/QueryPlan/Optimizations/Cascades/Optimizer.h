#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StatisticsDerivation.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/Logger.h>
#include <stack>
#include <utility>

namespace DB
{

class JoinGraphStep;

struct QueryPlanOptimizationSettings;

class CascadesOptimizer
{
public:
    CascadesOptimizer(QueryPlan & query_plan_, const QueryPlanOptimizationSettings & optimization_settings_);

    /// Runs the search and replaces the plan with the best alternative. Single use: the memo,
    /// the task stack and the best-plan state belong to this run, so a second call throws.
    void optimize();

    std::pair<GroupId, ExpressionProperties> addGroup(QueryPlan::Node & node);
    void pushTask(OptimizationTaskPtr task);
    GroupPtr getGroup(GroupId group_id);

    void updateBestPlan(GroupExpressionPtr expression);
    void deriveStatistics(GroupId group_id);

    /// Costs the expression (local operator cost plus inputs' best subtree costs), stores the cost
    /// on it and offers it to the group as a best-implementation candidate. Statistics must be
    /// derived first. With `prune_against_best` the expression is dropped early (cost not stored)
    /// when the group already holds a cheaper best for the same properties; returns false then.
    bool costAndUpdateBest(GroupExpressionPtr expression, bool prune_against_best);

    /// Fast-path costing: if all inputs already have best implementations,
    /// compute the expression's cost directly and update the group's best plan.
    /// Returns true if the expression was handled (all inputs ready), false otherwise.
    bool tryUpdateBestPlanDirectly(GroupExpressionPtr expression);

    /// Cost the expression now when every input already has a best implementation (avoids the
    /// whole `OptimizeInputsTask` chain), otherwise schedule input optimization.
    void scheduleCosting(GroupExpressionPtr expression);

    LoggerPtr log = getLogger("CascadesOptimizer");

    const std::vector<OptimizationRulePtr> & getTransformationRules() const { return transformation_rules; }
    const std::vector<OptimizationRulePtr> & getImplementationRules() const { return implementation_rules; }
    const std::vector<OptimizationRulePtr> & getEnforcerRules() const { return enforcer_rules; }

    Memo & getMemo() { return memo; }
    const Memo & getMemo() const { return memo; }

private:
    /// Imports a `JoinGraphStep`: each input once, and the joins of every join order over the input groups.
    std::pair<GroupId, ExpressionProperties> addJoinGraph(QueryPlan::Node & node, const JoinGraphStep & graph_step);

    void addRule(OptimizationRulePtr rule);
    void addEnforcerRule(OptimizationRulePtr rule);

    QueryPlanPtr buildBestPlan(GroupId subtree_root_group_id, ExpressionProperties required_properties);

    QueryPlan & query_plan;
    /// Owns the plans of the join orders that `addJoinGraph` builds, whose steps the memo takes.
    QueryPlan::Nodes join_graph_nodes;
    const QueryPlanOptimizationSettings & optimization_settings;
    bool optimize_was_called = false;

    std::vector<OptimizationRulePtr> transformation_rules;
    std::vector<OptimizationRulePtr> implementation_rules;
    std::vector<OptimizationRulePtr> enforcer_rules;

    /// The table statistics that the derivation reads. Owned here because
    /// `statistics_derivation` keeps a reference to it.
    OptimizerStatisticsPtr statistics;
    Memo memo{log};
    std::stack<OptimizationTaskPtr> tasks;
    CostEstimator cost_estimator;
    StatisticsDerivation statistics_derivation;
};

}
