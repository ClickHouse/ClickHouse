#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <base/types.h>
#include <memory>
#include <unordered_set>

namespace DB
{

class IOptimizationRule;
using OptimizationRulePtr = std::shared_ptr<const IOptimizationRule>;

class GroupExpression final
{
public:
    explicit GroupExpression(QueryPlan::Node * node_)
        : original_node(node_)
    {}

    GroupExpression(const GroupExpression & other_)
        : group_id(other_.group_id)
        , plan_step(other_.getQueryPlanStep()->clone())
        , original_node(nullptr)
        , inputs(other_.inputs)
    {}

    String getName() const;
    String getDescription() const;
    IQueryPlanStep * getQueryPlanStep() const;
    bool isApplied(const IOptimizationRule & rule) const;
    void setApplied(const IOptimizationRule & rule);

    void dump(WriteBuffer & out) const;
    String dump() const;

    GroupId group_id = INVALID_GROUP_ID;
    QueryPlanStepPtr plan_step;         /// Step for expression that was not in the original plan but was created by transformations
    QueryPlan::Node * original_node;    /// Node form the original query plan if the expression was directly created from it
    std::vector<GroupId> inputs;

    std::unordered_set<String> applied_rules;   /// TODO: implement more efficiently

    std::optional<ExpressionStatistics> statistics;
    std::optional<ExpressionCost> cost;
};

using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

}
