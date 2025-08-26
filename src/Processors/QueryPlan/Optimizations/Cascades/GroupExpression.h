#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
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
        , plan_step(other_.plan_step ? other_.plan_step->clone() : nullptr)
        , original_node(other_.original_node)
        , inputs(other_.inputs)
    {}

    String getName() const;
    String getDescription() const;
    IQueryPlanStep * getQueryPlanStep() const;
    bool isApplied(const IOptimizationRule & rule) const;
    void setApplied(const IOptimizationRule & rule);

    GroupId group_id = INVALID_GROUP_ID;
    QueryPlanStepPtr plan_step;
    QueryPlan::Node * original_node;
    std::vector<GroupId> inputs;

    std::unordered_set<String> applied_rules;   /// TODO: implement more efficiently
};

using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

}
