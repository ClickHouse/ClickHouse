#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
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
    explicit GroupExpression(QueryPlanStepPtr plan_step_)
        : plan_step(std::move(plan_step_))
    {}

    GroupExpression(const GroupExpression & other_)
        : group_id(other_.group_id)
        , plan_step(other_.getQueryPlanStep()->clone())
//        , original_node(nullptr)
        , inputs(other_.inputs)
    {}

    String getName() const;
    String getDescription() const;
    IQueryPlanStep * getQueryPlanStep() const;
    bool isApplied(const IOptimizationRule & rule, const ExpressionProperties & required_properties) const;
    void setApplied(const IOptimizationRule & rule, const ExpressionProperties & required_properties);

    void dump(WriteBuffer & out) const;
    String dump() const;

    GroupId group_id = INVALID_GROUP_ID;
    QueryPlanStepPtr plan_step;             /// Main implementation step
//    QueryPlan::Node * original_node;    /// Node form the original query plan if the expression was directly created from it

    struct Input
    {
        GroupId group_id;
        ExpressionProperties required_properties; /// TODO: optional?
    };

    std::vector<Input> inputs;

    ExpressionProperties properties;
    std::vector<QueryPlanStepPtr> property_enforcer_steps;   /// Steps that enforce required properties but don't change the logical set of rows

    std::unordered_set<String> applied_rules;   /// TODO: implement more efficiently

    std::optional<ExpressionStatistics> statistics;
    std::optional<ExpressionCost> cost;
};

using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

}
