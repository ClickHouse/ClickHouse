#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
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
    /// Initial creation from the query plan (takes ownership via unique_ptr,
    /// then stored as shared_ptr<const> for sharing across GroupExpressions).
    explicit GroupExpression(QueryPlanStepPtr plan_step_)
        : plan_step(std::move(plan_step_))
    {}

    /// Shallow copy: shares the immutable plan_step, copies only metadata.
    /// Rules that need a different step assign a new one to `plan_step`.
    GroupExpression(const GroupExpression & other_)
        : group_id(other_.group_id)
        , plan_step(other_.plan_step)
        , strategy(other_.strategy)
        , description_suffix(other_.description_suffix)
        , inputs(other_.inputs)
    {}

    String getName() const;
    String getDescription() const;
    const IQueryPlanStep * getQueryPlanStep() const;
    bool isApplied(const IOptimizationRule & rule, const ExpressionProperties & required_properties) const;
    void setApplied(const IOptimizationRule & rule, const ExpressionProperties & required_properties);

    void dump(WriteBuffer & out, const CostConfig & cost_config) const;
    String dump(const CostConfig & cost_config) const;
    String fingerprint() const;

    GroupId group_id = INVALID_GROUP_ID;
    std::shared_ptr<const IQueryPlanStep> plan_step;  /// Shared immutable plan step
    ImplementationStrategyPtr strategy;     /// Implementation strategy (nullptr = logical / default)
    String description_suffix;             /// Extra description set by rules (e.g., "(by col)" for single-key shuffle)

    struct Input
    {
        GroupId group_id;
        ExpressionProperties required_properties; /// TODO: optional?
    };

    std::vector<Input> inputs;

    ExpressionProperties properties;

    std::unordered_set<String> applied_rules;   /// TODO: implement more efficiently

    std::optional<ExpressionCost> cost;
};

using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

}
