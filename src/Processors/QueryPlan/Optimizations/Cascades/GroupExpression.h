#pragma once

#include <memory>
#include <unordered_set>
#include "Processors/QueryPlan/Optimizations/Cascades/Group.h"
#include "Processors/QueryPlan/QueryPlan.h"
#include "base/types.h"

namespace DB
{

class IOptimizationRule;
using OptimizationRulePtr = std::shared_ptr<const IOptimizationRule>;

class GroupExpression
{
public:
    explicit GroupExpression(const QueryPlan::Node & node_)
        : node(node_)
    {}

    String getName() const;
    String getDescription() const;
    bool isApplied(const IOptimizationRule & rule) const;
    void setApplied(const IOptimizationRule & rule);

    GroupId group_id = INVALID_GROUP_ID;
    const QueryPlan::Node & node;
    std::vector<GroupId> inputs;

    std::unordered_set<String> applied_rules;   /// TODO: implement more efficiently
};

using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

}
