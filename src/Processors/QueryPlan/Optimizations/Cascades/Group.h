#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>
#include <memory>
#include <optional>
#include <set>
#include <unordered_set>
#include <vector>

namespace DB
{

using GroupId = size_t;
constexpr GroupId INVALID_GROUP_ID = -1;

class GroupExpression;
using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

struct ExpressionStatistics;

struct ExpressionWithCost
{
    GroupExpressionPtr expression;
    ExpressionCost cost;         /// The cost of whole tree starting from this expression
};

class Group
{
public:
    explicit Group(GroupId group_id_)
        : group_id(group_id_)
    {}

    void addLogicalExpression(GroupExpressionPtr group_expression);
    void addPhysicalExpression(GroupExpressionPtr group_expression);
    bool isExplored() const { return is_explored; }
    void setExplored() { is_explored = true; }
    bool isOptimizedFor(const ExpressionProperties & required_properties) const;
    void setOptimizedFor(const ExpressionProperties & required_properties);

    /// Tracks whether optimization for a given property set is fully complete
    /// (all stages: explore, implement, enforce — have finished).
    bool isFullyDoneFor(const ExpressionProperties & required_properties) const;
    void setFullyDoneFor(const ExpressionProperties & required_properties);
    void updateBestImplementation(GroupExpressionPtr expression, const CostConfig & cost_config);
    ExpressionWithCost getBestImplementation(const ExpressionProperties & required_properties, const CostConfig & cost_config) const;

    /// Returns the weighted subtree cost of the best implementation satisfying
    /// the given properties, or infinity if none exists.
    Float64 getBestCostForProperties(const ExpressionProperties & required_properties, const CostConfig & cost_config) const;

    void dump(WriteBuffer & out, const CostConfig & cost_config, String indent = {}) const;
    String dump(const CostConfig & cost_config) const;

    std::vector<GroupExpressionPtr> logical_expressions;
    std::vector<GroupExpressionPtr> physical_expressions;

    /// Best implementation for various required properties 
    std::set<GroupExpressionPtr> best_implementations;

    /// Statistics for this group - shared by all expressions in the group
    /// since they all represent the same logical result
    std::optional<ExpressionStatistics> statistics;

private:
    const GroupId group_id;
    bool is_explored = false;
    std::set<String> optimized_properties;  /// Tracks which required properties have had implementation rules applied
    std::set<String> fully_done_properties; /// Tracks which required properties are fully optimized (all stages complete)
    std::unordered_set<String> physical_fingerprints;  /// Deduplicates identical physical expressions
};

using GroupPtr = std::shared_ptr<Group>;
using GroupConstPtr = std::shared_ptr<const Group>;

}
