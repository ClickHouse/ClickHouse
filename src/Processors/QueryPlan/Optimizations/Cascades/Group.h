#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>
#include <memory>
#include <optional>
#include <set>
#include <unordered_map>
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
    bool isEnforcedFor(const ExpressionProperties & required_properties) const;
    void setEnforcedFor(const ExpressionProperties & required_properties);

    /// Tracks whether optimization for a given property set is fully complete
    /// (all stages: explore, implement, enforce — have finished).
    bool isFullyDoneFor(const ExpressionProperties & required_properties) const;
    void setFullyDoneFor(const ExpressionProperties & required_properties);
    void updateBestImplementation(GroupExpressionPtr expression, const CostConfig & cost_config);
    ExpressionWithCost getBestImplementation(const ExpressionProperties & required_properties, const CostConfig & cost_config) const;

    /// Find the cheapest implementation satisfying `required_properties`,
    /// excluding expressions in `excluded`. Used for cycle detection in `buildBestPlan`.
    ExpressionWithCost getBestImplementationExcluding(
        const ExpressionProperties & required_properties,
        const CostConfig & cost_config,
        const std::unordered_set<GroupExpression *> & excluded) const;

    /// Returns the weighted subtree cost of the best implementation satisfying
    /// the given properties, or infinity if none exists.
    Float64 getBestCostForProperties(const ExpressionProperties & required_properties, const CostConfig & cost_config) const;

    void dump(WriteBuffer & out, const CostConfig & cost_config, String indent = {}) const;
    String dump(const CostConfig & cost_config) const;

    std::vector<GroupExpressionPtr> logical_expressions;
    std::vector<GroupExpressionPtr> physical_expressions;

    /// Best implementation for various required properties, indexed by distribution
    /// shape (node_count, is_replicated) for O(1) bucket lookup.
    std::unordered_map<UInt64, std::vector<GroupExpressionPtr>> best_implementations;

    /// Statistics for this group - shared by all expressions in the group
    /// since they all represent the same logical result
    std::optional<ExpressionStatistics> statistics;

private:
    const GroupId group_id;
    bool is_explored = false;
    std::unordered_set<ExpressionProperties, ExpressionPropertiesHash> optimized_properties;  /// Tracks which required properties have had implementation rules applied
    std::unordered_set<ExpressionProperties, ExpressionPropertiesHash> enforced_properties;   /// Tracks which required properties have had enforcer rules applied
    std::unordered_set<ExpressionProperties, ExpressionPropertiesHash> fully_done_properties; /// Tracks which required properties are fully optimized (all stages complete)
    std::unordered_set<String> physical_fingerprints;  /// Deduplicates identical physical expressions

    /// Encode (node_count, is_replicated) into a single key for best_implementations lookup.
    static UInt64 distributionKey(const DistributionDescription & distribution)
    {
        return (static_cast<UInt64>(distribution.node_count) << 1) | static_cast<UInt64>(distribution.is_replicated);
    }
};

using GroupPtr = std::shared_ptr<Group>;
using GroupConstPtr = std::shared_ptr<const Group>;

}
