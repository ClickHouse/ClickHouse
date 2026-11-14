#pragma once

#include <limits>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

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
constexpr GroupId INVALID_GROUP_ID = std::numeric_limits<GroupId>::max();

class GroupExpression;
using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

struct ExpressionStatistics;

struct ExpressionWithCost
{
    GroupExpressionPtr expression;
    ExpressionCost cost;         /// Both the expression's own cost and its whole subtree cost
};

class Group
{
public:
    explicit Group(GroupId group_id_)
        : group_id(group_id_)
    {}

    /// Returns true if the expression was inserted, false if an equal expression already
    /// exists in the group (the duplicate is dropped).
    bool addLogicalExpression(GroupExpressionPtr group_expression);
    /// Returns the expression that the group keeps: the given one, or an equal expression that
    /// the group already has (the given one is then dropped).
    GroupExpressionPtr addPhysicalExpression(GroupExpressionPtr group_expression);
    bool isExplored() const { return is_explored; }
    void setExplored() { is_explored = true; }
    bool isOptimizedFor(const ExpressionProperties & required_properties) const;
    void setOptimizedFor(const ExpressionProperties & required_properties);
    bool isEnforcedFor(const ExpressionProperties & required_properties) const;
    void setEnforcedFor(const ExpressionProperties & required_properties);

    /// Tracks whether optimization for a given property set is fully complete
    /// (all stages: explore, implement, enforce - have finished).
    bool isFullyDoneFor(const ExpressionProperties & required_properties) const;
    void setFullyDoneFor(const ExpressionProperties & required_properties);
    void updateBestImplementation(GroupExpressionPtr expression, const CostConfig & cost_config);

    /// The cheapest best implementation that answers `required_properties` (see
    /// `GroupExpression::answers`) and is not in `excluded`. Plan extraction excludes the
    /// expressions of the chain that it is building, so the chain cannot come back to itself.
    ExpressionWithCost getBestImplementation(
        const ExpressionProperties & required_properties,
        const CostConfig & cost_config,
        const UnorderedSetWithMemoryTracking<GroupExpression *> & excluded = {}) const;


    void dump(WriteBuffer & out, const CostConfig & cost_config, String indent = {}) const;
    String dump(const CostConfig & cost_config) const;

    VectorWithMemoryTracking<GroupExpressionPtr> logical_expressions;
    VectorWithMemoryTracking<GroupExpressionPtr> physical_expressions;

    /// Best implementation for various required properties, indexed by distribution
    /// shape (node_count, is_replicated) for O(1) bucket lookup.
    UnorderedMapWithMemoryTracking<UInt64, VectorWithMemoryTracking<GroupExpressionPtr>> best_implementations;

    /// Statistics for this group - shared by all expressions in the group
    /// since they all represent the same logical result
    std::optional<ExpressionStatistics> statistics;

private:
    const GroupId group_id;
    bool is_explored = false;
    UnorderedSetWithMemoryTracking<ExpressionProperties, ExpressionPropertiesHash> optimized_properties;  /// Tracks which required properties have had implementation rules applied
    UnorderedSetWithMemoryTracking<ExpressionProperties, ExpressionPropertiesHash> enforced_properties;   /// Tracks which required properties have had enforcer rules applied
    UnorderedSetWithMemoryTracking<ExpressionProperties, ExpressionPropertiesHash> fully_done_properties; /// Tracks which required properties are fully optimized (all stages complete)
    /// Deduplicates physical expressions by STRUCTURAL identity. Bucketed by fingerprint hash;
    /// a hit is dropped only when an existing expression is structurally equal, so a genuine
    /// hash collision keeps both alternatives instead of silently discarding one.
    UnorderedMapWithMemoryTracking<size_t, VectorWithMemoryTracking<GroupExpressionPtr>> physical_expressions_by_fingerprint;
    UnorderedMapWithMemoryTracking<size_t, VectorWithMemoryTracking<GroupExpression *>> logical_expressions_by_fingerprint;

    /// Encode (node_count, is_replicated) into a single key for best_implementations lookup.
    static UInt64 distributionKey(const DistributionDescription & distribution)
    {
        return (static_cast<UInt64>(distribution.node_count) << 1) | static_cast<UInt64>(distribution.is_replicated);
    }
};

using GroupPtr = std::shared_ptr<Group>;
using GroupConstPtr = std::shared_ptr<const Group>;

}
