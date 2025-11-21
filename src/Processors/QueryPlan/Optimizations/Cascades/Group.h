#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>
#include <memory>
#include <vector>

namespace DB
{

using GroupId = size_t;
constexpr GroupId INVALID_GROUP_ID = -1;

class GroupExpression;
using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

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
    bool isImplemented() const { return is_implemented; }
    void setImplemented() { is_implemented = true; }
    void updateBestImplementation(GroupExpressionPtr expression);
    ExpressionWithCost getBestImplementation(const ExpressionProperties & required_properties) const;

    void dump(WriteBuffer & out, String indent = {}) const;
    String dump() const;

    std::vector<GroupExpressionPtr> logical_expressions;
    std::vector<GroupExpressionPtr> physical_expressions;

    /// Best implementation for various required properties
    std::set<GroupExpressionPtr> best_implementations;

private:
    const GroupId group_id;
    bool is_explored = false;
    bool is_implemented = false;
};

using GroupPtr = std::shared_ptr<Group>;
using GroupConstPtr = std::shared_ptr<const Group>;

}
