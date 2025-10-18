#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
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

    void dump(WriteBuffer & out, String indent = {}) const;
    String dump() const;

    std::vector<GroupExpressionPtr> logical_expressions;
    std::vector<GroupExpressionPtr> physical_expressions;
    ExpressionWithCost best_implementation;

private:
    const GroupId group_id;
    bool is_explored = false;
};

using GroupPtr = std::shared_ptr<Group>;
using GroupConstPtr = std::shared_ptr<const Group>;

}
