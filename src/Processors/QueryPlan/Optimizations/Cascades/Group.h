#pragma once

#include <memory>
#include <vector>
#include <base/types.h>
#include <IO/WriteBuffer.h>

namespace DB
{

using GroupId = size_t;
constexpr GroupId INVALID_GROUP_ID = -1;

class GroupExpression;
using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

class Group
{
public:
    explicit Group(GroupId group_id_)
        : group_id(group_id_)
    {}

    void addExpression(GroupExpressionPtr group_expression);
    bool isExplored() const { return is_explored; }
    void setExplored() { is_explored = true; }

    void dump(WriteBuffer & out, String indent = {}) const;

    std::vector<GroupExpressionPtr> expressions;

private:
    const GroupId group_id;
    bool is_explored = false;
};

using GroupPtr = std::shared_ptr<Group>;
using GroupConstPtr = std::shared_ptr<const Group>;

}
