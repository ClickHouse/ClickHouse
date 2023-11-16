#pragma once

#include <Optimizer/SubQueryPlan.h>

namespace DB
{

class Pattern;
class GroupNode;
using GroupNodePtr = std::shared_ptr<GroupNode>;
class Group;

class Binder
{
public:
    Binder(const Pattern & pattern_, GroupNodePtr group_node_);

    std::vector<SubQueryPlanPtr> bind();

private:
    std::vector<SubQueryPlanPtr> extractGroupNode(const Pattern & pattern_, GroupNodePtr group_node_);

    std::vector<SubQueryPlanPtr> extractGroup(const Pattern & pattern_, Group & group);

    const Pattern & pattern;

    GroupNodePtr group_node;
};

}
