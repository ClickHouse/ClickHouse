#pragma once

#include <QueryCoordination/Optimizer/StepTree.h>

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

    std::vector<StepTreePtr> bind();

private:
    std::vector<StepTreePtr> extractGroupNode(const Pattern & pattern_, GroupNodePtr group_node_);

    std::vector<StepTreePtr> extractGroup(const Pattern & pattern_, Group & group);

    const Pattern & pattern;

    GroupNodePtr group_node;
};

}
