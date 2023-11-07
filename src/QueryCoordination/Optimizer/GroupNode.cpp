#include <QueryCoordination/Optimizer/Group.h>
#include <QueryCoordination/Optimizer/GroupNode.h>

namespace DB
{

GroupNode::GroupNode(QueryPlanStepPtr step_, const std::vector<Group *> & children_, bool is_enforce_node_)
    : id(0), step(step_), group(nullptr), children(children_), is_enforce_node(is_enforce_node_), stats_derived(false)
{
}

GroupNode::~GroupNode() = default;
GroupNode::GroupNode(GroupNode &&) noexcept = default;

void GroupNode::addChild(Group & child)
{
    children.emplace_back(&child);
}

size_t GroupNode::childSize() const
{
    return children.size();
}

std::vector<Group *> GroupNode::getChildren() const
{
    return children;
}

QueryPlanStepPtr GroupNode::getStep() const
{
    return step;
}

Group & GroupNode::getGroup()
{
    return *group;
}

void GroupNode::setGroup(Group * group_)
{
    group = group_;
}

bool GroupNode::updateBestChild(
    const PhysicalProperties & physical_properties, const std::vector<PhysicalProperties> & child_properties, Cost child_cost)
{
    if (!prop_to_best_child.contains(physical_properties) || child_cost < prop_to_best_child[physical_properties].cost)
    {
        prop_to_best_child[physical_properties] = {child_properties, child_cost};
        return true;
    }
    return false;
}

const std::vector<PhysicalProperties> & GroupNode::getChildrenProp(const PhysicalProperties & physical_properties)
{
    return prop_to_best_child[physical_properties].child_prop;
}

void GroupNode::addRequiredChildrenProp(ChildrenProp & child_pro)
{
    required_children_prop.emplace_back(child_pro);
}

AlternativeChildrenProp & GroupNode::getRequiredChildrenProp()
{
    return required_children_prop;
}

bool GroupNode::hasRequiredChildrenProp() const
{
    return !required_children_prop.empty();
}

bool GroupNode::isEnforceNode() const
{
    return is_enforce_node;
}

UInt32 GroupNode::getId() const
{
    return id;
}

void GroupNode::setId(UInt32 id_)
{
    id = id_;
}

void GroupNode::setStatsDerived()
{
    stats_derived = true;
}

bool GroupNode::hasStatsDerived() const
{
    return stats_derived;
}

String GroupNode::getDescription() const
{
    String res;
    res += "node " + std::to_string(getId()) + "(";
    res += step->getName() + ")";
    return res;
}

String GroupNode::toString() const
{
    String res;
    res += "node id: " + std::to_string(getId()) + ", ";
    res += step->getName() + " (" + step->getStepDescription() + "), ";
    res += "is_enforce_node: " + std::to_string(is_enforce_node) + ", ";

    String child_ids;
    for (auto * child : children)
        child_ids += std::to_string(child->getId()) + ", ";
    res += "children: " + child_ids;

    String prop_map;
    for (const auto & [output_prop, child_prop_cost] : prop_to_best_child)
    {
        prop_map += output_prop.toString() + "-";

        for (const auto & c_prop : child_prop_cost.child_prop)
            prop_map += c_prop.toString() + "|";
    }

    res += "best child prop map: " + prop_map;

    return res;
}

}
