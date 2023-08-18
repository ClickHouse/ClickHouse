#include <QueryCoordination/NewOptimizer/Group.h>

namespace DB
{

GroupNode & Group::addGroupNode(GroupNode & group_plan_node)
{
    group_nodes.emplace_back(std::move(group_plan_node));
    return group_nodes.back();
}

Float64 Group::getSatisfyBestCost(const PhysicalProperties & required_properties) const
{
    return getSatisfyBestGroupNode(required_properties).second.cost;
}

std::pair<PhysicalProperties, Group::GroupNodeCost> Group::getSatisfyBestGroupNode(const PhysicalProperties & required_properties) const
{
    Float64 min_cost = std::numeric_limits<Float64>::max();

    std::pair<PhysicalProperties, GroupNodeCost> res;

    for (const auto & [properties, group_node_cost] : prop_to_best_node)
    {
        if (properties.satisfy(required_properties))
        {
            if (group_node_cost.cost < min_cost)
            {
                min_cost = group_node_cost.cost;
                res.first = properties;
                res.second = group_node_cost;
            }
        }
    }

    if (!res.second.group_node)
        throw;

    return res;
}

void Group::updatePropBestNode(const PhysicalProperties & properties, GroupNode * group_node, Float64 cost)
{
    if (!prop_to_best_node.contains(properties) || cost < prop_to_best_node[properties].cost)
    {
        prop_to_best_node[properties] = {group_node, cost};
    }
}

Float64 Group::getCostByProp(const PhysicalProperties & properties)
{
    return prop_to_best_node[properties].cost;
}

String Group::toString() const
{
    String res;
    res += std::to_string(getId()) + ", ";

    res += "group_nodes: ";
    for (const auto & node : group_nodes)
    {
        res += "{ " + node.toString() + "}, ";
    }

    String prop_map;
    for(const auto & [prop, cost_group_node] : prop_to_best_node)
    {
        prop_map += "{ " + prop.toString() + "- (" + std::to_string(cost_group_node.cost) + "，" + std::to_string(cost_group_node.group_node->getId()) + ")}, ";
    }

    res += "prop_to_best_node: " + prop_map;
    return res;
}

}
