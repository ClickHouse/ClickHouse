#include <Optimizer/Group.h>

namespace DB
{

Group::Group(UInt32 id_) : id(id_)
{
}

GroupNodePtr Group::getOneGroupNode()
{
    return group_nodes.front();
}

const std::list<GroupNodePtr> & Group::getGroupNodes() const
{
    return group_nodes;
}

std::list<GroupNodePtr> & Group::getGroupNodes()
{
    return group_nodes;
}

void Group::addGroupNode(GroupNodePtr group_node, UInt32 group_node_id)
{
    group_node->setId(group_node_id);
    group_node->setGroup(this);
    group_nodes.emplace_back(std::move(group_node));
}

std::optional<std::pair<PhysicalProperties, Group::NodeAndCost>>
Group::getSatisfiedBestGroupNode(const PhysicalProperties & required_properties) const
{
    /// Use a NaN
    auto min_cost = std::numeric_limits<Float64>::max() + 1.0;
    std::pair<PhysicalProperties, NodeAndCost> res;

    for (const auto & [properties, group_node_cost] : prop_to_best)
    {
        if (properties.satisfy(required_properties))
        {
            if (group_node_cost.cost.get() < min_cost)
            {
                min_cost = group_node_cost.cost.get();
                res.first = properties;
                res.second = group_node_cost;
            }
        }
    }

    if (!res.second.group_node)
        return {};

    return {res};
}

bool Group::updatePropBestNode(const PhysicalProperties & properties, GroupNodePtr group_node, Cost cost)
{
    if (!prop_to_best.contains(properties) || cost < prop_to_best[properties].cost)
    {
        prop_to_best[properties] = {group_node, cost};
        return true;
    }
    return false;
}

Cost Group::getCostByProp(const PhysicalProperties & properties)
{
    return prop_to_best[properties].cost;
}

UInt32 Group::getId() const
{
    return id;
}

void Group::setStatistics(Statistics & statistics_)
{
    statistics = statistics_;
}

const Statistics & Group::getStatistics() const
{
    return statistics;
}

void Group::setStatsDerived()
{
    stats_derived = true;
}

bool Group::hasStatsDerived() const
{
    return stats_derived;
}

String Group::getDescription() const
{
    String res = "Group ";
    res += std::to_string(getId());

    if (!group_nodes.empty())
        res += " with first node: { " + group_nodes.front()->getDescription() + "}";
    return res;
}

String Group::toString() const
{
    String res;
    res += std::to_string(getId()) + ", ";

    res += "group_nodes: ";
    for (const auto & node : group_nodes)
        res += "{ " + node->toString() + "}, ";

    String prop_map;
    for (const auto & [prop, cost_group_node] : prop_to_best)
    {
        prop_map += "{ " + prop.toString() + "- (" + std::to_string(cost_group_node.cost.get()) + ", "
            + std::to_string(cost_group_node.group_node->getId()) + ")}, ";
    }

    res += "prop_to_best: " + prop_map;
    return res;
}

}
