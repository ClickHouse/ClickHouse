#include <QueryCoordination/NewOptimizer/GroupNode.h>
#include <QueryCoordination/NewOptimizer/Group.h>

namespace DB
{

void GroupNode::updateBestChild(const PhysicalProperties & physical_properties, const std::vector<PhysicalProperties> & child_properties, Float64 child_cost)
{
    if (!prop_to_best_child.contains(physical_properties) || child_cost < prop_to_best_child[physical_properties].cost)
    {
        prop_to_best_child[physical_properties] = {child_properties, child_cost};
    }
}

const std::vector<PhysicalProperties> & GroupNode::getChildrenProp(const PhysicalProperties & physical_properties)
{
    return prop_to_best_child[physical_properties].child_prop;
}

String GroupNode::toString() const
{
    String res;
    res += "node id: " + std::to_string(getId()) + ", ";
    res += step->getName() + step->getStepDescription() + ", ";
    res += "is_enforce_node: " + std::to_string(is_enforce_node) + ", ";

    String child_ids;
    for (auto * child : children)
    {
        child_ids += std::to_string(child->getId()) + ", ";
    }
    res += "children: " + child_ids;

    String prop_map;
    for (const auto & [output_prop, child_prop_cost] : prop_to_best_child)
    {
        prop_map += output_prop.toString() + "-";

        for (const auto & c_prop : child_prop_cost.child_prop)
        {
            prop_map += c_prop.toString() + "|";
        }
    }

    res += "best child prop map: " + prop_map;

    return res;
}

}
