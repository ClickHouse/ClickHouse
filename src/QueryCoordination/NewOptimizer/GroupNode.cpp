#include <QueryCoordination/NewOptimizer/GroupNode.h>
#include <QueryCoordination/NewOptimizer/Group.h>

namespace DB
{

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
    for (auto & [output_prop, child_prop] : best_prop_map)
    {
        prop_map += output_prop.toString() + "-";

        for (auto & c_prop : child_prop)
        {
            prop_map += c_prop.toString() + "|";
        }

        prop_map += ", ";
    }

    res += "best prop map: " + prop_map;

    return res;
}

}
