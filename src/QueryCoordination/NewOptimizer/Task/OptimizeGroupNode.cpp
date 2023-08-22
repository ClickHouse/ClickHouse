#include <QueryCoordination/NewOptimizer/Cost/CostCalculator.h>
#include <QueryCoordination/NewOptimizer/DerivationOutputProp.h>
#include <QueryCoordination/NewOptimizer/Group.h>
#include <QueryCoordination/NewOptimizer/GroupNode.h>
#include <QueryCoordination/NewOptimizer/Task/OptimizeGroupNode.h>
#include <QueryCoordination/NewOptimizer/derivationRequiredChildProp.h>

namespace DB
{

void OptimizeGroupNode::execute()
{
    if (group_node.isEnforceNode())
        return;

    /// get required prop to child
    AlternativeChildrenProp alternative_prop = derivationRequiredChildProp(group_node);

    /// every alternative prop, required to child
    for (auto & required_child_props : alternative_prop)
    {
        std::vector<PhysicalProperties> actual_children_prop;

        Float64 cost = 0;
        const auto & child_groups = group_node.getChildren();
        for (size_t j = 0; j < group_node.getChildren().size(); ++j)
        {
            auto required_child_prop = required_child_props[j];
            auto best_node = child_groups[j]->getSatisfyBestGroupNode(required_child_prop);
            if (!best_node)
            {
                /// TODO push task OptimizeGroup
//                best_node = enforce(*child_groups[j], required_child_prop);
            }
            cost += best_node->second.cost;
            actual_children_prop.emplace_back(best_node->first);
        }

        /// derivation output prop by required_prop and children_prop
        auto output_prop = DerivationOutputProp(group_node, required_prop, actual_children_prop).derivationOutputProp();
        group_node.updateBestChild(output_prop, actual_children_prop, cost);

        Float64 total_cost = calcCost(group_node.getStep()) + (actual_children_prop.empty() ? 0 : cost);
        group.updatePropBestNode(output_prop, &group_node, total_cost); /// need keep lowest cost

        /// enforce
        if (!output_prop.satisfy(required_prop))
        {
            enforceGroupNode(required_prop, output_prop, group_node, collection_enforced_nodes_child_prop);
        }
    }

    for (auto & [group_enforce_node, output_prop] : collection_enforced_nodes_child_prop)
    {
        // GroupNode group_enforce_singleton_node(exchange_step);
        auto & added_node = group.addGroupNode(group_enforce_node);

        auto child_cost = group.getCostByProp(output_prop);

        Float64 total_cost = calcCost(added_node.getStep()) + child_cost;
        added_node.updateBestChild(required_prop, {output_prop}, child_cost);

        group.updatePropBestNode(required_prop, &added_node, total_cost);
    }
}

}
