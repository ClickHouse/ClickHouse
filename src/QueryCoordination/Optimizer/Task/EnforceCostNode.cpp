#include <QueryCoordination/Optimizer/Cost/CostCalculator.h>
#include <QueryCoordination/Optimizer/DeriveOutputProp.h>
#include <QueryCoordination/Optimizer/DeriveRequiredChildProp.h>
#include <QueryCoordination/Optimizer/DeriveStatistics.h>
#include <QueryCoordination/Optimizer/Group.h>
#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/Memo.h>
#include <QueryCoordination/Optimizer/Task/EnforceCostGroup.h>
#include <QueryCoordination/Optimizer/Task/EnforceCostNode.h>
#include <QueryCoordination/Optimizer/Task/OptimizeGroup.h>


namespace DB
{

void EnforceCostNode::execute()
{
    if (group_node.isEnforceNode())
        return;

    /// get required prop to child
    DeriveRequiredChildProp visitor(group_node);
    AlternativeChildrenProp alternative_prop = group_node.accept(visitor);

    std::vector<std::pair<GroupNode, PhysicalProperties>> collection;
    /// every alternative prop, required to child
    for (auto & required_child_props : alternative_prop)
    {
        std::vector<PhysicalProperties> actual_children_prop;
        std::vector<Statistics> children_statistics;

        Float64 cost = 0;
        const auto & child_groups = group_node.getChildren();
        for (size_t j = 0; j < group_node.getChildren().size(); ++j)
        {
            auto required_child_prop = required_child_props[j];
            auto best_node = child_groups[j]->getSatisfyBestGroupNode(required_child_prop);
            if (!best_node)
            {
                pushTask(clone());

                auto opt_group = std::make_unique<EnforceCostGroup>(memo, stack, context, *child_groups[j], required_child_prop);
                pushTask(std::move(opt_group));
                return;
            }
            cost += best_node->second.cost;
            actual_children_prop.emplace_back(best_node->first);
            children_statistics.emplace_back(child_groups[j]->getStatistics());
        }

        /// derivation output prop by required_prop and children_prop
        DeriveOutputProp output_prop_visitor(required_prop, actual_children_prop);
        auto output_prop = group_node.accept(output_prop_visitor);

        group_node.updateBestChild(output_prop, actual_children_prop, cost);

        CostCalculator cost_calc(output_prop, group.getStatistics(), children_statistics);
        Float64 total_cost = group_node.accept(cost_calc) + (actual_children_prop.empty() ? 0 : cost);

        group.updatePropBestNode(output_prop, &group_node, total_cost); /// need keep lowest cost

        /// enforce
        if (!output_prop.satisfy(required_prop))
        {
            enforceGroupNode(output_prop, collection);
        }
    }

    for (auto & [group_enforce_node, child_prop] : collection)
    {
        // GroupNode group_enforce_singleton_node(exchange_step);
        auto & added_node = group.addGroupNode(group_enforce_node);

        auto child_cost = group.getCostByProp(child_prop);

        CostCalculator cost_calc(required_prop, group.getStatistics(), {});
        Float64 total_cost = added_node.accept(cost_calc) + child_cost;
        added_node.updateBestChild(required_prop, {child_prop}, child_cost);

        group.updatePropBestNode(required_prop, &added_node, total_cost);
    }
}

void EnforceCostNode::enforceGroupNode(
    const PhysicalProperties & output_prop,
    std::vector<std::pair<GroupNode, PhysicalProperties>> & collection)
{
    std::shared_ptr<ExchangeDataStep> exchange_step;

    switch (required_prop.distribution.type)
    {
        case PhysicalProperties::DistributionType::Singleton: {
            exchange_step = std::make_shared<ExchangeDataStep>(
                PhysicalProperties::DistributionType::Singleton, group_node.getStep()->getOutputStream());
            break;
        }
        case PhysicalProperties::DistributionType::Replicated: {
            exchange_step = std::make_shared<ExchangeDataStep>(
                PhysicalProperties::DistributionType::Replicated, group_node.getStep()->getOutputStream());
            // exchange_step = std::make_shared<ExchangeDataStep>(Replicated);
            break;
        }
        case PhysicalProperties::DistributionType::Hashed: {
            exchange_step = std::make_shared<ExchangeDataStep>(
                PhysicalProperties::DistributionType::Hashed, group_node.getStep()->getOutputStream());
            // exchange_step = std::make_shared<ExchangeDataStep>(Hashed);
            break;
        }
        default:
            break;
    }

    if (!output_prop.sort_description.empty())
    {
        auto * sorting_step = typeid_cast<SortingStep *>(group_node.getStep().get());
        if (sorting_step)
        {
            const SortDescription & sort_description = sorting_step->getSortDescription();
            const UInt64 limit = sorting_step->getLimit();
            const auto max_block_size = context->getSettingsRef().max_block_size;
            const auto exact_rows_before_limit = context->getSettingsRef().exact_rows_before_limit;

            ExchangeDataStep::SortInfo sort_info{
                .max_block_size = max_block_size,
                .always_read_till_end = exact_rows_before_limit,
                .limit = limit,
                .result_description = sort_description};
            exchange_step->setSortInfo(sort_info);
        }
    }

    GroupNode group_enforce_node(exchange_step, true);

    group_enforce_node.setId(memo.fetchGroupNodeId());

    collection.emplace_back(std::move(group_enforce_node), output_prop);
}

std::unique_ptr<EnforceCostNode> EnforceCostNode::clone()
{
    return std::make_unique<EnforceCostNode>(memo, stack, context, group, group_node, required_prop);
}

}
