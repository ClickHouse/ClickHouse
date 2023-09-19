#include <stack>
#include <QueryCoordination/Optimizer/Cost/CostCalculator.h>
#include <QueryCoordination/Optimizer/DeriveOutputProp.h>
#include <QueryCoordination/Optimizer/DeriveRequiredChildProp.h>
#include <QueryCoordination/Optimizer/GroupStep.h>
#include <QueryCoordination/Optimizer/Memo.h>
#include <QueryCoordination/Optimizer/Transform/Transformation.h>
#include <Common/typeid_cast.h>
#include "QueryCoordination/Optimizer/Statistics/DeriveStatistics.h"


namespace DB
{

Memo::Memo(QueryPlan && plan, ContextPtr context_) : context(context_)
{
    root_group = &buildGroup(*plan.getRootNode());

    /// root group must required singleton
    auto & group_nodes = root_group->getGroupNodes();
    for (auto & group_node : group_nodes)
    {
        std::vector<PhysicalProperties> required_child_prop;
        for (size_t j = 0; j < group_node.getChildren().size(); ++j)
        {
            required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
        }
        group_node.addRequiredChildrenProp(required_child_prop);
    }
}

Group & Memo::buildGroup(const QueryPlan::Node & node)
{
    std::vector<Group *> child_groups;
    for (auto * child : node.children)
    {
        auto & child_group = buildGroup(*child);
        child_groups.emplace_back(&child_group);
    }

    groups.emplace_back(Group(++group_id_counter));

    Group & group = groups.back();

    GroupNode group_node(node.step, child_groups);
    group.addGroupNode(group_node,  ++group_node_id_counter);
    return group;
}

GroupNode & Memo::addPlanNodeToGroup(const QueryPlan::Node & node, Group * target_group)
{
    std::vector<Group *> child_groups;
    for (auto * child : node.children)
    {
        if (auto * group_step = typeid_cast<GroupStep *>(child->step.get()))
        {
            child_groups.emplace_back(&group_step->getGroup());
        }
        else
        {
            GroupNode & added_node = addPlanNodeToGroup(*child, nullptr);
            child_groups.emplace_back(&added_node.getGroup());
        }
    }

    GroupNode group_node(node.step, child_groups);
    /// TODO group_node duplicate detection


    if (target_group)
    {
        return target_group->addGroupNode(group_node, ++group_node_id_counter);
    }
    else
    {
        groups.emplace_back(Group(++group_id_counter));
        auto & group = groups.back();

        return group.addGroupNode(group_node, ++group_node_id_counter);
    }
}

void Memo::dump()
{
//    auto & group_nodes = group.getGroupNodes();
//
//    for (auto & group_node : group_nodes)
//    {
//        String child;
//        for (const auto & child_group : group_node.getChildren())
//        {
//            child += std::to_string(child_group->getId()) + ", ";
//        }
//
//        LOG_DEBUG(log, "Group id {}, {}, child group : {}", group.getId(),  group_node.getStep()->getName(), child);
//        for (auto * child_group : group_node.getChildren())
//        {
//            dump(*child_group);
//        }
//    }

    for (auto & group : groups)
    {
        LOG_DEBUG(log, "Group: {}", group.toString());
    }
}

Group & Memo::rootGroup()
{
    return *root_group;
}

void Memo::deriveStat()
{
    deriveStat(*root_group);
}

Statistics Memo::deriveStat(Group & group)
{
    auto & group_nodes = group.getGroupNodes();

    Statistics res;
    for (auto & group_node : group_nodes)
    {
        std::vector<Statistics> child_statistics;
        for (auto * child_group : group_node.getChildren())
        {
            Statistics stat = deriveStat(*child_group);
            child_statistics.emplace_back(stat);
        }

        DeriveStatistics visitor(child_statistics);
        Statistics stat = group_node.accept(visitor);
        group.setStatistics(stat);
        res = stat;
    }
    LOG_DEBUG(log, "Group {} statistics output row size {}", group.getId(), group.getStatistics().getOutputRowSize());
    return res;
}

void Memo::transform()
{
    dump();

    std::unordered_map<Group *, std::vector<StepTree>> group_transformed_node;
    transform(*root_group, group_transformed_node);

    for (auto & [group, sub_query_plans] : group_transformed_node)
    {
        for (auto & sub_query_plan : sub_query_plans)
        {
            addPlanNodeToGroup(*sub_query_plan.getRoot(), group);
        }
    }

    dump();
}

void Memo::transform(Group & group, std::unordered_map<Group *, std::vector<StepTree>> & group_transformed_node)
{
    auto & group_nodes = group.getGroupNodes();

    for (auto & group_node : group_nodes)
    {
        const auto & transformations = Optimizer::getTransformations();

        /// Apply all transformations.
        for (const auto & transformation : transformations)
        {
            if (!transformation.apply)
                continue;

            auto sub_query_plans = transformation.apply(group_node, context);

            if (!sub_query_plans.empty())
                group_transformed_node.emplace(&group, std::move(sub_query_plans));
        }

        for (auto * child_group : group_node.getChildren())
        {
            transform(*child_group, group_transformed_node);
        }
    }
}

void Memo::enforce()
{
    enforce(*root_group, PhysicalProperties{.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});

    dump();
}

std::optional<std::pair<PhysicalProperties, Group::GroupNodeCost>> Memo::enforce(Group & group, const PhysicalProperties & required_prop)
{
    auto & group_nodes = group.getGroupNodes();

    std::vector<std::pair<GroupNode, PhysicalProperties>> collection_enforced_nodes_child_prop;

    for (auto & group_node : group_nodes)
    {
        if (group_node.isEnforceNode())
            continue;

        /// get required prop to child
        DeriveRequiredChildProp visitor(group_node);
        AlternativeChildrenProp alternative_prop = group_node.accept(visitor);

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
                    best_node = enforce(*child_groups[j], required_child_prop);
                }
                cost += best_node->second.cost;
                actual_children_prop.emplace_back(best_node->first);
                children_statistics.emplace_back(child_groups[j]->getStatistics());
            }

            /// derivation output prop by required_prop and children_prop
            DeriveOutputProp output_prop_visitor(required_prop, actual_children_prop, context);
            auto output_prop = group_node.accept(output_prop_visitor);

            group_node.updateBestChild(output_prop, actual_children_prop, cost);

            CostCalculator cost_calc(group.getStatistics(), children_statistics);
            Float64 total_cost = group_node.accept(cost_calc) + (actual_children_prop.empty() ? 0 : cost);

            LOG_DEBUG(log, "Try update prop {} best node total cost {}, group {} group node {}", output_prop.toString(), total_cost, group.getId(), group_node.toString());
            group.updatePropBestNode(output_prop, &group_node, total_cost); /// need keep lowest cost

            /// enforce
            if (!output_prop.satisfy(required_prop))
            {
                enforceGroupNode(required_prop, output_prop, group_node, collection_enforced_nodes_child_prop);
            }
        }
    }

    for (auto & [group_enforce_node, child_prop] : collection_enforced_nodes_child_prop)
    {
        // GroupNode group_enforce_singleton_node(exchange_step);
        auto & added_node = group.addGroupNode(group_enforce_node, ++group_node_id_counter);

        auto child_cost = group.getCostByProp(child_prop);

        CostCalculator cost_calc(group.getStatistics(), {});
        Float64 total_cost = added_node.accept(cost_calc) + child_cost;
        added_node.updateBestChild(required_prop, {child_prop}, child_cost);

        LOG_DEBUG(log, "Try update prop {} best node total cost {}, group {} group node {}", required_prop.toString(), total_cost, group.getId(), added_node.toString());
        group.updatePropBestNode(required_prop, &added_node, total_cost);
    }

    auto best_node = group.getSatisfyBestGroupNode(required_prop);
    return best_node;
}

void Memo::enforceGroupNode(
    const PhysicalProperties & required_prop,
    const PhysicalProperties & output_prop,
    GroupNode & group_node,
    std::vector<std::pair<GroupNode, PhysicalProperties>> & collection)
{
    std::shared_ptr<ExchangeDataStep> exchange_step
        = std::make_shared<ExchangeDataStep>(required_prop.distribution, group_node.getStep()->getOutputStream());

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

    GroupNode group_enforce_node(exchange_step, {});
    collection.emplace_back(std::move(group_enforce_node), output_prop);
}

StepTree Memo::extractPlan()
{
    StepTree sub_plan = extractPlan(*root_group, PhysicalProperties{.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});

    WriteBufferFromOwnString buffer;
    StepTree::ExplainPlanOptions settings;
    sub_plan.explainPlan(buffer, settings);

    LOG_INFO(log, "extractPlan dump: {}", buffer.str());

    return sub_plan;
}

StepTree Memo::extractPlan(Group & group, const PhysicalProperties & required_prop)
{
    const auto & prop_group_node = group.getSatisfyBestGroupNode(required_prop);
    auto & group_node = *prop_group_node->second.group_node;
    LOG_DEBUG(log, "Best node: group id {}, {}, required_prop {}", group.getId(), group_node.getStep()->getName(), required_prop.toString());

    auto child_prop = group_node.getChildrenProp(prop_group_node->first);

    std::vector<StepTreePtr> child_plans;
    const auto & children_group = group_node.getChildren();
    for (size_t i = 0; i < child_prop.size(); ++i)
    {
        StepTreePtr plan_ptr;
        if (group_node.isEnforceNode())
        {
            plan_ptr = std::make_unique<StepTree>(extractPlan(group, child_prop[i]));
        }
        else
        {
            plan_ptr = std::make_unique<StepTree>(extractPlan(*children_group[i], child_prop[i]));
        }
        child_plans.emplace_back(std::move(plan_ptr));
    }

    StepTree plan;
    if (child_plans.size() > 1)
    {
        plan.unitePlans(group_node.getStep(), std::move(child_plans));
    }
    else if (child_plans.size() == 1)
    {
        plan = std::move(*child_plans[0].get());
        plan.addStep(group_node.getStep());
    }
    else
    {
        plan.addStep(group_node.getStep());
    }
    return plan;
}

}
