#include <QueryCoordination/NewOptimizer/Memo.h>
#include <QueryCoordination/NewOptimizer/Rule/Optimizations.h>
#include <QueryCoordination/NewOptimizer/derivationProperties.h>
#include <QueryCoordination/NewOptimizer/Cost/CostCalc.h>
#include <stack>
#include <Common/typeid_cast.h>


namespace DB
{

Memo::Memo(QueryPlan && plan, ContextPtr context_) : context(context_)
{
    root_group = &buildGroup(*plan.getRootNode());
}

void Memo::addPlanNodeToGroup(const QueryPlan::Node & node, Group & target_group)
{
    GroupNode group_node(node.step);

    group_node.setId(++group_node_id_counter);

    /// get logical equivalence child
    const auto children = target_group.getOneGroupNode().getChildren();

    if (!node.children.empty())
    {
        for (auto * child : node.children)
        {
            auto & child_group = buildGroup(*child, children);
            group_node.addChild(child_group);
        }
    }
    else
    {
        group_node.replaceChildren(children);
    }

    target_group.addGroupNode(group_node);
}

Group & Memo::buildGroup(const QueryPlan::Node & node, const std::vector<Group *> children_groups)
{
    /// children_groups push to bottom

    GroupNode group_node(node.step);

    group_node.setId(++group_node_id_counter);

    if (!node.children.empty())
    {
        for (auto * child : node.children)
        {
            auto & child_group = buildGroup(*child, children_groups);
            group_node.addChild(child_group);
        }
    }
    else
    {
        group_node.replaceChildren(children_groups);
    }

    groups.emplace_back(Group(group_node, ++group_id_counter));
    return groups.back();
}

Group & Memo::buildGroup(const QueryPlan::Node & node)
{
    GroupNode group_node(node.step);

    group_node.setId(++group_node_id_counter);

    for (auto * child : node.children)
    {
        auto & child_group = buildGroup(*child);
        group_node.addChild(child_group);
    }

    groups.emplace_back(Group(group_node, ++group_id_counter));
    return groups.back();
}

void Memo::dump(Group & /*group*/)
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

void Memo::transform()
{
    dump(*root_group);

    std::unordered_map<Group *, std::vector<SubQueryPlan>> group_transformed_node;
    transform(*root_group, group_transformed_node);

    for (auto & [group, sub_query_plans] : group_transformed_node)
    {
        for (auto & sub_query_plan : sub_query_plans)
        {
            addPlanNodeToGroup(sub_query_plan.getRoot(), *group);
        }
    }

    dump(*root_group);
}

void Memo::transform(Group & group, std::unordered_map<Group *, std::vector<SubQueryPlan>> & group_transformed_node)
{
    const auto & group_nodes = group.getGroupNodes();

    for (const auto & group_node : group_nodes)
    {
        const auto & step = group_node.getStep();

        const auto & optimizations = NewOptimizer::getOptimizations();

        /// Apply all optimizations.
        for (const auto & optimization : optimizations)
        {
            if (!optimization.apply)
                continue;

            auto sub_query_plans = optimization.apply(step, context);

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

    dump(*root_group);
}

Float64 Memo::enforce(Group & group, const PhysicalProperties & required_properties)
{
    auto & group_nodes = group.getGroupNodes();

    std::vector<std::pair<GroupNode, PhysicalProperties>> enforced_nodes_child_prop;

    bool is_root_group = &group == root_group;

    for (auto & group_node : group_nodes)
    {
        if (group_node.isEnforceNode())
            continue;

        PropAndAlternativeChildrenProp prop_required_child_prop;
        if (is_root_group)
        {
            PropAndAlternativeChildrenProp res;
            PhysicalProperties properties{.distribution = {.type = PhysicalProperties::DistributionType::Singleton}};
            AlternativeChildrenProp alternative_properties;
            for (size_t j = 0; j < group_node.getChildren().size(); ++j)
            {
                std::vector<PhysicalProperties> required_child_prop;
                required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
                alternative_properties.emplace_back(required_child_prop);
            }
            res[properties] = alternative_properties;
            prop_required_child_prop = std::move(res);
        }
        else
        {
            prop_required_child_prop = DB::derivationProperties(group_node.getStep()); /// TODO cache
        }

        for (auto & [output_properties, required_child_prop] : prop_required_child_prop)
        {
            Float64 min_cost = std::numeric_limits<Float64>::max();
            /// every alternative prop
            for (size_t i = 0; i < required_child_prop.size(); ++i)
            {
                Float64 cost = 0;
                const auto & child_groups = group_node.getChildren();
                for (size_t j = 0; j < group_node.getChildren().size(); ++j)
                {
                    cost += enforce(*child_groups[j], required_child_prop[i][j]);
                    LOG_DEBUG(log, "Enforced end cost group id {}, {}, output_properties {}, required_child_prop {}", group.getId(), group_node.getStep()->getName(), output_properties.toString(), required_child_prop[i][j].toString());
                }
                min_cost = std::min(cost, min_cost);
                group_node.updateBestChild(output_properties, required_child_prop.empty() ? std::vector<PhysicalProperties>() : required_child_prop[i], cost);
            }

            Float64 total_cost = calcCost(group_node.getStep()) + (required_child_prop.empty() ? 0 : min_cost);

            LOG_DEBUG(log, "Group id {}, {}, output_properties {}, total cost {}", group.getId(), group_node.getStep()->getName(), output_properties.toString(), total_cost);

            group.updatePropBestNode(output_properties, &group_node, total_cost); /// need keep lowest cost

            if (!output_properties.satisfy(required_properties))
            {
                std::shared_ptr<ExchangeDataStep> exchange_step;

                switch (required_properties.distribution.type)
                {
                    case PhysicalProperties::DistributionType::Singleton:
                    {
                        exchange_step = std::make_shared<ExchangeDataStep>(PhysicalProperties::DistributionType::Singleton, group_node.getStep()->getOutputStream());
                        break;
                    }
                    case PhysicalProperties::DistributionType::Replicated:
                    {
                        exchange_step = std::make_shared<ExchangeDataStep>(PhysicalProperties::DistributionType::Replicated, group_node.getStep()->getOutputStream());
                        // exchange_step = std::make_shared<ExchangeDataStep>(Replicated);
                        break;
                    }
                    case PhysicalProperties::DistributionType::Hashed:
                    {
                        exchange_step = std::make_shared<ExchangeDataStep>(PhysicalProperties::DistributionType::Hashed, group_node.getStep()->getOutputStream());
                        // exchange_step = std::make_shared<ExchangeDataStep>(Hashed);
                        break;
                    }
                    default:
                        break;
                }

                if (!output_properties.sort_description.empty())
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

                group_enforce_node.setId(++group_node_id_counter);

                enforced_nodes_child_prop.emplace_back(std::move(group_enforce_node), output_properties);
            }
        }
    }

    for (auto & [group_enforce_node, output_properties] : enforced_nodes_child_prop)
    {
        // GroupNode group_enforce_singleton_node(exchange_step);
        auto & added_node = group.addGroupNode(group_enforce_node);

        auto child_cost = group.getCostByProp(output_properties);

        Float64 total_cost = calcCost(added_node.getStep()) + child_cost;
        added_node.updateBestChild(required_properties, {output_properties}, child_cost);
        group.updatePropBestNode(required_properties, &added_node, total_cost);
    }

    /// extract plan
    Float64 lowest_cost = group.getSatisfyBestCost(required_properties);

    LOG_DEBUG(log, "Group id {}, required_properties {}, lowest cost {}", group.getId(), required_properties.toString(), lowest_cost);
    return lowest_cost;
}

SubQueryPlan Memo::extractPlan()
{
    SubQueryPlan sub_plan = extractPlan(*root_group, PhysicalProperties{.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});

    WriteBufferFromOwnString buffer;
    SubQueryPlan::ExplainPlanOptions settings;
    sub_plan.explainPlan(buffer, settings);

    LOG_INFO(log, "extractPlan dump: {}", buffer.str());

    return sub_plan;
}

SubQueryPlan Memo::extractPlan(Group & group, const PhysicalProperties & required_properties)
{
    const auto & properties_group_node = group.getSatisfyBestGroupNode(required_properties);
    auto & group_node = *properties_group_node.second.group_node;
    LOG_DEBUG(log, "Best node: group id {}, {}, required_properties {}", group.getId(), group_node.getStep()->getName(), required_properties.toString());

    auto child_properties = group_node.getChildrenProp(properties_group_node.first);

    std::vector<SubQueryPlanPtr> child_plans;
    const auto & children_group = group_node.getChildren();
    for (size_t i = 0; i < child_properties.size(); ++i)
    {
        SubQueryPlanPtr plan_ptr;
        if (group_node.isEnforceNode())
        {
            plan_ptr = std::make_unique<SubQueryPlan>(extractPlan(group, child_properties[i]));
        }
        else
        {
            plan_ptr = std::make_unique<SubQueryPlan>(extractPlan(*children_group[i], child_properties[i]));
        }
        child_plans.emplace_back(std::move(plan_ptr));
    }

    SubQueryPlan plan;
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

//
//void Memo::derivationProperties()
//{
//    derivationProperties(root_group);
//}
//
//void Memo::derivationProperties(Group * group)
//{
//    auto & group_nodes = group->getGroupNodes();
//
//    for (auto & group_node : group_nodes)
//    {
//        const auto & step = group_node.getStep();
//        auto properties = DB::derivationProperties(step);
//
//        group_node.addLowestCostChildPropertyMap(properties);
//
//        for (auto * child_group : group_node.getChildren())
//        {
//            derivationProperties(child_group);
//        }
//    }
//}

}
