#include <QueryCoordination/NewOptimizer/Memo.h>
#include <QueryCoordination/NewOptimizer/Rule/Optimizations.h>
#include <QueryCoordination/NewOptimizer/derivationProperties.h>
#include <QueryCoordination/NewOptimizer/Cost/CostCalc.h>
#include <stack>

namespace DB
{

Memo::Memo(QueryPlan && plan, ContextPtr context_) : context(context_)
{
    root_group = &buildGroup(*plan.getRootNode());
}

void Memo::addPlanNodeToGroup(const QueryPlan::Node & node, Group & target_group)
{
    GroupNode group_node(node.step);

    /// get logical equivalence child
    const auto children = target_group.getOneGroupNode().getChildren();

    if (node.children.size())
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

    if (node.children.size())
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

    groups.emplace_back(Group(group_node));
    return groups.back();
}

Group & Memo::buildGroup(const QueryPlan::Node & node)
{
    GroupNode group_node(node.step);

    for (auto * child : node.children)
    {
        auto & child_group = buildGroup(*child);
        group_node.addChild(child_group);
    }

    groups.emplace_back(Group(group_node));
    return groups.back();
}

void Memo::dump(Group & group)
{
    auto & group_nodes = group.getGroupNodes();

    for (auto & group_node : group_nodes)
    {
        for (auto * child_group : group_node.getChildren())
        {
            dump(*child_group);
        }
    }
}

void Memo::transform()
{
    std::unordered_map<Group *, std::vector<SubQueryPlan>> group_transformed_node;
    transform(*root_group, group_transformed_node);

    for (auto & [group, sub_query_plans] : group_transformed_node)
    {
        for (auto & sub_query_plan : sub_query_plans)
        {
            addPlanNodeToGroup(sub_query_plan.getRoot(), *group);
        }
    }
}

void Memo::transform(Group & group, std::unordered_map<Group *, std::vector<SubQueryPlan>> & group_transformed_node)
{
    const auto & group_nodes = group.getGroupNodes();

    for (const auto & group_node : group_nodes)
    {
        const auto & step = group_node.getStep();
        const auto & sub_query_plans = QueryPlanOptimizations::trySplitAggregation(step, context);

        if (!sub_query_plans.empty())
            group_transformed_node.emplace(&group, sub_query_plans);

        for (auto * child_group : group_node.getChildren())
        {
            transform(*child_group, group_transformed_node);
        }
    }
}

void Memo::enforce()
{
    enforce(*root_group, PhysicalProperties{.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
}

Float64 Memo::enforce(Group & group, const PhysicalProperties & required_properties)
{
    auto & group_nodes = group.getGroupNodes();

    std::vector<std::pair<GroupNode, PhysicalProperties>> enforced_nodes_child_prop;

    for (auto & group_node : group_nodes)
    {
        auto output_prop_required_child_prop = DB::derivationProperties(group_node.getStep()); /// TODO cache

        for (auto & [output_properties, required_child_prop] : output_prop_required_child_prop)
        {
            /// every alternative prop
            Float64 min_cost = std::numeric_limits<Float64>::max();
            size_t min_cost_index = 0;
            for (size_t i = 0; i < required_child_prop.size(); ++i)
            {
                Float64 cost = 0;
                auto & child_groups = group_node.getChildren();
                for (size_t j = 0; j < group_node.getChildren().size(); ++j)
                {
                    cost += enforce(*child_groups[j], required_child_prop[i][j]);
                }
                if (cost < min_cost)
                {
                    min_cost = cost;
                    min_cost_index = i;
                }
            }

            /// TODO calc cost for group_node cost + min_cost(child total cost) = total cost
            Float64 total_cost = calcCost(group_node.getStep()) + (required_child_prop.empty() ? 0 : min_cost);
            group_node.addLowestCostChildPropertyMap(output_properties, required_child_prop.empty() ? std::vector<PhysicalProperties>() : required_child_prop[min_cost_index]); /// need keep lowest cost
            group.addLowestCostGroupNode(output_properties, &group_node, total_cost); /// need keep lowest cost

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

                GroupNode group_enforce_node(exchange_step, true);
                enforced_nodes_child_prop.emplace_back(std::move(group_enforce_node), output_properties);
            }
        }
    }

    for (auto & [group_enforce_node, output_properties] : enforced_nodes_child_prop)
    {
        // GroupNode group_enforce_singleton_node(exchange_step);
        auto & added_node = group.addGroupNode(group_enforce_node);

        /// TODO calc cost for group_enforce_singleton_node cost + group->getCost(output_properties) = total cost
        Float64 total_cost = calcCost(added_node.getStep()) + group.getCost(output_properties);
        added_node.addLowestCostChildPropertyMap(required_properties, {output_properties});
        group.addLowestCostGroupNode(required_properties, &added_node, total_cost);
    }

    /// extract plan
    return group.getLowestCost(required_properties);
}

QueryPlan Memo::extractPlan()
{
    return extractPlan(*root_group, PhysicalProperties{.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
}

QueryPlan Memo::extractPlan(Group & group, const PhysicalProperties & required_properties)
{
    auto & group_node = group.getBestGroupNode(required_properties);
    auto child_properties = group_node.getChildProperties(required_properties);

    std::vector<QueryPlanPtr> child_plans;
    auto & children_group = group_node.getChildren();
    for (size_t i = 0; i < child_properties.size(); ++i)
    {
        QueryPlanPtr plan_ptr;
        if (group_node.isEnforceNode())
        {
            plan_ptr = std::make_unique<QueryPlan>(extractPlan(group, child_properties[i]));
        }
        else
        {
            plan_ptr = std::make_unique<QueryPlan>(extractPlan(*children_group[i], child_properties[i]));
        }
        child_plans.emplace_back(std::move(plan_ptr));
    }

    QueryPlan plan;
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
