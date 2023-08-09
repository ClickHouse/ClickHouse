#include <QueryCoordination/NewOptimizer/Memo.h>
#include <QueryCoordination/NewOptimizer/Rule/Optimizations.h>
#include <QueryCoordination/NewOptimizer/derivationProperties.h>
#include <stack>

namespace DB
{

Memo::Memo(QueryPlan && plan)
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
            group_node.addChild(&child_group);
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
            group_node.addChild(&child_group);
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
        group_node.addChild(&child_group);
    }

    groups.emplace_back(Group(group_node));
    return groups.back();
}

void Memo::dump(Group * group)
{
    auto & group_nodes = group->getGroupNodes();

    for (auto & group_node : group_nodes)
    {
        for (auto * child_group : group_node.getChildren())
        {
            dump(child_group);
        }
    }
}

void Memo::transform()
{
    std::unordered_map<Group *, std::vector<SubQueryPlan>> group_transformed_node;
    transform(root_group, group_transformed_node);

    for (auto & [group, sub_query_plans] : group_transformed_node)
    {
        for (auto & sub_query_plan : sub_query_plans)
        {
            addPlanNodeToGroup(sub_query_plan.getRoot(), *group);
        }
    }
}

void Memo::transform(Group * group, std::unordered_map<Group *, std::vector<SubQueryPlan>> & group_transformed_node)
{
    const auto & group_nodes = group->getGroupNodes();

    for (const auto & group_node : group_nodes)
    {
        const auto & step = group_node.getStep();
        const auto & sub_query_plans = QueryPlanOptimizations::trySplitAggregation(step);

        if (!sub_query_plans.empty())
            group_transformed_node.emplace(group, sub_query_plans);

        for (auto * child_group : group_node.getChildren())
        {
            transform(child_group, group_transformed_node);
        }
    }
}

void Memo::enforce()
{
    enforce(root_group);
}

void Memo::enforce(Group * group)
{
    const auto & group_nodes = group->getGroupNodes();

    for (const auto & group_node : group_nodes)
    {
        const auto & step = group_node.getStep();

        for (auto * child_group : group_node.getChildren())
        {
            enforce(child_group);
        }
    }
}


void Memo::derivationProperties()
{
    derivationProperties(root_group);
}

void Memo::derivationProperties(Group * group)
{
    const auto & group_nodes = group->getGroupNodes();

    for (const auto & group_node : group_nodes)
    {
        const auto & step = group_node.getStep();
        auto properties = derivationProperties(step);

        for (auto * child_group : group_node.getChildren())
        {
            derivationProperties(child_group);
        }
    }
}

}
