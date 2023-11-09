#include <QueryCoordination/Optimizer/Memo.h>

#include <stack>
#include <QueryCoordination/Optimizer/DeriveOutputProp.h>
#include <QueryCoordination/Optimizer/GroupStep.h>
#include <Common/typeid_cast.h>


namespace DB
{

Memo::Memo(QueryPlan && plan, ContextPtr context_) : context(context_)
{
    root_group = &buildGroup(*plan.getRootNode());

    /// Required distribution of children of root is always singleton
    auto & group_nodes = root_group->getGroupNodes();
    for (auto & group_node : group_nodes)
    {
        std::vector<PhysicalProperties> required_child_prop;
        for (size_t j = 0; j < group_node->getChildren().size(); ++j)
            required_child_prop.push_back({.distribution = {.type = Distribution::Singleton}});
        group_node->addRequiredChildrenProp(required_child_prop);
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

    GroupNodePtr group_node = std::make_shared<GroupNode>(node.step, child_groups);
    group.addGroupNode(group_node, ++group_node_id_counter);
    all_group_nodes.insert(group_node);

    return group;
}

GroupNodePtr Memo::addPlanNodeToGroup(const QueryPlan::Node & node, Group * target_group)
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
            GroupNodePtr added_node = addPlanNodeToGroup(*child, nullptr);
            child_groups.emplace_back(&added_node->getGroup());
        }
    }

    GroupNodePtr group_node = std::make_shared<GroupNode>(node.step, child_groups);
    /// group_node duplicate detection
    auto it = all_group_nodes.find(group_node);
    if (it != all_group_nodes.end())
        return *it;

    if (target_group)
    {
        target_group->addGroupNode(group_node, ++group_node_id_counter);
    }
    else
    {
        groups.emplace_back(Group(++group_id_counter));
        auto & group = groups.back();
        group.addGroupNode(group_node, ++group_node_id_counter);
    }

    all_group_nodes.insert(group_node);
    return group_node;
}

void Memo::dump()
{
    for (auto & group : groups)
        LOG_DEBUG(log, "Group: {}", group.toString());
}

Group & Memo::rootGroup()
{
    return *root_group;
}

QueryPlan Memo::extractPlan()
{
    /// The distribution of root node is always singleton.
    PhysicalProperties required_pro{.distribution = {.type = Distribution::Singleton}};
    SubQueryPlan plan = extractPlan(*root_group, required_pro);

    WriteBufferFromOwnString buffer;
    SubQueryPlan::ExplainPlanOptions settings;
    plan.explainPlan(buffer, settings);

    LOG_TRACE(log, "Found best plan: {}", buffer.str());
    return plan;
}

SubQueryPlan Memo::extractPlan(Group & group, const PhysicalProperties & required_prop)
{
    const auto & prop_group_node = group.getSatisfiedBestGroupNode(required_prop);
    if (!prop_group_node.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No best node for group {}", group.getId());

    auto & group_node = *prop_group_node->second.group_node;
    LOG_TRACE(
        log,
        "Found best node {} for group {} required property {}",
        group_node.getStep()->getName(),
        group.getId(),
        required_prop.toString());

    auto child_prop = group_node.getChildrenProp(prop_group_node->first);

    std::vector<SubQueryPlanPtr> child_plans;
    const auto & children_group = group_node.getChildren();

    for (size_t i = 0; i < child_prop.size(); ++i)
    {
        SubQueryPlanPtr plan_ptr;
        if (group_node.isEnforceNode())
            plan_ptr = std::make_unique<SubQueryPlan>(extractPlan(group, child_prop[i]));
        else
            plan_ptr = std::make_unique<SubQueryPlan>(extractPlan(*children_group[i], child_prop[i]));
        child_plans.emplace_back(std::move(plan_ptr));
    }

    SubQueryPlan plan;
    if (child_plans.size() > 1)
    {
        plan.unitePlans(group_node.getStep(), child_plans);
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

    auto * root = plan.getRootNode();
    root->cost = prop_group_node->second.cost;
    root->statistics = group.getStatistics().clone();

    return plan;
}

}
