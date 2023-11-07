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

    /// root group must required singleton
    auto & group_nodes = root_group->getGroupNodes();
    for (auto & group_node : group_nodes)
    {
        std::vector<PhysicalProperties> required_child_prop;
        for (size_t j = 0; j < group_node->getChildren().size(); ++j)
            required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
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

StepTree Memo::extractPlan()
{
    StepTree sub_plan
        = extractPlan(*root_group, PhysicalProperties{.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});

    WriteBufferFromOwnString buffer;
    StepTree::ExplainPlanOptions settings;
    sub_plan.explainPlan(buffer, settings);

    LOG_TRACE(log, "CBO optimizer find best plan: {}", buffer.str());

    return sub_plan;
}

StepTree Memo::extractPlan(Group & group, const PhysicalProperties & required_prop)
{
    const auto & prop_group_node = group.getSatisfyBestGroupNode(required_prop);
    chassert(prop_group_node.has_value());

    auto & group_node = *prop_group_node->second.group_node;
    LOG_DEBUG(
        log, "Best node: group id {}, {}, required_prop {}", group.getId(), group_node.getStep()->getName(), required_prop.toString());

    auto child_prop = group_node.getChildrenProp(prop_group_node->first);

    std::vector<StepTreePtr> child_plans;
    const auto & children_group = group_node.getChildren();

    for (size_t i = 0; i < child_prop.size(); ++i)
    {
        StepTreePtr plan_ptr;
        if (group_node.isEnforceNode())
            plan_ptr = std::make_unique<StepTree>(extractPlan(group, child_prop[i]));
        else
            plan_ptr = std::make_unique<StepTree>(extractPlan(*children_group[i], child_prop[i]));
        child_plans.emplace_back(std::move(plan_ptr));
    }

    StepTree plan;
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
