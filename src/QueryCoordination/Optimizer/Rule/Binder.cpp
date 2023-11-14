#include <QueryCoordination/Optimizer/Group.h>
#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/GroupStep.h>
#include <QueryCoordination/Optimizer/Rule/Binder.h>
#include <QueryCoordination/Optimizer/Rule/Pattern.h>


namespace DB
{

Binder::Binder(const Pattern & pattern_, GroupNodePtr group_node_) : pattern(pattern_), group_node(group_node_)
{
}

std::vector<SubQueryPlanPtr> Binder::bind()
{
    return extractGroupNode(pattern, group_node);
}

void generateAllCases(
    GroupNodePtr group_node_,
    size_t children_index,
    std::vector<std::vector<SubQueryPlanPtr>> & children_candidate_lists,
    std::vector<SubQueryPlanPtr> & children_select_list,
    std::vector<SubQueryPlanPtr> & results)
{
    if (children_index >= children_candidate_lists.size())
    {
        SubQueryPlanPtr result = std::make_shared<SubQueryPlan>();
        result->unitePlans(group_node_->getStep(), children_select_list);
        children_select_list.resize(children_select_list.size() - 1);
        results.emplace_back(result);
        return;
    }
    for (size_t i = 0; i < children_candidate_lists[children_index].size(); ++i)
    {
        children_select_list.emplace_back(children_candidate_lists[children_index][i]);
        generateAllCases(group_node_, children_index + 1, children_candidate_lists, children_select_list, results);
    }
}

std::vector<SubQueryPlanPtr>
generateSubQueryPlanWithChildren(GroupNodePtr group_node_, std::vector<std::vector<SubQueryPlanPtr>> & children_candidate_lists)
{
    std::vector<SubQueryPlanPtr> results;
    for (auto & candidate_list : children_candidate_lists)
    {
        if (candidate_list.empty())
        {
            // Every child should have at least one candidate
            return results;
        }
    }

    std::vector<SubQueryPlanPtr> children_select_list;
    generateAllCases(group_node_, 0, children_candidate_lists, children_select_list, results);
    return results;
}

std::vector<SubQueryPlanPtr> Binder::extractGroupNode(const Pattern & pattern_, GroupNodePtr group_node_)
{
    if (pattern_.getStepType() == StepType::PatternAny)
    {
        SubQueryPlanPtr sub_plan = std::make_shared<SubQueryPlan>();
        auto child_step = std::make_shared<GroupStep>(group_node_->getStep()->getOutputStream(), group_node_->getGroup());
        sub_plan->addStep(child_step);
        return {sub_plan};
    }

    if (pattern_.getStepType() != group_node_->getStep()->stepType())
        return {};

    const auto & child_pattern = pattern_.getChildren();
    const auto & child_group = group_node_->getChildren();

    if (child_pattern.size() != child_group.size())
        return {};

    std::vector<std::vector<SubQueryPlanPtr>> children_candidate_lists;
    for (size_t i = 0; i < child_pattern.size(); ++i)
    {
        auto candidate_list = extractGroup(child_pattern[i], *child_group[i]);
        children_candidate_lists.emplace_back(std::move(candidate_list));
    }

    return generateSubQueryPlanWithChildren(group_node_, children_candidate_lists);
}

std::vector<SubQueryPlanPtr> Binder::extractGroup(const Pattern & pattern_, Group & group)
{
    std::vector<SubQueryPlanPtr> results;
    auto & group_nodes = group.getGroupNodes();
    for (auto it = group_nodes.begin(); it != group_nodes.end(); ++it)
    {
        auto result = extractGroupNode(pattern_, *it);
        for (auto & r : result)
            results.emplace_back(std::move(r));
    }
    return results;
}


//    std::vector<size_t> cursors;
//    for (auto & result : children_results)
//    {
//        if (result.empty())
//        {
//            // Every child should have at least one candidate
//            return results;
//        }
//        cursors.emplace_back(0);
//    }
//
//    while(true)
//    {
//        std::vector<SubQueryPlanPtr> children;
//        for (size_t i = 0; i < children_results.size(); ++i)
//        {
//            children.emplace_back(std::move(children_results[i][cursors[i]]));
//        }
//
//        SubQueryPlanPtr candidate = std::make_shared<SubQueryPlan>();
//        candidate->unitePlans(group_node_.getStep(), children);
//        results.emplace_back(std::move(candidate));
//
//        bool shifted = false;
//        // Shift cursor
//        for (Int32 i = static_cast<Int32>(cursors.size() - 1); i >= 0; --i)
//        {
//            if (!shifted)
//            {
//                // Shift cursor
//                cursors[i] += 1;
//                shifted = true;
//            }
//
//            if (i == 0 && cursors[0] > children_results[0].size() - 1)
//            {
//                // Candidates are exhausted
//                break; // break while true
//            }
//            else if (i > 0 && cursors[i] > children_results[i].size() - 1)
//            {
//                // Shift previous children
//                cursors[i] = 0;
//                cursors[i - 1] += 1;
//                continue;
//            }
//            else
//            {
//                break;
//            }
//        }
//    }
//    return results;

}
