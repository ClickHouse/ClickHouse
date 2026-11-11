#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace ProfileEvents
{
    extern const Event JoinEnumerationRuleExpansions;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Adds the joins of a join graph that the import left out (setting `cascades_join_enumeration = 'all_splits_rule'`):
/// the other splits of the sets of relations that have a group, and the sets that do not. It matches a join of the
/// graph's group of all relations and runs once per graph, while the search explores the graph, so every group gets
/// all its joins before any group of the graph is implemented.
class JoinEnumeration : public IOptimizationRule
{
public:
    String getName() const override { return "JoinEnumeration"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 3000; }
    bool isTransformation() const override { return true; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const override;
};

bool JoinEnumeration::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & memo) const
{
    if (!typeid_cast<const JoinStepLogical *>(expression->getQueryPlanStep()))
        return false;
    auto expansion = memo.getJoinGraphExpansion(expression->group_id);
    return expansion && !expansion->expanded;
}

std::vector<GroupExpressionPtr> JoinEnumeration::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    /// An application queued before the graph was expanded runs without a second pattern check.
    auto expansion = memo.getJoinGraphExpansion(expression->group_id);
    if (!expansion || expansion->expanded)
        return {};
    expansion->expanded = true;
    ProfileEvents::increment(ProfileEvents::JoinEnumerationRuleExpansions);

    auto set_plan = expansion->order_builder->buildRemainingSets();
    if (!set_plan)
        return {};

    std::vector<GroupExpressionPtr> result;
    size_t new_groups = 0;
    for (auto & set : set_plan->sets)
    {
        std::optional<GroupId> group_id;
        if (auto it = expansion->set_groups.find(set.relations); it != expansion->set_groups.end())
        {
            group_id = it->second;
            /// A new join in a group that is implemented already would not be costed.
            if (memo.getGroup(*group_id)->isOptimizedForAnyProperties())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Group #{} of a join graph is implemented before the graph is expanded", *group_id);
        }

        for (auto & split : set.splits)
        {
            auto join_expression = std::make_shared<GroupExpression>(std::move(split.step));
            for (UInt64 side : {split.left, split.right})
            {
                auto it = expansion->input_pending_props.find(side);
                join_expression->inputs.push_back(
                    {expansion->set_groups.at(side), it != expansion->input_pending_props.end() ? it->second : ExpressionProperties{}});
            }

            if (!group_id)
            {
                group_id = memo.addGroup(join_expression);
                ++new_groups;
                result.push_back(std::move(join_expression));
            }
            else if (memo.getGroup(*group_id)->addLogicalExpression(join_expression))
            {
                result.push_back(std::move(join_expression));
            }
        }
        expansion->set_groups[set.relations] = *group_id;
    }

    LOG_DEBUG(getLogger("JoinEnumeration"), "Added {} joins and {} groups to the join graph of group #{}",
        result.size(), new_groups, expression->group_id);
    return result;
}

OptimizationRulePtr createJoinEnumeration() { return std::make_shared<JoinEnumeration>(); }

}
