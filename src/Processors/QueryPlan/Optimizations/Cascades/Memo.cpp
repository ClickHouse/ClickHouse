#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

GroupId Memo::addGroup(GroupExpressionPtr group_expression)
{
    auto group_id = groups_by_id.size();
    GroupPtr new_group = std::make_shared<Group>(group_id);
    group_expression->group_id = group_id;
    new_group->addLogicalExpression(group_expression);
    groups_by_id.push_back(new_group);
    LOG_TEST(log, "Add group '{}' -> id {}", group_expression->getName(), group_id);
    return group_id;
}

GroupPtr Memo::getGroup(GroupId group_id)
{
    if (group_id >= groups_by_id.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No group #{} in the memo ({} groups)", group_id, groups_by_id.size());
    return groups_by_id[group_id];
}

GroupConstPtr Memo::getGroup(GroupId group_id) const
{
    if (group_id >= groups_by_id.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No group #{} in the memo ({} groups)", group_id, groups_by_id.size());
    return groups_by_id[group_id];
}

void Memo::dump(WriteBuffer & out) const
{
    for (GroupId group_id = 0; group_id < groups_by_id.size(); ++group_id)
    {
        const auto & group = groups_by_id.at(group_id);
        out << "Group #" << group_id << "\n";
        group->dump(out, context.cost_config, "    ");
        out << "\n";
    }
}

String Memo::dump() const
{
    WriteBufferFromOwnString out;
    dump(out);
    return out.str();
}

}
