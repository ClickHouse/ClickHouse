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
    ++context.memo_counters.groups_created;
    LOG_TEST(log, "Add group '{}' -> id {}", group_expression->getName(), group_id);
    return group_id;
}

bool Memo::participatesInLogicalIndex(const GroupExpression & group_expression) const
{
    return context.cascades_memo_deduplication
        && group_expression.strategy == nullptr
        && group_expression.enforced_property == EnforcedProperty::None
        && group_expression.plan_step
        && group_expression.plan_step->hasLogicalDigest();
}

GroupId Memo::findInternedGroup(UInt128 fingerprint, const GroupExpression & group_expression) const
{
    auto bucket = logical_expression_index.find(fingerprint);
    if (bucket == logical_expression_index.end())
        return INVALID_GROUP_ID;

    for (const auto & entry : bucket->second)
    {
        /// `entry.insertion_time_fingerprint` equals `fingerprint` here by construction - it is the
        /// bucket key. The field is scaffolding for a future removal or relocation path, which needs
        /// the key an entry was filed under without recomputing a digest that may have moved.
        /// Compares the frame field by field and the two live steps' logical digests byte for byte.
        if (entry.expression->logicallyEqualTo(group_expression))
            return entry.group_id;
    }
    return INVALID_GROUP_ID;
}

GroupId Memo::internExpression(GroupExpressionPtr group_expression)
{
    chassert(group_expression->strategy == nullptr && group_expression->enforced_property == EnforcedProperty::None);

    if (!participatesInLogicalIndex(*group_expression))
        return addGroup(std::move(group_expression));

    const UInt128 fingerprint = group_expression->logicalFingerprint();
    const GroupId existing_group_id = findInternedGroup(fingerprint, *group_expression);

    if (existing_group_id != INVALID_GROUP_ID)
    {
        /// The inputs are final by contract, so a cycle formed here would be permanent.
        chassert(!isGroupReachableFromInputs(*group_expression, existing_group_id));
        ++context.memo_counters.groups_reused;
        LOG_TEST(log, "Interned '{}' into existing group #{}", group_expression->getName(), existing_group_id);
        /// A knob variant survives here as a costed alternative; only a fully-equal expression is
        /// dropped. No detection probe: the match that brought us here is in this very group.
        getGroup(existing_group_id)->addLogicalExpression(std::move(group_expression));
        return existing_group_id;
    }

    const GroupId group_id = addGroup(group_expression);
    logical_expression_index[fingerprint].push_back({fingerprint, group_id, group_expression.get()});
    return group_id;
}

bool Memo::addLogicalExpressionToGroup(GroupId group_id, GroupExpressionPtr group_expression)
{
    auto * inserted = group_expression.get();
    if (!getGroup(group_id)->addLogicalExpression(std::move(group_expression)))
        return false;

    detectDuplicateGroup(*inserted, group_id);
    return true;
}

void Memo::detectDuplicateGroup(const GroupExpression & group_expression, GroupId group_id)
{
    if (!participatesInLogicalIndex(group_expression))
        return;

    const GroupId other_group_id = findInternedGroup(group_expression.logicalFingerprint(), group_expression);
    if (other_group_id == INVALID_GROUP_ID || other_group_id == group_id)
        return;

    ++context.memo_counters.duplicate_group_detections;
    LOG_DEBUG(log, "Groups #{} and #{} are logically equal: '{}', inserted into #{}, computes the relation of #{}. "
        "Not merged - group merging is a later stage.",
        group_id, other_group_id, group_expression.getName(), group_id, other_group_id);
}

bool Memo::isGroupReachableFromInputs(const GroupExpression & group_expression, GroupId target) const
{
    std::vector<GroupId> to_visit;
    std::unordered_set<GroupId> visited;
    for (const auto & input : group_expression.inputs)
        to_visit.push_back(input.group_id);

    while (!to_visit.empty())
    {
        const GroupId group_id = to_visit.back();
        to_visit.pop_back();
        if (group_id == target)
            return true;
        if (group_id >= groups_by_id.size() || !visited.insert(group_id).second)
            continue;

        for (const auto & member : groups_by_id[group_id]->logical_expressions)
            for (const auto & input : member->inputs)
                to_visit.push_back(input.group_id);
    }

    return false;
}

size_t Memo::countGroupsUnreachableFrom(GroupId root_group_id) const
{
    if (root_group_id >= groups_by_id.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No group #{} in the memo ({} groups)", root_group_id, groups_by_id.size());

    /// Both expression lists: a group can be consumed by an implementation of its parent alone.
    std::vector<GroupId> to_visit{root_group_id};
    std::unordered_set<GroupId> visited{root_group_id};
    while (!to_visit.empty())
    {
        const GroupId group_id = to_visit.back();
        to_visit.pop_back();

        const auto & group = groups_by_id[group_id];
        for (const auto * expressions : {&group->logical_expressions, &group->physical_expressions})
            for (const auto & member : *expressions)
                for (const auto & input : member->inputs)
                    if (input.group_id < groups_by_id.size() && visited.insert(input.group_id).second)
                        to_visit.push_back(input.group_id);
    }

    return groups_by_id.size() - visited.size();
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
