#include <Interpreters/Access/InterpreterCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/formatAST.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/RowPolicy.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <boost/range/algorithm/sort.hpp>


namespace DB
{
namespace
{
    void updateRowPolicyFromQueryImpl(
        RowPolicy & policy,
        const ASTCreateRowPolicyQuery & query,
        const RowPolicyName & override_name,
        const std::optional<RolesOrUsersSet> & override_to_set,
        const std::optional<RolesOrUsersSet> & override_of_set)
    {
        if (!override_name.empty())
            policy.setFullName(override_name);
        else if (!query.new_short_name.empty())
            policy.setShortName(query.new_short_name);
        else if (query.names->full_names.size() == 1)
            policy.setFullName(query.names->full_names.front());

        auto old_kind = policy.getKind();
        if (query.kind)
            policy.setKind(*query.kind);
        bool kind_changed = (policy.getKind() != old_kind) || !query.alter;

        for (const auto & [filter_type, filter] : query.filters)
            policy.filters[static_cast<size_t>(filter_type)] = filter ? serializeAST(*filter) : String{};

        if (override_to_set)
            policy.to_set = *override_to_set;
        else if (query.to_set)
            policy.to_set = *query.to_set;

        if (override_of_set)
            policy.of_set = *override_of_set;
        else if (query.of_set)
            policy.of_set = *query.of_set;
        else if ((policy.getKind() == RowPolicyKind::PERMISSIVE) && (override_to_set || query.to_set || kind_changed))
            policy.of_set = RolesOrUsersSet::AllTag{}; /// By default permissive row policies are OF ALL.
        else if (policy.getKind() == RowPolicyKind::RESTRICTIVE)
            policy.of_set.clear();

        if ((policy.getKind() == RowPolicyKind::PERMISSIVE) && !policy.of_set.contains(policy.to_set))
            throw Exception("Users and roles in the TO clause must be a subset of ones in the OF clause", ErrorCodes::BAD_ARGUMENTS);

        if ((policy.getKind() == RowPolicyKind::RESTRICTIVE) && !policy.of_set.empty())
            throw Exception("OF clause can only be used with permissive row policies", ErrorCodes::BAD_ARGUMENTS);
    }
}


BlockIO InterpreterCreateRowPolicyQuery::execute()
{
    auto & query = query_ptr->as<ASTCreateRowPolicyQuery &>();
    auto required_access = getRequiredAccess();

    if (!query.cluster.empty())
    {
        query.replaceCurrentUserTag(getContext()->getUserName());
        return executeDDLQueryOnCluster(query_ptr, getContext(), required_access);
    }

    assert(query.names->cluster.empty());
    auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(required_access);

    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

    std::optional<RolesOrUsersSet> to_set_from_query, of_set_from_query;
    if (query.to_set)
        to_set_from_query = RolesOrUsersSet{*query.to_set, access_control, getContext()->getUserID()};
    if (query.of_set)
        of_set_from_query = RolesOrUsersSet{*query.of_set, access_control, getContext()->getUserID()};

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_policy = typeid_cast<std::shared_ptr<RowPolicy>>(entity->clone());
            updateRowPolicyFromQueryImpl(*updated_policy, query, {}, to_set_from_query, of_set_from_query);
            return updated_policy;
        };
        Strings names = query.names->toStrings();
        if (query.if_exists)
        {
            auto ids = access_control.find<RowPolicy>(names);
            access_control.tryUpdate(ids, update_func);
        }
        else
            access_control.update(access_control.getIDs<RowPolicy>(names), update_func);
    }
    else
    {
        std::vector<AccessEntityPtr> new_policies;
        for (const auto & full_name : query.names->full_names)
        {
            auto new_policy = std::make_shared<RowPolicy>();
            updateRowPolicyFromQueryImpl(*new_policy, query, full_name, to_set_from_query, of_set_from_query);
            new_policies.emplace_back(std::move(new_policy));
        }

        if (query.if_not_exists)
            access_control.tryInsert(new_policies);
        else if (query.or_replace)
            access_control.insertOrReplace(new_policies);
        else
            access_control.insert(new_policies);
    }

    return {};
}


void InterpreterCreateRowPolicyQuery::updateRowPolicyFromQuery(RowPolicy & policy, const ASTCreateRowPolicyQuery & query)
{
    updateRowPolicyFromQueryImpl(policy, query, {}, {}, {});
}


AccessRightsElements InterpreterCreateRowPolicyQuery::getRequiredAccess() const
{
    const auto & query = query_ptr->as<const ASTCreateRowPolicyQuery &>();
    AccessRightsElements res;
    auto access_type = (query.alter ? AccessType::ALTER_ROW_POLICY : AccessType::CREATE_ROW_POLICY);
    for (const auto & row_policy_name : query.names->full_names)
        res.emplace_back(access_type, row_policy_name.database, row_policy_name.table_name);
    return res;
}

}
