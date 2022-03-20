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
extern const UInt64 RBAC_VERSION_ROW_POLICIES_ARE_SIMPLE_BY_DEFAULT;

namespace
{
    void updateRowPolicyFromQueryImpl(
        RowPolicy & policy,
        const ASTCreateRowPolicyQuery & query,
        UInt64 rbac_version,
        const RowPolicyName & override_name,
        const std::optional<RolesOrUsersSet> & override_to_roles)
    {
        if (!override_name.empty())
            policy.setFullName(override_name);
        else if (!query.new_short_name.empty())
            policy.setShortName(query.new_short_name);
        else if (query.names->full_names.size() == 1)
            policy.setFullName(query.names->full_names.front());

        std::optional<RowPolicyKind> new_kind = query.kind;
        if (!query.alter && !new_kind)
        {
            if (rbac_version < RBAC_VERSION_ROW_POLICIES_ARE_SIMPLE_BY_DEFAULT)
                new_kind = RowPolicyKind::PERMISSIVE;
            else
                new_kind = RowPolicyKind::SIMPLE;
        }

        if (new_kind)
            policy.setKind(*new_kind);

        for (const auto & [filter_type, filter] : query.filters)
            policy.filters[static_cast<size_t>(filter_type)] = filter ? serializeAST(*filter) : String{};

        if (override_to_roles)
            policy.to_roles = *override_to_roles;
        else if (query.to_roles)
            policy.to_roles = *query.to_roles;
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
    UInt64 rbac_version = getContext()->getSettingsRef().rbac_version;
    getContext()->checkAccess(required_access);

    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

    std::optional<RolesOrUsersSet> roles_from_query;
    if (query.to_roles)
        roles_from_query = RolesOrUsersSet{*query.to_roles, access_control, getContext()->getUserID()};

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_policy = typeid_cast<std::shared_ptr<RowPolicy>>(entity->clone());
            updateRowPolicyFromQueryImpl(*updated_policy, query, rbac_version, {}, roles_from_query);
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
            updateRowPolicyFromQueryImpl(*new_policy, query, rbac_version, full_name, roles_from_query);
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


void InterpreterCreateRowPolicyQuery::updateRowPolicyFromQuery(RowPolicy & policy, const ASTCreateRowPolicyQuery & query, UInt64 rbac_version)
{
    updateRowPolicyFromQueryImpl(policy, query, rbac_version, {}, {});
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
