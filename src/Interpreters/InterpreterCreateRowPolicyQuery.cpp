#include <Interpreters/InterpreterCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Access/AccessControlManager.h>
#include <Access/AccessFlags.h>
#include <boost/range/algorithm/sort.hpp>


namespace DB
{
namespace
{
    void updateRowPolicyFromQueryImpl(
        RowPolicy & policy,
        const ASTCreateRowPolicyQuery & query,
        const std::optional<ExtendedRoleSet> & roles_from_query = {})
    {
        if (query.alter)
        {
            if (!query.new_short_name.empty())
                policy.setShortName(query.new_short_name);
        }
        else
            policy.setNameParts(query.name_parts);

        if (query.is_restrictive)
            policy.setRestrictive(*query.is_restrictive);

        for (auto condition_type : ext::range(RowPolicy::MAX_CONDITION_TYPE))
        {
            const auto & condition = query.conditions[condition_type];
            if (condition)
                policy.conditions[condition_type] = *condition ? serializeAST(**condition) : String{};
        }

        const ExtendedRoleSet * roles = nullptr;
        std::optional<ExtendedRoleSet> temp_role_set;
        if (roles_from_query)
            roles = &*roles_from_query;
        else if (query.roles)
            roles = &temp_role_set.emplace(*query.roles);

        if (roles)
            policy.to_roles = *roles;
    }
}


BlockIO InterpreterCreateRowPolicyQuery::execute()
{
    auto & query = query_ptr->as<ASTCreateRowPolicyQuery &>();
    auto & access_control = context.getAccessControlManager();
    context.checkAccess(query.alter ? AccessType::ALTER_ROW_POLICY : AccessType::CREATE_ROW_POLICY);

    if (!query.cluster.empty())
    {
        query.replaceCurrentUserTagWithName(context.getUserName());
        return executeDDLQueryOnCluster(query_ptr, context);
    }

    std::optional<ExtendedRoleSet> roles_from_query;
    if (query.roles)
        roles_from_query = ExtendedRoleSet{*query.roles, access_control, context.getUserID()};

    if (query.name_parts.database.empty())
        query.name_parts.database = context.getCurrentDatabase();

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_policy = typeid_cast<std::shared_ptr<RowPolicy>>(entity->clone());
            updateRowPolicyFromQueryImpl(*updated_policy, query, roles_from_query);
            return updated_policy;
        };
        if (query.if_exists)
        {
            if (auto id = access_control.find<RowPolicy>(query.name_parts.getName()))
                access_control.tryUpdate(*id, update_func);
        }
        else
            access_control.update(access_control.getID<RowPolicy>(query.name_parts.getName()), update_func);
    }
    else
    {
        auto new_policy = std::make_shared<RowPolicy>();
        updateRowPolicyFromQueryImpl(*new_policy, query, roles_from_query);

        if (query.if_not_exists)
            access_control.tryInsert(new_policy);
        else if (query.or_replace)
            access_control.insertOrReplace(new_policy);
        else
            access_control.insert(new_policy);
    }

    return {};
}


void InterpreterCreateRowPolicyQuery::updateRowPolicyFromQuery(RowPolicy & policy, const ASTCreateRowPolicyQuery & query)
{
    updateRowPolicyFromQueryImpl(policy, query);
}

}
