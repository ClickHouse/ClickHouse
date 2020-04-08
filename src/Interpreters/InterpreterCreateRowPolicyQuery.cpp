#include <Interpreters/InterpreterCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTRoleList.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <boost/range/algorithm/sort.hpp>


namespace DB
{
BlockIO InterpreterCreateRowPolicyQuery::execute()
{
    context.checkRowPolicyManagementIsAllowed();
    const auto & query = query_ptr->as<const ASTCreateRowPolicyQuery &>();
    auto & access_control = context.getAccessControlManager();

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_policy = typeid_cast<std::shared_ptr<RowPolicy>>(entity->clone());
            updateRowPolicyFromQuery(*updated_policy, query);
            return updated_policy;
        };
        String full_name = query.name_parts.getFullName(context);
        if (query.if_exists)
        {
            if (auto id = access_control.find<RowPolicy>(full_name))
                access_control.tryUpdate(*id, update_func);
        }
        else
            access_control.update(access_control.getID<RowPolicy>(full_name), update_func);
    }
    else
    {
        auto new_policy = std::make_shared<RowPolicy>();
        updateRowPolicyFromQuery(*new_policy, query);

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
    if (query.alter)
    {
        if (!query.new_policy_name.empty())
            policy.setName(query.new_policy_name);
    }
    else
    {
        policy.setDatabase(query.name_parts.database.empty() ? context.getCurrentDatabase() : query.name_parts.database);
        policy.setTableName(query.name_parts.table_name);
        policy.setName(query.name_parts.policy_name);
    }

    if (query.is_restrictive)
        policy.setRestrictive(*query.is_restrictive);

    for (const auto & [index, condition] : query.conditions)
        policy.conditions[index] = condition ? serializeAST(*condition) : String{};

    if (query.roles)
    {
        const auto & query_roles = *query.roles;

        /// We keep `roles` sorted.
        policy.roles = query_roles.roles;
        if (query_roles.current_user)
            policy.roles.push_back(context.getClientInfo().current_user);
        boost::range::sort(policy.roles);
        policy.roles.erase(std::unique(policy.roles.begin(), policy.roles.end()), policy.roles.end());

        policy.all_roles = query_roles.all_roles;

        /// We keep `except_roles` sorted.
        policy.except_roles = query_roles.except_roles;
        if (query_roles.except_current_user)
            policy.except_roles.push_back(context.getClientInfo().current_user);
        boost::range::sort(policy.except_roles);
        policy.except_roles.erase(std::unique(policy.except_roles.begin(), policy.except_roles.end()), policy.except_roles.end());
    }
}
}
