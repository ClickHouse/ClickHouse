#include <Interpreters/InterpreterCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTGenericRoleSet.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/AccessFlags.h>
#include <boost/range/algorithm/sort.hpp>


namespace DB
{
BlockIO InterpreterCreateRowPolicyQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateRowPolicyQuery &>();
    auto & access_control = context.getAccessControlManager();
    context.checkAccess(query.alter ? AccessType::ALTER_POLICY : AccessType::CREATE_POLICY);

    std::optional<GenericRoleSet> roles_from_query;
    if (query.roles)
        roles_from_query = GenericRoleSet{*query.roles, access_control, context.getUserID()};

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_policy = typeid_cast<std::shared_ptr<RowPolicy>>(entity->clone());
            updateRowPolicyFromQuery(*updated_policy, query, roles_from_query);
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
        updateRowPolicyFromQuery(*new_policy, query, roles_from_query);

        if (query.if_not_exists)
            access_control.tryInsert(new_policy);
        else if (query.or_replace)
            access_control.insertOrReplace(new_policy);
        else
            access_control.insert(new_policy);
    }

    return {};
}


void InterpreterCreateRowPolicyQuery::updateRowPolicyFromQuery(RowPolicy & policy, const ASTCreateRowPolicyQuery & query, const std::optional<GenericRoleSet> & roles_from_query)
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

    if (roles_from_query)
        policy.roles = *roles_from_query;
}
}
