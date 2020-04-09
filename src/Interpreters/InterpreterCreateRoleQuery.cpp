#include <Interpreters/InterpreterCreateRoleQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/Role.h>


namespace DB
{
BlockIO InterpreterCreateRoleQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateRoleQuery &>();
    auto & access_control = context.getAccessControlManager();
    if (query.alter)
        context.checkAccess(AccessType::CREATE_ROLE | AccessType::DROP_ROLE);
    else
        context.checkAccess(AccessType::CREATE_ROLE);

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_role = typeid_cast<std::shared_ptr<Role>>(entity->clone());
            updateRoleFromQuery(*updated_role, query);
            return updated_role;
        };
        if (query.if_exists)
        {
            if (auto id = access_control.find<Role>(query.name))
                access_control.tryUpdate(*id, update_func);
        }
        else
            access_control.update(access_control.getID<Role>(query.name), update_func);
    }
    else
    {
        auto new_role = std::make_shared<Role>();
        updateRoleFromQuery(*new_role, query);

        if (query.if_not_exists)
            access_control.tryInsert(new_role);
        else if (query.or_replace)
            access_control.insertOrReplace(new_role);
        else
            access_control.insert(new_role);
    }

    return {};
}


void InterpreterCreateRoleQuery::updateRoleFromQuery(Role & role, const ASTCreateRoleQuery & query)
{
    if (query.alter)
    {
        if (!query.new_name.empty())
            role.setName(query.new_name);
    }
    else
        role.setName(query.name);
}
}
