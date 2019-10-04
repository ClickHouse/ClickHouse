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

    std::vector<AttributesPtr> roles;
    roles.reserve(query.role_names.size());
    for (const String & role_name : query.role_names)
    {
        auto role = std::make_shared<Role>();
        role->name = role_name;
        roles.emplace_back(std::move(role));
    }

    if (query.if_not_exists)
        context.getAccessControlManager().tryInsert(roles);
    else
        context.getAccessControlManager().insert(roles);

    return {};
}
}
