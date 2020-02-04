#include <Interpreters/InterpreterGrantQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTRoleList.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/AccessRightsContext.h>
#include <Access/User.h>


namespace DB
{
BlockIO InterpreterGrantQuery::execute()
{
    const auto & query = query_ptr->as<const ASTGrantQuery &>();
    auto & access_control = context.getAccessControlManager();
    context.getAccessRights()->checkGrantOption(query.access_rights_elements);

    using Kind = ASTGrantQuery::Kind;

    if (query.to_roles->all_roles)
        throw Exception(
            "Cannot " + String((query.kind == Kind::GRANT) ? "GRANT to" : "REVOKE from") + " ALL", ErrorCodes::NOT_IMPLEMENTED);

    String current_database = context.getCurrentDatabase();

    auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
    {
        auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
        if (query.kind == Kind::GRANT)
        {
            updated_user->access.grant(query.access_rights_elements, current_database);
            if (query.grant_option)
                updated_user->access_with_grant_option.grant(query.access_rights_elements, current_database);
        }
        else
        {
            updated_user->access_with_grant_option.revoke(query.access_rights_elements, current_database);
            if (!query.grant_option)
                updated_user->access.revoke(query.access_rights_elements, current_database);
        }
        return updated_user;
    };

    std::vector<UUID> ids = access_control.getIDs<User>(query.to_roles->roles);
    if (query.to_roles->current_user)
        ids.push_back(context.getUserID());
    access_control.update(ids, update_func);
    return {};
}

}
