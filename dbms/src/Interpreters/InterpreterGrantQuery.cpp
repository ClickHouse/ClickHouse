#include <Interpreters/InterpreterGrantQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/AccessRightsContext.h>
#include <Access/GenericRoleSet.h>
#include <Access/User.h>


namespace DB
{
BlockIO InterpreterGrantQuery::execute()
{
    const auto & query = query_ptr->as<const ASTGrantQuery &>();
    auto & access_control = context.getAccessControlManager();
    context.getAccessRights()->checkGrantOption(query.access_rights_elements);

    using Kind = ASTGrantQuery::Kind;
    std::vector<UUID> to_roles = GenericRoleSet{*query.to_roles, access_control, context.getUserID()}.getMatchingUsers(access_control);
    String current_database = context.getCurrentDatabase();
    using Kind = ASTGrantQuery::Kind;

    auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
    {
        auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
        if (query.kind == Kind::GRANT)
        {
            updated_user->access.grant(query.access_rights_elements, current_database);
            if (query.grant_option)
                updated_user->access_with_grant_option.grant(query.access_rights_elements, current_database);
        }
        else if (context.getSettingsRef().partial_revokes)
        {
            updated_user->access_with_grant_option.partialRevoke(query.access_rights_elements, current_database);
            if (!query.grant_option)
                updated_user->access.partialRevoke(query.access_rights_elements, current_database);
        }
        else
        {
            updated_user->access_with_grant_option.revoke(query.access_rights_elements, current_database);
            if (!query.grant_option)
                updated_user->access.revoke(query.access_rights_elements, current_database);
        }
        return updated_user;
    };

    access_control.update(to_roles, update_func);

    return {};
}

}
