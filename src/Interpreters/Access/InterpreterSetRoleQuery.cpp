#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterSetRoleQuery.h>
#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/AccessControl.h>
#include <Access/User.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SET_NON_GRANTED_ROLE;
}


BlockIO InterpreterSetRoleQuery::execute()
{
    const auto & query = query_ptr->as<const ASTSetRoleQuery &>();
    if (query.kind == ASTSetRoleQuery::Kind::SET_DEFAULT_ROLE)
        setDefaultRole(query);
    else
        setRole(query);
    return {};
}


void InterpreterSetRoleQuery::setRole(const ASTSetRoleQuery & query)
{
    auto session_context = getContext()->getSessionContext();

    if (query.kind == ASTSetRoleQuery::Kind::SET_ROLE_DEFAULT)
        session_context->setCurrentRolesDefault();
    else
        session_context->setCurrentRoles(RolesOrUsersSet{*query.roles, session_context->getAccessControl()});
}


void InterpreterSetRoleQuery::setDefaultRole(const ASTSetRoleQuery & query)
{
    getContext()->checkAccess(AccessType::ALTER_USER);

    auto & access_control = getContext()->getAccessControl();
    std::vector<UUID> to_users = RolesOrUsersSet{*query.to_users, access_control, getContext()->getUserID()}.getMatchingIDs(access_control);
    RolesOrUsersSet roles_from_query{*query.roles, access_control};

    auto update_func = [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
    {
        auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
        updateUserSetDefaultRoles(*updated_user, roles_from_query);
        return updated_user;
    };

    access_control.update(to_users, update_func);
}


void InterpreterSetRoleQuery::updateUserSetDefaultRoles(User & user, const RolesOrUsersSet & roles_from_query)
{
    if (!roles_from_query.all)
    {
        for (const auto & id : roles_from_query.getMatchingIDs())
        {
            if (!user.granted_roles.isGranted(id))
                throw Exception(ErrorCodes::SET_NON_GRANTED_ROLE, "Role should be granted to set default");
        }
    }
    user.default_roles = roles_from_query;
}

void registerInterpreterSetRoleQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterSetRoleQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterSetRoleQuery", create_fn);
}

}
