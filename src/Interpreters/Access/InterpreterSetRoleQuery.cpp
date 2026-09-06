#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterSetRoleQuery.h>
#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/User.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 readonly;
}

namespace ErrorCodes
{
    extern const int READONLY;
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

    /// `readonly` is read from the query context: a client `--readonly 1` raises it there only, while a
    /// mid-session `SET readonly = 1` also reaches it because a query context inherits session settings.
    if (getContext()->getAccessControl().doesReadonlyRestrictSetRole()
        && getContext()->getSettingsRef()[Setting::readonly])
        throw Exception(
            ErrorCodes::READONLY,
            "Cannot execute SET ROLE in readonly mode. "
            "This restriction is enabled by access_control_improvements.readonly_restricts_set_role");

    if (query.kind == ASTSetRoleQuery::Kind::SET_ROLE_DEFAULT)
        session_context->setCurrentRolesDefault();
    else
        session_context->setCurrentRoles(RolesOrUsersSet{*query.roles, session_context->getAccessControl()});
}


void InterpreterSetRoleQuery::setDefaultRole(const ASTSetRoleQuery & query)
{
    getContext()->getAccess()->checkCanAdministerDefaultRoles();
    getContext()->checkAccess(query.to_users->collectRequiredGrants(AccessType::ALTER_USER));

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

void registerInterpreterSetRoleQuery(InterpreterFactory & factory);
void registerInterpreterSetRoleQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterSetRoleQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterSetRoleQuery", create_fn);
}

}
