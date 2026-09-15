#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterSetRoleQuery.h>
#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/User.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SET_NON_GRANTED_ROLE;
    extern const int BAD_ARGUMENTS;
}


BlockIO InterpreterSetRoleQuery::execute()
{
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query_ptr->as<const ASTSetRoleQuery &>();

    /// Only `SET DEFAULT ROLE` changes the user and can be distributed. `SET ROLE` and `SET ROLE DEFAULT`
    /// change the current session, so a cluster is meaningless for them - and executing one on another
    /// host, where there is no session at all, is not something to attempt. The parser only accepts the
    /// clause for `SET DEFAULT ROLE`, so this guards an AST built by other means.
    if (!query.cluster.empty() && query.kind != ASTSetRoleQuery::Kind::SET_DEFAULT_ROLE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ON CLUSTER is only supported for SET DEFAULT ROLE, because SET ROLE changes the current session");

    if (query.kind == ASTSetRoleQuery::Kind::SET_DEFAULT_ROLE)
        return setDefaultRole(updated_query_ptr, query);

    setRole(query);
    return {};
}


void InterpreterSetRoleQuery::setRole(const ASTSetRoleQuery & query)
{
    auto session_context = getContext()->getSessionContext();

    /// There are contexts with no user at all - the one the DDL worker executes a distributed query in,
    /// unless `distributed_ddl_use_initial_user_and_roles` is set. There are no current roles to change
    /// there, and `setCurrentRoles` reaches `ContextAccess::getUser`, which treats a missing user as a
    /// logical error and aborts. Refuse it as the user error it is instead.
    if (!session_context->getUserID())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SET ROLE requires a user in the current context");

    if (query.kind == ASTSetRoleQuery::Kind::SET_ROLE_DEFAULT)
        session_context->setCurrentRolesDefault();
    else
        session_context->setCurrentRoles(RolesOrUsersSet{*query.roles, session_context->getAccessControl()});
}


BlockIO InterpreterSetRoleQuery::setDefaultRole(const ASTPtr & updated_query_ptr, const ASTSetRoleQuery & query)
{
    getContext()->getAccess()->checkCanAdministerDefaultRoles();
    getContext()->checkAccess(query.to_users->collectRequiredGrants(AccessType::ALTER_USER));

    auto & access_control = getContext()->getAccessControl();

    /// Resolve the roles on the initiator, before a cluster query is queued into the DDL log. An unknown
    /// role name is a user error and has to be reported to the client right away - the same contract as the
    /// local path here and as `ALTER USER ... DEFAULT ROLE ... ON CLUSTER`, which also resolves the roles
    /// before distributing the query. Otherwise the query would be accepted and then fail on every host.
    RolesOrUsersSet roles_from_query{*query.roles, access_control};

    if (!query.cluster.empty())
    {
        /// `CURRENT_USER` must be resolved here, on the initiator: the AST text is what reaches every host, and
        /// a DDL worker there runs as its own user, so an unresolved tag would set the default roles of the
        /// wrong user. It has to happen after the access check above, because `collectRequiredGrants` demands
        /// `ALTER USER` for a named user while the `CURRENT_USER` tag demands nothing - setting one's own
        /// default roles is allowed without any grant, and resolving the tag first would break that.
        query.replaceCurrentUserTag(getContext()->getUserName());
        return executeDDLQueryOnCluster(updated_query_ptr, getContext());
    }

    /// A `CURRENT_USER` tag cannot be resolved in a context with no user - the one a DDL worker executes a
    /// distributed query in, unless `distributed_ddl_use_initial_user_and_roles` is set. `RolesOrUsersSet`
    /// asserts on the missing user, and in a DDL worker that turns into a crash loop, because the queued
    /// entry is retried on every restart. The initiator resolves the tag before distributing the query, so
    /// this guards an AST that reaches here by other means - a fuzzer, or an entry queued by an older server.
    if (!getContext()->getUserID() && (query.to_users->current_user || query.to_users->except_current_user))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SET DEFAULT ROLE TO CURRENT_USER requires a user in the current context");

    std::vector<UUID> to_users = RolesOrUsersSet{*query.to_users, access_control, getContext()->getUserID()}.getMatchingIDs(access_control);

    auto update_func = [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
    {
        auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
        updateUserSetDefaultRoles(*updated_user, roles_from_query);
        return updated_user;
    };

    access_control.update(to_users, update_func);
    return {};
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
