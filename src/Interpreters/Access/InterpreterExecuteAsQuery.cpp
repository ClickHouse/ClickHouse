#include <Interpreters/Access/InterpreterExecuteAsQuery.h>

#include <Access/AccessControl.h>
#include <Access/User.h>
#include <Core/Settings.h>
#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/QueryFlags.h>
#include <Interpreters/executeQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{
    /// Creates another query context to execute a query as another user.
    ContextMutablePtr impersonateQueryContext(ContextPtr context, const String & target_user_name)
    {
        auto new_context = Context::createCopy(context->getGlobalContext());
        new_context->setClientInfo(context->getClientInfo());
        new_context->makeQueryContext();
        new_context->setCurrentQueryId({});

        /// The wrapped statement is part of the same query the caller sent, so its query parameters
        /// (`{name:Type}`) travel with it. Without this, any parameterized statement under `EXECUTE AS` -
        /// including one stored in a SQL-defined HTTP handler, where the parameters were validated at
        /// CREATE HANDLER time and bound from the request - fails with UNKNOWN_QUERY_PARAMETER.
        new_context->setQueryParameters(context->getQueryParameters());

        /// An empty current database cannot be copied over: `setCurrentDatabase` rejects an empty name,
        /// and it cannot happen for a query anyway - the server refuses to start with an empty
        /// `default_database`, so every session context has a database to inherit.
        const auto & database = context->getCurrentDatabase();
        if (!database.empty() && database != new_context->getCurrentDatabase())
            new_context->setCurrentDatabase(database);

        new_context->setInsertionTable(context->getInsertionTable(), context->getInsertionTableColumnNames(), context->getInsertionTableColumnsDescription());
        new_context->setProgressCallback(context->getProgressCallback());
        new_context->setProcessListElement(context->getProcessListElement());

        if (context->getCurrentTransaction())
            new_context->setCurrentTransaction(context->getCurrentTransaction());

        if (context->getZooKeeperMetadataTransaction())
            new_context->initZooKeeperMetadataTransaction(context->getZooKeeperMetadataTransaction());

        /// The auth-method limits (`GRANTS` clause, per-method `VALID UNTIL`) belong to the session's
        /// credential, not to the principal, so impersonation must not shed them: the impersonated
        /// context keeps the intersection with the originating method's grants and its expiry.
        new_context->setUser(
            context->getAccessControl().getID<User>(target_user_name),
            /* external_roles_= */ {},
            context->getAuthenticationGrants(),
            context->getAuthenticationValidUntil());

        /// `setUser` replaces the current database with the target user's default database, but
        /// the wrapped statement belongs to the caller's query and must keep its database scope.
        if (!database.empty() && database != new_context->getCurrentDatabase())
            new_context->setCurrentDatabase(database);

        /// We need to update the client info to make currentUser() return `target_user_name`.
        new_context->setCurrentUserName(target_user_name);
        new_context->setInitialUserName(target_user_name);

        auto changed_settings = context->getSettingsRef().changes();
        new_context->clampToSettingsConstraints(changed_settings, SettingSource::QUERY);
        new_context->applySettingsChanges(changed_settings);

        return new_context;
    }

    /// Changes the session context to execute all following queries in this session as another user.
    void impersonateSessionContext(ContextMutablePtr context, const String & target_user_name)
    {
        auto database = context->getCurrentDatabase();
        auto changed_settings = context->getSettingsRef().changes();

        /// `setUser` resets the auth-method limits (`GRANTS` clause, per-method `VALID UNTIL`), but they
        /// belong to the session's credential, not to the principal — capture and re-apply them so an
        /// auth-limited session cannot escape its limit by switching principals.
        auto authentication_grants = context->getAuthenticationGrants();
        auto authentication_valid_until = context->getAuthenticationValidUntil();

        context->setUser(
            context->getAccessControl().getID<User>(target_user_name),
            /* external_roles_= */ {},
            authentication_grants,
            authentication_valid_until);

        /// We need to update the client info to make currentUser() return `target_user_name`.
        context->setCurrentUserName(target_user_name);
        context->setInitialUserName(target_user_name);

        context->clampToSettingsConstraints(changed_settings, SettingSource::QUERY);
        context->applySettingsChanges(changed_settings);

        /// See the note about an empty current database in `impersonateQueryContext`.
        if (!database.empty() && database != context->getCurrentDatabase())
            context->setCurrentDatabase(database);
    }
}


BlockIO InterpreterExecuteAsQuery::execute()
{
    if (!getContext()->getAccessControl().isImpersonateUserAllowed())
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "IMPERSONATE feature is disabled, set access_control_improvements.allow_impersonate_user to 1 to enable");
    }

    const auto & query = query_ptr->as<const ASTExecuteAsQuery &>();
    String target_user_name = query.target_user->as<const ASTUserNameWithHost &>().toString();
    getContext()->checkAccess(AccessType::IMPERSONATE, target_user_name);

    if (query.subquery)
    {
        /// EXECUTE AS <user> <subquery>
        auto subquery_context = impersonateQueryContext(getContext(), target_user_name);
        return executeQuery(query.subquery->formatWithSecretsOneLine(), subquery_context, QueryFlags{ .internal = true }).second;
    }
    else
    {
        /// EXECUTE AS <user>
        impersonateSessionContext(getContext()->getSessionContext(), target_user_name);
        return {};
    }
}


void registerInterpreterExecuteAsQuery(InterpreterFactory & factory);
void registerInterpreterExecuteAsQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterExecuteAsQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterExecuteAsQuery", create_fn);
}

}
