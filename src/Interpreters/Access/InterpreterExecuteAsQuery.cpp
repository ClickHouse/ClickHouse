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
    /// Resolves the target user name to a UUID.
    ///
    /// Two-pass lookup so that the existing storage precedence is preserved:
    ///
    ///   1. A normal `find<User>(name)` first. This walks all configured storages in
    ///      `user_directories` order and returns the first one that matches in its
    ///      in-memory state. A target that already exists locally wins over an LDAP
    ///      entry of the same name, regardless of the order of the storages.
    ///   2. Only on a global miss, retry with `force_external_lookup=true`. That lets
    ///      `LDAPAccessStorage` resolve names that are not yet in its in-memory cache
    ///      by querying the upstream directory with the configured service-bind
    ///      credentials. This covers users provisioned in LDAP who have not yet
    ///      authenticated against this server since the last restart.
    ///
    /// If both passes miss, fall through to `getID` so the caller sees the canonical
    /// `UNKNOWN_USER` error message.
    UUID resolveImpersonationTargetUser(const ContextPtr & context, const String & target_user_name)
    {
        const auto & access_control = context->getAccessControl();
        if (auto id = access_control.find<User>(target_user_name))
            return *id;
        if (auto id = access_control.find<User>(target_user_name, /* force_external_lookup = */ true))
            return *id;
        return access_control.getID<User>(target_user_name);
    }

    /// Creates another query context to execute a query as another user.
    ContextMutablePtr impersonateQueryContext(ContextPtr context, const String & target_user_name)
    {
        auto new_context = Context::createCopy(context->getGlobalContext());
        new_context->setClientInfo(context->getClientInfo());
        new_context->makeQueryContext();
        new_context->setCurrentQueryId({});

        const auto & database = context->getCurrentDatabase();
        if (!database.empty() && database != new_context->getCurrentDatabase())
            new_context->setCurrentDatabase(database);

        new_context->setInsertionTable(context->getInsertionTable(), context->getInsertionTableColumnNames());
        new_context->setProgressCallback(context->getProgressCallback());
        new_context->setProcessListElement(context->getProcessListElement());

        if (context->getCurrentTransaction())
            new_context->setCurrentTransaction(context->getCurrentTransaction());

        if (context->getZooKeeperMetadataTransaction())
            new_context->initZooKeeperMetadataTransaction(context->getZooKeeperMetadataTransaction());

        new_context->setUser(resolveImpersonationTargetUser(context, target_user_name));

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

        context->setUser(resolveImpersonationTargetUser(context, target_user_name));

        /// We need to update the client info to make currentUser() return `target_user_name`.
        context->setCurrentUserName(target_user_name);
        context->setInitialUserName(target_user_name);

        context->clampToSettingsConstraints(changed_settings, SettingSource::QUERY);
        context->applySettingsChanges(changed_settings);

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
    String target_user_name = query.target_user->toString();
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


void registerInterpreterExecuteAsQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterExecuteAsQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterExecuteAsQuery", create_fn);
}

}
