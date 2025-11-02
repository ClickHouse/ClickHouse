#include <Interpreters/Access/InterpreterExecuteAsQuery.h>

#include <Access/AccessControl.h>
#include <Access/User.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
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

namespace ServerSetting
{
    extern const ServerSettingsBool allow_impersonate_user;
}

namespace
{
    /// Creates another query context to execute a query as another user.
    ContextMutablePtr impersonateQueryContext(ContextPtr context, const String & target_user_name)
    {
        auto new_context = Context::createCopy(context->getGlobalContext());
        new_context->setClientInfo(context->getClientInfo());
        new_context->makeQueryContext();

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

        new_context->setUser(context->getAccessControl().getID<User>(target_user_name));

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

        context->setUser(context->getAccessControl().getID<User>(target_user_name));

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
    if (!getContext()->getGlobalContext()->getServerSettings()[ServerSetting::allow_impersonate_user])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "IMPERSONATE feature is disabled, set allow_impersonate_user to 1 to enable");

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
