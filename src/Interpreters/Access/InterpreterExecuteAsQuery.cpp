#include <Core/ServerSettings.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterExecuteAsQuery.h>
#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/AccessControl.h>
#include <Access/User.h>
#include <Interpreters/Context.h>

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


BlockIO InterpreterExecuteAsQuery::execute()
{
    const auto & query = query_ptr->as<const ASTExecuteAsQuery &>();
    setImpersonateUser(query);
    return {};
}

void InterpreterExecuteAsQuery::setImpersonateUser(const ASTExecuteAsQuery & query)
{
    auto session_context = getContext()->getSessionContext();
    const auto targetusername = query.targetuser->names[0];
    if (!getContext()->getGlobalContext()->getServerSettings()[ServerSetting::allow_impersonate_user])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "IMPERSONATE feature is disabled, set allow_impersonate_user to 1 to enable");
    getContext()->checkAccess(AccessType::IMPERSONATE, targetusername);
    session_context->switchImpersonateUser(RolesOrUsersSet{*query.targetuser, session_context->getAccessControl()});
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
