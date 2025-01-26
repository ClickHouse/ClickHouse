#include <Core/ServerSettings.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterExecuteAsQuery.h>
#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/AccessControl.h>
#include <Access/User.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

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
    const auto current_user_uuid = getContext()->getSessionContext()->getUserID();
    const auto & query = query_ptr->as<const ASTExecuteAsQuery &>();

    setImpersonateUser(query);

    BlockIO return_value = {};

    auto restore_previous_user = [&] () { getContext()->getSessionContext()->switchImpersonateUser(RolesOrUsersSet(current_user_uuid.value())); };

    if (query.select)
    {
        try
        {
            auto select_query = InterpreterSelectWithUnionQuery(query.select->clone(), getContext()->getSessionContext(), {});
            return_value = select_query.execute();
        }
        catch (...)
        {
            restore_previous_user();
            throw;
        }
        restore_previous_user();
    }
    return return_value;
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
