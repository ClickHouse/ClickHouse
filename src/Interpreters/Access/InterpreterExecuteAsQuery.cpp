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
    extern const int SET_NON_GRANTED_ROLE;
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
