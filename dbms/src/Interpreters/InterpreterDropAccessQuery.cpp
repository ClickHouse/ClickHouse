#include <Interpreters/InterpreterDropAccessQuery.h>
#include <Parsers/ASTDropAccessQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/Role.h>
#include <Access/User2.h>


namespace DB
{
BlockIO InterpreterDropAccessQuery::execute()
{
    const auto & query = query_ptr->as<const ASTDropAccessQuery &>();

    if (query.kind == ASTDropAccessQuery::Kind::ROLE)
    {
        if (query.if_exists)
            context.getAccessControlManager().tryRemove<Role>(query.names);
        else
            context.getAccessControlManager().remove<Role>(query.names);
    }
    else if (query.kind == ASTDropAccessQuery::Kind::USER)
    {
        if (query.if_exists)
            context.getAccessControlManager().tryRemove<User2>(query.names);
        else
            context.getAccessControlManager().remove<User2>(query.names);
    }

    return {};
}
}
