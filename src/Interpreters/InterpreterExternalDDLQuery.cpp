#include "config.h"

#include <Interpreters/InterpreterExternalDDLQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Context.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExternalDDLQuery.h>

#if USE_MYSQL
#    include <Interpreters/MySQL/InterpretersMySQLDDLQuery.h>
#    include <Parsers/MySQL/ASTAlterQuery.h>
#    include <Parsers/MySQL/ASTCreateQuery.h>
#    include <Parsers/MySQL/ASTDropQuery.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
}

InterpreterExternalDDLQuery::InterpreterExternalDDLQuery(const ASTPtr & query_, ContextMutablePtr context_)
    : WithMutableContext(context_), query(query_)
{
}

BlockIO InterpreterExternalDDLQuery::execute()
{
    const ASTExternalDDLQuery & external_ddl_query = query->as<ASTExternalDDLQuery &>();

    if (getContext()->getClientInfo().query_kind != ClientInfo::QueryKind::SECONDARY_QUERY)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Cannot parse and execute EXTERNAL DDL FROM.");

    if (external_ddl_query.from->name == "MySQL")
    {
#if USE_MYSQL
        const ASTs & arguments = external_ddl_query.from->arguments->children;

        if (arguments.size() != 2 || !arguments[0]->as<ASTIdentifier>() || !arguments[1]->as<ASTIdentifier>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MySQL External require two identifier arguments.");

        if (external_ddl_query.external_ddl->as<MySQLParser::ASTDropQuery>())
            return MySQLInterpreter::InterpreterMySQLDropQuery(
                external_ddl_query.external_ddl, getContext(), getIdentifierName(arguments[0]),
                getIdentifierName(arguments[1])).execute();
        if (external_ddl_query.external_ddl->as<ASTRenameQuery>())
            return MySQLInterpreter::InterpreterMySQLRenameQuery(
                       external_ddl_query.external_ddl, getContext(), getIdentifierName(arguments[0]), getIdentifierName(arguments[1]))
                .execute();
        if (external_ddl_query.external_ddl->as<MySQLParser::ASTAlterQuery>())
            return MySQLInterpreter::InterpreterMySQLAlterQuery(
                       external_ddl_query.external_ddl, getContext(), getIdentifierName(arguments[0]), getIdentifierName(arguments[1]))
                .execute();
        if (external_ddl_query.external_ddl->as<MySQLParser::ASTCreateQuery>())
            return MySQLInterpreter::InterpreterMySQLCreateQuery(
                       external_ddl_query.external_ddl, getContext(), getIdentifierName(arguments[0]), getIdentifierName(arguments[1]))
                .execute();
#endif
    }

    return BlockIO();
}

void registerInterpreterExternalDDLQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterExternalDDLQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterExternalDDLQuery", create_fn);
}

}
