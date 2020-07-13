#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#include <Interpreters/InterpreterExternalDDLQuery.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExternalDDLQuery.h>

#ifdef USE_MYSQL
#    include <Parsers/MySQL/ASTAlterQuery.h>
#    include <Parsers/MySQL/ASTCreateQuery.h>
#    include <Interpreters/MySQL/InterpretersMySQLDDLQuery.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

InterpreterExternalDDLQuery::InterpreterExternalDDLQuery(const ASTPtr & query_, Context & context_)
    : query(query_), context(context_)
{
}

BlockIO InterpreterExternalDDLQuery::execute()
{
    const ASTExternalDDLQuery & external_ddl_query = query->as<ASTExternalDDLQuery &>();

    if (external_ddl_query.from->name == "MySQL")
    {
#ifdef USE_MYSQL
        const ASTs & arguments = external_ddl_query.from->arguments->children;

        if (arguments.size() != 2 || !arguments[0]->as<ASTIdentifier>() || !arguments[1]->as<ASTIdentifier>())
            throw Exception("MySQL External require two identifier arguments.", ErrorCodes::BAD_ARGUMENTS);

#endif
        if (external_ddl_query.external_ddl->as<ASTDropQuery>())
            return MySQLInterpreter::InterpreterMySQLDropQuery(
                external_ddl_query.external_ddl, context, getIdentifierName(arguments[0]),
                getIdentifierName(arguments[1])).execute();
        else if (external_ddl_query.external_ddl->as<ASTRenameQuery>())
            return MySQLInterpreter::InterpreterMySQLRenameQuery(
                external_ddl_query.external_ddl, context, getIdentifierName(arguments[0]),
                getIdentifierName(arguments[1])).execute();
        else if (external_ddl_query.external_ddl->as<MySQLParser::ASTAlterQuery>())
            return MySQLInterpreter::InterpreterMySQLAlterQuery(
                external_ddl_query.external_ddl, context, getIdentifierName(arguments[0]),
                getIdentifierName(arguments[1])) .execute();
        else if (external_ddl_query.external_ddl->as<MySQLParser::ASTCreateQuery>())
            return MySQLInterpreter::InterpreterMySQLCreateQuery(
                external_ddl_query.external_ddl, context, getIdentifierName(arguments[0]),
                getIdentifierName(arguments[1])) .execute();
    }

    return BlockIO();
}

}
