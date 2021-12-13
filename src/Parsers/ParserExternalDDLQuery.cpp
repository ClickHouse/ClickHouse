#include "config_core.h"

#include <Parsers/ASTExternalDDLQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserExternalDDLQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserRenameQuery.h>

#ifdef USE_MYSQL
#    include <Parsers/MySQL/ASTAlterQuery.h>
#    include <Parsers/MySQL/ASTCreateQuery.h>
#endif

namespace DB
{

#ifdef USE_MYSQL
namespace ErrorCodes
{
    extern const int MYSQL_SYNTAX_ERROR;
}
#endif

bool ParserExternalDDLQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserFunction p_function;
    ParserKeyword s_external("EXTERNAL DDL FROM");

    ASTPtr from;
    auto external_ddl_query = std::make_shared<ASTExternalDDLQuery>();

    if (!s_external.ignore(pos, expected))
        return false;

    if (!p_function.parse(pos, from, expected))
        return false;

    external_ddl_query->set(external_ddl_query->from, from);

    bool res = false;
    if (external_ddl_query->from->name == "MySQL")
    {
#ifdef USE_MYSQL
        ParserDropQuery p_drop_query;
        ParserRenameQuery p_rename_query;
        MySQLParser::ParserAlterQuery p_alter_query;
        MySQLParser::ParserCreateQuery p_create_query;

        res = p_create_query.parse(pos, external_ddl_query->external_ddl, expected)
            || p_drop_query.parse(pos, external_ddl_query->external_ddl, expected)
            || p_alter_query.parse(pos, external_ddl_query->external_ddl, expected)
            || p_rename_query.parse(pos, external_ddl_query->external_ddl, expected);

        if (external_ddl_query->external_ddl)
            external_ddl_query->children.push_back(external_ddl_query->external_ddl);

        if (!res)
        {
            /// Syntax error is ignored, so we need to convert the error code for parsing failure

            if (ParserKeyword("ALTER TABLE").ignore(pos))
                throw Exception("Cannot parse MySQL alter query.", ErrorCodes::MYSQL_SYNTAX_ERROR);

            if (ParserKeyword("RENAME TABLE").ignore(pos))
                throw Exception("Cannot parse MySQL rename query.", ErrorCodes::MYSQL_SYNTAX_ERROR);

            if (ParserKeyword("DROP TABLE").ignore(pos) || ParserKeyword("TRUNCATE").ignore(pos))
                throw Exception("Cannot parse MySQL drop query.", ErrorCodes::MYSQL_SYNTAX_ERROR);

            if (ParserKeyword("CREATE TABLE").ignore(pos) || ParserKeyword("CREATE TEMPORARY TABLE").ignore(pos))
                throw Exception("Cannot parse MySQL create query.", ErrorCodes::MYSQL_SYNTAX_ERROR);
        }
#endif
    }

    node = external_ddl_query;
    return res;
}

}
