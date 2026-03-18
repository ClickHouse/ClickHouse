#include "config.h"

#include <Parsers/ASTExternalDDLQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserExternalDDLQuery.h>
#include <Parsers/ParserRenameQuery.h>

#if USE_MYSQL
#    include <Parsers/MySQL/ASTAlterQuery.h>
#    include <Parsers/MySQL/ASTCreateQuery.h>
#    include <Parsers/MySQL/ASTDropQuery.h>
#endif

namespace DB
{

#if USE_MYSQL
namespace ErrorCodes
{
    extern const int MYSQL_SYNTAX_ERROR;
}
#endif

bool ParserExternalDDLQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserFunction p_function;
    ParserKeyword s_external(Keyword::EXTERNAL_DDL_FROM);

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
#if USE_MYSQL
        MySQLParser::ParserDropQuery p_drop_query;
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

            if (ParserKeyword(Keyword::ALTER_TABLE).ignore(pos))
                throw Exception(ErrorCodes::MYSQL_SYNTAX_ERROR, "Cannot parse MySQL alter query.");

            if (ParserKeyword(Keyword::RENAME_TABLE).ignore(pos))
                throw Exception(ErrorCodes::MYSQL_SYNTAX_ERROR, "Cannot parse MySQL rename query.");

            if (ParserKeyword(Keyword::DROP_TABLE).ignore(pos) || ParserKeyword(Keyword::TRUNCATE).ignore(pos))
                throw Exception(ErrorCodes::MYSQL_SYNTAX_ERROR, "Cannot parse MySQL drop query.");

            if (ParserKeyword(Keyword::CREATE_TABLE).ignore(pos) || ParserKeyword(Keyword::CREATE_TEMPORARY_TABLE).ignore(pos))
                throw Exception(ErrorCodes::MYSQL_SYNTAX_ERROR, "Cannot parse MySQL create query.");
        }
#endif
    }

    node = external_ddl_query;
    return res;
}

}
