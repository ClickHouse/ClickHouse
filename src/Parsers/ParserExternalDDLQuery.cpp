#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

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
#endif
    }

    node = external_ddl_query;
    return res;
}

}
