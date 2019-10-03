#include <Parsers/ParserQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserCreateAccessQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserDropAccessQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserUseQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ParserGrantQuery.h>
#include <Parsers/ParserCreateQuery.h>


namespace DB
{


bool ParserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserQueryWithOutput query_with_output_p(enable_explain);
    ParserInsertQuery insert_p(end);
    ParserUseQuery use_p;
    ParserSetQuery set_p;
    ParserSystemQuery system_p;

    bool res = query_with_output_p.parse(pos, node, expected)
        || insert_p.parse(pos, node, expected)
        || use_p.parse(pos, node, expected)
        || set_p.parse(pos, node, expected)
        || system_p.parse(pos, node, expected)
        || ParserCreateUserQuery{}.parse(pos, node, expected)
        || ParserCreateRoleQuery{}.parse(pos, node, expected)
        || ParserDropAccessQuery{}.parse(pos, node, expected)
        || ParserGrantQuery{}.parse(pos, node, expected);

    return res;
}

}
