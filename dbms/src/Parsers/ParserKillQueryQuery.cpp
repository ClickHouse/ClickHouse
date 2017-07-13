#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ASTKillQueryQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

#include <Common/typeid_cast.h>

namespace DB
{


bool ParserKillQueryQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;
    auto query = std::make_shared<ASTKillQueryQuery>();

    if (!ParserKeyword{"KILL QUERY"}.ignore(pos, expected))
        return false;

    if (!ParserKeyword{"WHERE"}.ignore(pos, expected))
        return false;

    ParserExpression p_where_expression;
    if (!p_where_expression.parse(pos, query->where_expression, expected))
        return false;

    if (ParserKeyword{"SYNC"}.ignore(pos))
        query->sync = true;
    else if (ParserKeyword{"ASYNC"}.ignore(pos))
        query->sync = false;
    else if (ParserKeyword{"TEST"}.ignore(pos))
        query->test = true;

    query->range = StringRange(begin, pos);

    node = std::move(query);

    return true;
}

}
