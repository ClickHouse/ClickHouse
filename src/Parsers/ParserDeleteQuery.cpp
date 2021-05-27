#include <Parsers/ParserDeleteQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{
bool ParserDeleteQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTDeleteQuery>();

    node = query;

    ParserKeyword s_delete_from("DELETE FROM");
    ParserKeyword s_where("WHERE");

    if (!s_delete_from.ignore(pos, expected))
        return false;

    if (!parseDatabaseAndTableName(pos, expected, query->database, query->table))
        return false;

    if (!s_where.ignore(pos, expected))
        return false;

    ParserExpression parser_exp_elem;

    return parser_exp_elem.parse(pos, query->predicate, expected);
}
}
