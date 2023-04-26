#include <Parsers/ParserShowIndexesQuery.h>

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

bool ParserShowIndexesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr from_database;
    ASTPtr from_table;

    auto query = std::make_shared<ASTShowIndexesQuery>();

    if (!ParserKeyword("SHOW").ignore(pos, expected))
        return false;

    if (ParserKeyword("EXTENDED").ignore(pos, expected))
        query->extended = true;

    if (!(ParserKeyword("INDEX").ignore(pos, expected) || ParserKeyword("INDEXES").ignore(pos, expected) || ParserKeyword("KEYS").ignore(pos, expected)))
        return false;

    if (ParserKeyword("FROM").ignore(pos, expected) || ParserKeyword("IN").ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier().parse(pos, from_table, expected))
            return false;
    }
    else
        return false;

    tryGetIdentifierNameInto(from_table, query->from_table);
    bool abbreviated_form = query->from_table.contains("."); /// FROM <db>.<table>

    if (!abbreviated_form)
        if (ParserKeyword("FROM").ignore(pos, expected) || ParserKeyword("IN").ignore(pos, expected))
            if (!ParserIdentifier().parse(pos, from_database, expected))
                return false;

    tryGetIdentifierNameInto(from_database, query->from_database);

    if (ParserKeyword("WHERE").ignore(pos, expected))
        if (!ParserExpressionWithOptionalAlias(false).parse(pos, query->where_expression, expected))
            return false;

    node = query;

    return true;
}

}

