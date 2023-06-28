#include <Parsers/ParserShowColumnsQuery.h>

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

bool ParserShowColumnsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr like;
    ASTPtr from_database;
    ASTPtr from_table;

    auto query = std::make_shared<ASTShowColumnsQuery>();

    if (!ParserKeyword("SHOW").ignore(pos, expected))
        return false;

    if (ParserKeyword("EXTENDED").ignore(pos, expected))
        query->extended = true;

    if (ParserKeyword("FULL").ignore(pos, expected))
        query->full = true;

    if (!ParserKeyword("COLUMNS").ignore(pos, expected) || ParserKeyword("FIELDS").ignore(pos, expected))
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

    if (ParserKeyword("NOT").ignore(pos, expected))
        query->not_like = true;

    if (bool insensitive = ParserKeyword("ILIKE").ignore(pos, expected); insensitive || ParserKeyword("LIKE").ignore(pos, expected))
    {
        if (insensitive)
            query->case_insensitive_like = true;

        if (!ParserStringLiteral().parse(pos, like, expected))
            return false;
    }
    else if (query->not_like)
        return false;
    else if (ParserKeyword("WHERE").ignore(pos, expected))
        if (!ParserExpressionWithOptionalAlias(false).parse(pos, query->where_expression, expected))
            return false;

    if (ParserKeyword("LIMIT").ignore(pos, expected))
        if (!ParserExpressionWithOptionalAlias(false).parse(pos, query->limit_length, expected))
            return false;

    if (like)
        query->like = like->as<ASTLiteral &>().value.safeGet<const String &>();

    node = query;

    return true;
}

}
