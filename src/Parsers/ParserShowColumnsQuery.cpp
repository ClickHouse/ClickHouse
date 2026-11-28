#include <Parsers/ParserShowColumnsQuery.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{

bool ParserShowColumnsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr like;

    auto query = make_intrusive<ASTShowColumnsQuery>();

    if (!ParserKeyword(Keyword::SHOW).ignore(pos, expected))
        return false;

    if (ParserKeyword(Keyword::EXTENDED).ignore(pos, expected))
        query->extended = true;

    if (ParserKeyword(Keyword::FULL).ignore(pos, expected))
        query->full = true;

    if (!(ParserKeyword(Keyword::COLUMNS).ignore(pos, expected) || ParserKeyword(Keyword::FIELDS).ignore(pos, expected)))
        return false;

    if (!(ParserKeyword(Keyword::FROM).ignore(pos, expected) || ParserKeyword(Keyword::IN).ignore(pos, expected)))
        return false;
    if (!parseTableNameWithOptionalFromDatabase(pos, expected, query->database, query->table))
        return false;

    if (ParserKeyword(Keyword::NOT).ignore(pos, expected))
        query->not_like = true;

    if (bool insensitive = ParserKeyword(Keyword::ILIKE).ignore(pos, expected); insensitive || ParserKeyword(Keyword::LIKE).ignore(pos, expected))
    {
        if (insensitive)
            query->case_insensitive_like = true;

        if (!ParserStringLiteral().parse(pos, like, expected))
            return false;
    }
    else if (query->not_like)
        return false;
    else if (ParserKeyword(Keyword::WHERE).ignore(pos, expected))
        if (!ParserExpressionWithOptionalAlias(false).parse(pos, query->where_expression, expected))
            return false;

    if (ParserKeyword(Keyword::LIMIT).ignore(pos, expected))
        if (!ParserExpressionWithOptionalAlias(false).parse(pos, query->limit_length, expected))
            return false;

    if (like)
    {
        query->like = like->as<ASTLiteral &>().value.safeGet<String>();
        query->has_like = true;
    }

    node = query;

    return true;
}

}
