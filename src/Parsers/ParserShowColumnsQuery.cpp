#include <Parsers/ParserShowColumnsQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

bool ParserShowColumnsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr like;
    ASTPtr from1;
    ASTPtr from2;

    String from2_str;

    auto query = std::make_shared<ASTShowColumnsQuery>();

    if (!ParserKeyword(Keyword::SHOW).ignore(pos, expected))
        return false;

    if (ParserKeyword(Keyword::EXTENDED).ignore(pos, expected))
        query->extended = true;

    if (ParserKeyword(Keyword::FULL).ignore(pos, expected))
        query->full = true;

    if (!(ParserKeyword(Keyword::COLUMNS).ignore(pos, expected) || ParserKeyword(Keyword::FIELDS).ignore(pos, expected)))
        return false;

    if (ParserKeyword(Keyword::FROM).ignore(pos, expected) || ParserKeyword(Keyword::IN).ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier().parse(pos, from1, expected))
            return false;
    }
    else
        return false;

    const auto * table_id = from1->as<ASTIdentifier>();
    if (!table_id)
        return false;
    query->table = table_id->shortName();
    if (table_id->compound())
        query->database = table_id->name_parts[0];
    else
    {
        if (ParserKeyword(Keyword::FROM).ignore(pos, expected) || ParserKeyword(Keyword::IN).ignore(pos, expected))
            if (!ParserIdentifier().parse(pos, from2, expected))
                return false;
        tryGetIdentifierNameInto(from2, from2_str);
        query->database = from2_str;
    }

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
        query->like = like->as<ASTLiteral &>().value.safeGet<const String &>();

    node = query;

    return true;
}

}
