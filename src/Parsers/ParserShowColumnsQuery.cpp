#include <Parsers/ParserShowColumnsQuery.h>

#include <Parsers/ASTIdentifier_fwd.h>
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

    String from1_str;
    String from2_str;

    auto query = std::make_shared<ASTShowColumnsQuery>();

    if (!ParserKeyword("SHOW").ignore(pos, expected))
        return false;

    if (ParserKeyword("EXTENDED").ignore(pos, expected))
        query->extended = true;

    if (ParserKeyword("FULL").ignore(pos, expected))
        query->full = true;

    if (!(ParserKeyword("COLUMNS").ignore(pos, expected) || ParserKeyword("FIELDS").ignore(pos, expected)))
        return false;

    if (ParserKeyword("FROM").ignore(pos, expected) || ParserKeyword("IN").ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier().parse(pos, from1, expected))
            return false;
    }
    else
        return false;

    tryGetIdentifierNameInto(from1, from1_str);

    bool abbreviated_form = from1_str.contains("."); // FROM database.table
    if (abbreviated_form)
    {
        std::vector<String> split;
        boost::split(split, from1_str, boost::is_any_of("."));
        query->database = split[0];
        query->table = split[1];
    }
    else
    {
        if (ParserKeyword("FROM").ignore(pos, expected) || ParserKeyword("IN").ignore(pos, expected))
            if (!ParserIdentifier().parse(pos, from2, expected))
                return false;

        tryGetIdentifierNameInto(from2, from2_str);

        query->table = from1_str;
        query->database = from2_str;
    }

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
