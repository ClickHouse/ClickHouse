#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{


/// Parse `name = value`.
static bool parseNameValuePair(ASTSetQuery::Change & change, IParser::Pos & pos, Expected & expected)
{
    ParserIdentifier name_p;
    ParserLiteral value_p;
    ParserToken s_eq(TokenType::Equals);

    ASTPtr name;
    ASTPtr value;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!s_eq.ignore(pos, expected))
        return false;

    if (!value_p.parse(pos, value, expected))
        return false;

    change.name = typeid_cast<const ASTIdentifier &>(*name).name;
    change.value = typeid_cast<const ASTLiteral &>(*value).value;

    return true;
}


bool ParserSetQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken s_comma(TokenType::Comma);

    if (!parse_only_internals)
    {
        ParserKeyword s_set("SET");

        if (!s_set.ignore(pos, expected))
            return false;
    }

    ASTSetQuery::Changes changes;

    while (true)
    {
        if (!changes.empty() && !s_comma.ignore(pos))
            break;

        changes.push_back(ASTSetQuery::Change());

        if (!parseNameValuePair(changes.back(), pos, expected))
            return false;
    }

    auto query = std::make_shared<ASTSetQuery>();
    node = query;

    query->is_standalone = !parse_only_internals;
    query->changes = changes;

    return true;
}


}
