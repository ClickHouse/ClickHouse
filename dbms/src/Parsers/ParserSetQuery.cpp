#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{


/// Parse `name = value`.
static bool parseNameValuePair(ASTSetQuery::Change & change, IParser::Pos & pos, IParser::Pos end, IParser::Pos & max_parsed_pos, Expected & expected)
{
    ParserIdentifier name_p;
    ParserLiteral value_p;
    ParserWhitespaceOrComments ws;
    ParserString s_eq("=");

    ASTPtr name;
    ASTPtr value;

    ws.ignore(pos, end);

    if (!name_p.parse(pos, end, name, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (!s_eq.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (!value_p.parse(pos, end, value, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    change.name = typeid_cast<const ASTIdentifier &>(*name).name;
    change.value = typeid_cast<const ASTLiteral &>(*value).value;

    return true;
}


bool ParserSetQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    ParserWhitespaceOrComments ws;
    ParserString s_comma(",");

    if (!parse_only_internals)
    {
        ParserKeyword s_set("SET");
        ParserKeyword s_global("GLOBAL");

        ws.ignore(pos, end);

        if (!s_set.ignore(pos, end, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);
    }

    ASTSetQuery::Changes changes;

    while (true)
    {
        ws.ignore(pos, end);

        if (!changes.empty() && !s_comma.ignore(pos, end))
            break;

        ws.ignore(pos, end);

        changes.push_back(ASTSetQuery::Change());

        if (!parseNameValuePair(changes.back(), pos, end, max_parsed_pos, expected))
            return false;
    }

    auto query = std::make_shared<ASTSetQuery>(StringRange(begin, pos));
    node = query;

    query->changes = changes;

    return true;
}


}
