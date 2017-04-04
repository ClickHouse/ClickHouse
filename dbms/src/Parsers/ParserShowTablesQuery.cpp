#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTShowTablesQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ExpressionElementParsers.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserShowTablesQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    ParserWhiteSpaceOrComments ws;
    ParserString s_show("SHOW", true, true);
    ParserString s_tables("TABLES", true, true);
    ParserString s_databases("DATABASES", true, true);
    ParserString s_from("FROM", true, true);
    ParserString s_not("NOT", true, true);
    ParserString s_like("LIKE", true, true);
    ParserStringLiteral like_p;
    ParserIdentifier name_p;

    ASTPtr like;
    ASTPtr database;

    auto query = std::make_shared<ASTShowTablesQuery>();

    ws.ignore(pos, end);

    if (!s_show.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (s_databases.ignore(pos, end))
    {
        query->databases = true;
    }
    else if (s_tables.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        if (s_from.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!name_p.parse(pos, end, database, max_parsed_pos, expected))
                return false;
        }

        ws.ignore(pos, end);

        if (s_not.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            query->not_like = true;
        }

        if (s_like.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!like_p.parse(pos, end, like, max_parsed_pos, expected))
                return false;
        }
        else if (query->not_like)
            return false;
    }
    else
        return false;

    ws.ignore(pos, end);

    query->range = StringRange(begin, pos);

    if (database)
        query->from = typeid_cast<ASTIdentifier &>(*database).name;
    if (like)
        query->like = safeGet<const String &>(typeid_cast<ASTLiteral &>(*like).value);

    node = query;

    return true;
}


}
