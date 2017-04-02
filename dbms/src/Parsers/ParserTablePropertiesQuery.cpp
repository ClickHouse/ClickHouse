#include <Parsers/ASTIdentifier.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserTablePropertiesQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserTablePropertiesQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    ParserWhiteSpaceOrComments ws;
    ParserString s_exists("EXISTS", true, true);
    ParserString s_describe("DESCRIBE", true, true);
    ParserString s_desc("DESC", true, true);
    ParserString s_show("SHOW", true, true);
    ParserString s_create("CREATE", true, true);
    ParserString s_table("TABLE", true, true);
    ParserString s_dot(".");
    ParserIdentifier name_p;

    ASTPtr database;
    ASTPtr table;
    std::shared_ptr<ASTQueryWithTableAndOutput> query;

    ws.ignore(pos, end);

    if (s_exists.ignore(pos, end, max_parsed_pos, expected))
    {
        query = std::make_shared<ASTExistsQuery>();
    }
    else if (s_describe.ignore(pos, end, max_parsed_pos, expected) || s_desc.ignore(pos, end, max_parsed_pos, expected))
    {
        query = std::make_shared<ASTDescribeQuery>();
    }
    else if (s_show.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        if (!s_create.ignore(pos, end, max_parsed_pos, expected))
            return false;

        query = std::make_shared<ASTShowCreateQuery>();
    }
    else
    {
        return false;
    }

    ws.ignore(pos, end);

    s_table.ignore(pos, end, max_parsed_pos, expected);

    ws.ignore(pos, end);

    if (!name_p.parse(pos, end, table, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (s_dot.ignore(pos, end, max_parsed_pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, end, table, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);
    }

    ws.ignore(pos, end);

    query->range = StringRange(begin, pos);

    if (database)
        query->database = typeid_cast<ASTIdentifier &>(*database).name;
    if (table)
        query->table = typeid_cast<ASTIdentifier &>(*table).name;

    node = query;

    return true;
}


}
