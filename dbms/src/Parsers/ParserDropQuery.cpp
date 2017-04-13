#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTDropQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDropQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserDropQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    ParserWhiteSpaceOrComments ws;
    ParserString s_drop("DROP", true, true);
    ParserString s_detach("DETACH", true, true);
    ParserString s_table("TABLE", true, true);
    ParserString s_database("DATABASE", true, true);
    ParserString s_dot(".");
    ParserString s_if("IF", true, true);
    ParserString s_exists("EXISTS", true, true);
    ParserIdentifier name_p;

    ASTPtr database;
    ASTPtr table;
    ASTPtr cluster;
    bool detach = false;
    bool if_exists = false;

    ws.ignore(pos, end);

    if (!s_drop.ignore(pos, end, max_parsed_pos, expected))
    {
        if (s_detach.ignore(pos, end, max_parsed_pos, expected))
            detach = true;
        else
            return false;
    }

    ws.ignore(pos, end);

    if (s_database.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        if (s_if.ignore(pos, end, max_parsed_pos, expected)
            && ws.ignore(pos, end)
            && s_exists.ignore(pos, end, max_parsed_pos, expected)
            && ws.ignore(pos, end))
            if_exists = true;

        if (!name_p.parse(pos, end, database, max_parsed_pos, expected))
            return false;
    }
    else
    {
        if (!s_table.ignore(pos, end, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (s_if.ignore(pos, end, max_parsed_pos, expected)
            && ws.ignore(pos, end)
            && s_exists.ignore(pos, end, max_parsed_pos, expected)
            && ws.ignore(pos, end))
            if_exists = true;

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

        if (ParserString{"ON", true, true}.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!ParserString{"CLUSTER", true, true}.ignore(pos, end, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            if (!name_p.parse(pos, end, cluster, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);
        }
    }

    ws.ignore(pos, end);

    auto query = std::make_shared<ASTDropQuery>(StringRange(begin, pos));
    node = query;

    query->detach = detach;
    query->if_exists = if_exists;
    if (database)
        query->database = typeid_cast<ASTIdentifier &>(*database).name;
    if (table)
        query->table = typeid_cast<ASTIdentifier &>(*table).name;
    if (cluster)
        query->cluster = typeid_cast<ASTIdentifier &>(*cluster).name;

    return true;
}


}
