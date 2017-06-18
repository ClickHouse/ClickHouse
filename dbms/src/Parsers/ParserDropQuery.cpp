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

    ParserWhitespaceOrComments ws;
    ParserKeyword s_drop("DROP");
    ParserKeyword s_detach("DETACH");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_database("DATABASE");
    ParserKeyword s_dot(".");
    ParserKeyword s_if_exists("IF EXISTS");
    ParserIdentifier name_p;

    ASTPtr database;
    ASTPtr table;
    String cluster_str;
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

        if (s_if_exists.ignore(pos, end, max_parsed_pos, expected))
            if_exists = true;

        ws.ignore(pos, end);

        if (!name_p.parse(pos, end, database, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (ParserKeyword{"ON"}.ignore(pos, end, max_parsed_pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, end, cluster_str, max_parsed_pos, expected))
                return false;
        }
    }
    else
    {
        if (!s_table.ignore(pos, end, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (s_if_exists.ignore(pos, end, max_parsed_pos, expected))
            if_exists = true;

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

        if (ParserKeyword{"ON"}.ignore(pos, end, max_parsed_pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, end, cluster_str, max_parsed_pos, expected))
                return false;
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
    query->cluster = cluster_str;

    return true;
}


}
