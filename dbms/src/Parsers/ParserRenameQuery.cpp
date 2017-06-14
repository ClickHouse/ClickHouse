#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTRenameQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserRenameQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{


/// Parse database.table or table.
static bool parseDatabaseAndTable(
    ASTRenameQuery::Table & db_and_table, IParser::Pos & pos, IParser::Pos end, IParser::Pos & max_parsed_pos, Expected & expected)
{
    ParserIdentifier name_p;
    ParserWhiteSpaceOrComments ws;
    ParserString s_dot(".");

    ASTPtr database;
    ASTPtr table;

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

    db_and_table.database = database ? typeid_cast<const ASTIdentifier &>(*database).name : "";
    db_and_table.table = typeid_cast<const ASTIdentifier &>(*table).name;

    return true;
}


bool ParserRenameQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    ParserWhiteSpaceOrComments ws;
    ParserString s_rename("RENAME", true, true);
    ParserString s_table("TABLE", true, true);
    ParserString s_to("TO", true, true);
    ParserString s_comma(",");

    ws.ignore(pos, end);

    if (!s_rename.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (!s_table.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ASTRenameQuery::Elements elements;

    while (true)
    {
        ws.ignore(pos, end);

        if (!elements.empty() && !s_comma.ignore(pos, end))
            break;

        ws.ignore(pos, end);

        elements.push_back(ASTRenameQuery::Element());

        if (!parseDatabaseAndTable(elements.back().from, pos, end, max_parsed_pos, expected)
            || !s_to.ignore(pos, end)
            || !parseDatabaseAndTable(elements.back().to, pos, end, max_parsed_pos, expected))
            return false;
    }

    String cluster_str;
    if (ParserString{"ON", true, true}.ignore(pos, end, max_parsed_pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, end, cluster_str, max_parsed_pos, expected))
            return false;
    }

    auto query = std::make_shared<ASTRenameQuery>(StringRange(begin, pos));
    query->cluster = cluster_str;
    node = query;

    query->elements = elements;
    return true;
}


}
