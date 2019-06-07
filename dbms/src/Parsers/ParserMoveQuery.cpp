#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTMoveQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserMoveQuery.h>


namespace DB
{


/// Parse database.table or table.
static bool parseDatabaseAndTable(
    ASTMoveQuery::Table & db_and_table, IParser::Pos & pos, Expected & expected)
{
    ParserIdentifier name_p;
    ParserToken s_dot(TokenType::Dot);

    ASTPtr database;
    ASTPtr table;

    if (!name_p.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }

    db_and_table.database.clear();
    getIdentifierName(database, db_and_table.database);
    getIdentifierName(table, db_and_table.table);

    return true;
}


bool ParserMoveQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier name_p;
    ParserKeyword s_table("TABLE");
    ParserKeyword s_move_part("MOVE PART");
    ParserKeyword s_to("TO");
    ParserToken s_comma(TokenType::Comma);

    if (!s_table.ignore(pos, expected))
        return false;

    ASTMoveQuery::Table table;

    if (!parseDatabaseAndTable(table, pos, expected))
        return false;

    if (!s_move_part.ignore(pos, expected))
        return false;

    String part_name;
    String destination_disk_name;

    ASTPtr part_name_ast_ptr;
    ASTPtr destination_disk_name_ast_ptr;

    if (!name_p.parse(pos, part_name_ast_ptr, expected))
        return false;

    if (!s_to.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, destination_disk_name_ast_ptr, expected))
        return false;


    getIdentifierName(part_name_ast_ptr, part_name);
    getIdentifierName(destination_disk_name_ast_ptr, destination_disk_name);

    String cluster_str;
    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = std::make_shared<ASTMoveQuery>();
    query->cluster = cluster_str;
    node = query;

    query->table = table;
    query->part_name = part_name;
    query->destination_disk_name = destination_disk_name;
    return true;
}


}
