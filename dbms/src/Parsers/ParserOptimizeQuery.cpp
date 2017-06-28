#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOptimizeQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ASTLiteral.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserOptimizeQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    ParserWhitespaceOrComments ws;
    ParserKeyword s_optimize_table("OPTIMIZE TABLE");
    ParserKeyword s_partition("PARTITION");
    ParserKeyword s_final("FINAL");
    ParserKeyword s_deduplicate("DEDUPLICATE");
    ParserString s_dot(".");
    ParserIdentifier name_p;
    ParserLiteral partition_p;

    ASTPtr database;
    ASTPtr table;
    ASTPtr partition;
    bool final = false;
    bool deduplicate = false;

    ws.ignore(pos, end);

    if (!s_optimize_table.ignore(pos, end, max_parsed_pos, expected))
        return false;

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

    if (s_partition.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        if (!partition_p.parse(pos, end, partition, max_parsed_pos, expected))
            return false;
    }

    ws.ignore(pos, end);

    if (s_final.ignore(pos, end, max_parsed_pos, expected))
        final = true;

    ws.ignore(pos, end);

    if (s_deduplicate.ignore(pos, end, max_parsed_pos, expected))
        deduplicate = true;

    auto query = std::make_shared<ASTOptimizeQuery>(StringRange(begin, pos));
    node = query;

    if (database)
        query->database = typeid_cast<const ASTIdentifier &>(*database).name;
    if (table)
        query->table = typeid_cast<const ASTIdentifier &>(*table).name;
    if (partition)
        query->partition = applyVisitor(FieldVisitorToString(), typeid_cast<const ASTLiteral &>(*partition).value);
    query->final = final;
    query->deduplicate = deduplicate;

    return true;
}


}
