#include <Parsers/ParserCheckQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ParserPartition.h>


namespace DB
{

bool ParserCheckQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    ParserKeyword s_check_table("CHECK TABLE");
    ParserKeyword s_partition("PARTITION");
    ParserToken s_dot(TokenType::Dot);

    ParserIdentifier table_parser;
    ParserPartition partition_parser;

    ASTPtr table;
    ASTPtr database;

    if (!s_check_table.ignore(pos, expected, ranges))
        return false;
    if (!table_parser.parse(pos, database, expected, ranges))
        return false;

    auto query = std::make_shared<ASTCheckQuery>();
    if (s_dot.ignore(pos, expected, ranges))
    {
        if (!table_parser.parse(pos, table, expected, ranges))
            return false;

        tryGetIdentifierNameInto(database, query->database);
        tryGetIdentifierNameInto(table, query->table);
    }
    else
    {
        table = database;
        tryGetIdentifierNameInto(table, query->table);
    }

    if (s_partition.ignore(pos, expected, ranges))
    {
        if (!partition_parser.parse(pos, query->partition, expected, ranges))
            return false;
    }

    node = query;
    return true;
}

}
