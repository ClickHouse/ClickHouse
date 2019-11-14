#include <Parsers/ParserCheckQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{

bool ParserCheckQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_check_table("CHECK TABLE");
    ParserKeyword s_partition("PARTITION");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier identifier_parser;

    ParserIdentifier table_parser;
    ParserPartition partition_parser;

    ASTPtr table;
    ASTPtr database;

    if (!s_check_table.ignore(pos, expected))
        return false;

    auto query = std::make_shared<ASTCheckQuery>();

    if (!identifier_parser.parse(pos, query->database, expected))
        return false;

    if (s_dot.ignore(pos))
    {
        if (!identifier_parser.parse(pos, query->table, expected))
            return false;
    }
    else
    {
        query->table = query->database;
        query->database.reset();
    }

    if (s_partition.ignore(pos, expected))
    {
        if (!partition_parser.parse(pos, query->partition, expected))
            return false;
    }

    node = query;
    return true;
}

}
