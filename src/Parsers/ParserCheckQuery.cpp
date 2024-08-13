#include <Parsers/ParserCheckQuery.h>
#include <Parsers/CommonParsers.h>
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

    ParserPartition partition_parser;

    if (!s_check_table.ignore(pos, expected))
        return false;

    auto query = std::make_shared<ASTCheckQuery>();

    if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
        return false;

    if (s_partition.ignore(pos, expected))
    {
        if (!partition_parser.parse(pos, query->partition, expected))
            return false;
    }

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    node = query;
    return true;
}

}
