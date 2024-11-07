#include <Parsers/ParserCheckQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{

bool ParserCheckQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_check_table(Keyword::CHECK_ALL_TABLES);
    if (s_check_table.ignore(pos, expected))
    {
        auto query = std::make_shared<ASTCheckAllTablesQuery>();
        node = query;
        return true;
    }

    return parseCheckTable(pos, node, expected);
}

bool ParserCheckQuery::parseCheckTable(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_check_table(Keyword::CHECK_TABLE);
    ParserKeyword s_partition(Keyword::PARTITION);
    ParserKeyword s_part(Keyword::PART);
    ParserToken s_dot(TokenType::Dot);

    ParserPartition partition_parser;
    ParserStringLiteral parser_string_literal;

    if (!s_check_table.ignore(pos, expected))
        return false;

    auto query = std::make_shared<ASTCheckTableQuery>();

    if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
        return false;

    if (s_partition.ignore(pos, expected))
    {
        if (!partition_parser.parse(pos, query->partition, expected))
            return false;
    }
    else if (s_part.ignore(pos, expected))
    {
        ASTPtr ast_part_name;
        if (!parser_string_literal.parse(pos, ast_part_name, expected))
            return false;

        const auto * ast_literal = ast_part_name->as<ASTLiteral>();
        if (!ast_literal || ast_literal->value.getType() != Field::Types::String)
            return false;
        query->part_name = ast_literal->value.safeGet<const String &>();
    }

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    node = query;
    return true;
}

}
