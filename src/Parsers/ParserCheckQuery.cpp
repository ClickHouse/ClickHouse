#include <Parsers/ParserCheckQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTCheckDatabaseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/StatementFactory.h>


namespace DB
{

bool ParserCheckQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_check_table(Keyword::CHECK_ALL_TABLES);
    if (s_check_table.ignore(pos, expected))
    {
        auto query = make_intrusive<ASTCheckAllTablesQuery>();
        node = query;
        return true;
    }

    if (parseCheckTable(pos, node, expected))
        return true;

    return parseCheckDatabase(pos, node, expected);
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

    auto query = make_intrusive<ASTCheckTableQuery>();

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
        query->part_name = ast_literal->value.safeGet<String>();
    }

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    node = query;
    return true;
}

bool ParserCheckQuery::parseCheckDatabase(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_check_database(Keyword::CHECK_DATABASE);

    if (!s_check_database.ignore(pos, expected))
        return false;

    auto query = make_intrusive<ASTCheckDatabaseQuery>();

    if (!parseDatabaseAsAST(pos, expected, query->database))
        return false;

    if (query->database)
        query->children.push_back(query->database);

    node = query;
    return true;
}

}

namespace DB
{

REGISTER_STATEMENTS(Check)
{
    factory.registerStatement("CHECK TABLE", "",
    {
        .description = R"(
Performs a validation check on a table or on its partitions or parts. It verifies the checksums and the other internal
data structures, in particular it compares the actual file sizes with the expected values stored on the server.
)",
        .syntax = R"(
CHECK TABLE table_name [PARTITION partition_expression | PART part_name] [FORMAT format] [SETTINGS check_query_single_value_result = (0|1) [, other_settings]]
)",
        .examples = {{"Check a table", "CHECK TABLE test_table;", ""}},
        .related = {"CHECK DATABASE", "SYSTEM", "OPTIMIZE"},
    });

    factory.registerStatement("CHECK DATABASE", "",
    {
        .description = R"(
Verifies the health of a database. Its primary use is with the `DataLakeCatalog` database engine, where it checks that
the external catalog backing the database is reachable and that its list of tables can be retrieved. This is a
lightweight probe: it confirms connectivity and authentication without reading any table data.
)",
        .syntax = R"(
CHECK DATABASE database_name
)",
        .examples = {{"Check a database", "CHECK DATABASE datalake;", ""}},
        .related = {"CHECK TABLE", "CREATE DATABASE"},
    });
}

}
