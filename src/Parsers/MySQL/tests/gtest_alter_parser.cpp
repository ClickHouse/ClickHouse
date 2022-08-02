#include <gtest/gtest.h>

#include <Parsers/parseQuery.h>
#include <Parsers/MySQL/ASTAlterQuery.h>
#include <Parsers/MySQL/ASTAlterCommand.h>

using namespace DB;
using namespace DB::MySQLParser;

static inline ASTPtr tryParserQuery(IParser & parser, const String & query)
{
    return parseQuery(parser, query.data(), query.data() + query.size(), "", 0, 0);
}

TEST(ParserAlterQuery, AlterQuery)
{
    ParserAlterQuery alter_p;

    ASTPtr ast = tryParserQuery(alter_p, "ALTER TABLE test_table_2 ADD COLUMN (f INT, g INT)");
    EXPECT_EQ(ast->as<ASTAlterQuery>()->command_list->children.size(), 1);
    EXPECT_EQ(ast->as<ASTAlterQuery>()->command_list->children.front()->as<ASTAlterCommand>()->type, ASTAlterCommand::ADD_COLUMN);
}
