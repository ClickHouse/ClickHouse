#include <gtest/gtest.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/MySQL/ASTDeclarePartitionOptions.h>
#include <Parsers/MySQL/ASTCreateDefines.h>
#include <IO/WriteBufferFromOStream.h>

using namespace DB;
using namespace DB::MySQLParser;

TEST(CreateTableParser, LikeCreate)
{
    ParserCreateQuery p_create_query;
    String like_create_01 = "CREATE TABLE IF NOT EXISTS table_name LIKE table_name_01";
    parseQuery(p_create_query, like_create_01.data(), like_create_01.data() + like_create_01.size(), "", 0, 0);
    String like_create_02 = "CREATE TABLE IF NOT EXISTS table_name (LIKE table_name_01)";
    parseQuery(p_create_query, like_create_02.data(), like_create_02.data() + like_create_02.size(), "", 0, 0);
}

TEST(CreateTableParser, SimpleCreate)
{
    ParserCreateQuery p_create_query;
    String input = "CREATE TABLE IF NOT EXISTS table_name(col_01 VARCHAR(100), INDEX (col_01), CHECK 1) ENGINE INNODB PARTITION BY HASH(col_01)";
    ASTPtr ast = parseQuery(p_create_query, input.data(), input.data() + input.size(), "", 0, 0);
    EXPECT_TRUE(ast->as<ASTCreateQuery>()->if_not_exists);
    EXPECT_EQ(ast->as<ASTCreateQuery>()->columns_list->as<ASTCreateDefines>()->columns->children.size(), 1);
    EXPECT_EQ(ast->as<ASTCreateQuery>()->columns_list->as<ASTCreateDefines>()->indices->children.size(), 1);
    EXPECT_EQ(ast->as<ASTCreateQuery>()->columns_list->as<ASTCreateDefines>()->constraints->children.size(), 1);
    EXPECT_EQ(ast->as<ASTCreateQuery>()->table_options->as<ASTDeclareOptions>()->changes["engine"]->as<ASTIdentifier>()->name(), "INNODB");
    EXPECT_TRUE(ast->as<ASTCreateQuery>()->partition_options->as<ASTDeclarePartitionOptions>());
}

TEST(CreateTableParser, SS)
{
    ParserCreateQuery p_create_query;
    String input = "CREATE TABLE `test_table_1` (`a` int DEFAULT NULL, `b` int DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
    ASTPtr ast = parseQuery(p_create_query, input.data(), input.data() + input.size(), "", 0, 0);
    WriteBufferFromOStream buf(std::cerr, 4096);
    ast->dumpTree(buf);

}
