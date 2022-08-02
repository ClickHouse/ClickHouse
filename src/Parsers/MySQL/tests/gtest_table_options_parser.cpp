#include <gtest/gtest.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/MySQL/ASTDeclareTableOptions.h>

using namespace DB;
using namespace DB::MySQLParser;

TEST(ParserTableOptions, AllSubpatitionOptions)
{
    String input = "AUTO_INCREMENt = 1 AVG_ROW_LENGTh 3 CHARACTER SET utf8 CHECKSUM 1 COLLATE utf8_bin"
                   " COMMENT 'table option comment' COMPRESSION 'LZ4' CONNECTION 'connect_string' DATA DIRECTORY 'data_directory'"
                   " INDEX DIRECTORY 'index_directory' DELAY_KEY_WRITE 0 ENCRYPTION 'Y' ENGINE INNODB INSERT_METHOD NO KEY_BLOCK_SIZE 3"
                   " MAX_ROWS 1000 MIN_ROWS 0 PACK_KEYS DEFAULT PASSWORD 'password' ROW_FORMAT DYNAMIC STATS_AUTO_RECALC DEFAULT "
                   " STATS_PERSISTENT DEFAULT STATS_SAMPLE_PAGES 3 TABLESPACE tablespace_name STORAGE MEMORY UNION (table_01, table_02)";

    ParserDeclareTableOptions p_table_options;
    ASTPtr ast = parseQuery(p_table_options, input.data(), input.data() + input.size(), "", 0, 0);

    ASTDeclareOptions * declare_options = ast->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options->changes["auto_increment"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
    EXPECT_EQ(declare_options->changes["avg_row_length"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 3);
    EXPECT_EQ(declare_options->changes["character_set"]->as<ASTIdentifier>()->name(), "utf8");
    EXPECT_EQ(declare_options->changes["checksum"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
    EXPECT_EQ(declare_options->changes["collate"]->as<ASTIdentifier>()->name(), "utf8_bin");
    EXPECT_EQ(declare_options->changes["comment"]->as<ASTLiteral>()->value.safeGet<String>(), "table option comment");
    EXPECT_EQ(declare_options->changes["compression"]->as<ASTLiteral>()->value.safeGet<String>(), "LZ4");
    EXPECT_EQ(declare_options->changes["connection"]->as<ASTLiteral>()->value.safeGet<String>(), "connect_string");
    EXPECT_EQ(declare_options->changes["data_directory"]->as<ASTLiteral>()->value.safeGet<String>(), "data_directory");
    EXPECT_EQ(declare_options->changes["index_directory"]->as<ASTLiteral>()->value.safeGet<String>(), "index_directory");
    EXPECT_EQ(declare_options->changes["delay_key_write"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 0);
    EXPECT_EQ(declare_options->changes["encryption"]->as<ASTLiteral>()->value.safeGet<String>(), "Y");
    EXPECT_EQ(declare_options->changes["engine"]->as<ASTIdentifier>()->name(), "INNODB");
    EXPECT_EQ(declare_options->changes["insert_method"]->as<ASTIdentifier>()->name(), "NO");
    EXPECT_EQ(declare_options->changes["key_block_size"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 3);

    EXPECT_EQ(declare_options->changes["max_rows"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1000);
    EXPECT_EQ(declare_options->changes["min_rows"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 0);
    EXPECT_EQ(declare_options->changes["pack_keys"]->as<ASTIdentifier>()->name(), "DEFAULT");
    EXPECT_EQ(declare_options->changes["password"]->as<ASTLiteral>()->value.safeGet<String>(), "password");
    EXPECT_EQ(declare_options->changes["row_format"]->as<ASTIdentifier>()->name(), "DYNAMIC");
    EXPECT_EQ(declare_options->changes["stats_auto_recalc"]->as<ASTIdentifier>()->name(), "DEFAULT");
    EXPECT_EQ(declare_options->changes["stats_persistent"]->as<ASTIdentifier>()->name(), "DEFAULT");
    EXPECT_EQ(declare_options->changes["stats_sample_pages"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 3);
    EXPECT_EQ(declare_options->changes["tablespace"]->as<ASTIdentifier>()->name(), "tablespace_name");

    ASTPtr arguments = declare_options->changes["union"]->as<ASTFunction>()->arguments;
    EXPECT_EQ(arguments->children.front()->as<ASTIdentifier>()->name(), "table_01");
    EXPECT_EQ((*++arguments->children.begin())->as<ASTIdentifier>()->name(), "table_02");
}

TEST(ParserTableOptions, OptionalTableOptions)
{
    String input = "STATS_AUTO_RECALC DEFAULT AUTO_INCREMENt = 1 ";
    ParserDeclareTableOptions p_table_options;
    ASTPtr ast = parseQuery(p_table_options, input.data(), input.data() + input.size(), "", 0, 0);

    ASTDeclareOptions * declare_options = ast->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options->changes["auto_increment"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
    EXPECT_EQ(declare_options->changes["stats_auto_recalc"]->as<ASTIdentifier>()->name(), "DEFAULT");
}
