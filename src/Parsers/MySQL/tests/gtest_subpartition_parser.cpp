#include <gtest/gtest.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/MySQL/ASTDeclareSubPartition.h>

using namespace DB;
using namespace DB::MySQLParser;

TEST(ParserSubpartition, AllSubpatitionOptions)
{
    String input = "SUBPARTITION subpartition_name ENGINE = engine_name COMMENT 'subpartition comment'"
                   " DATA DIRECTORY 'data_directory' INDEX DIRECTORY 'index_directory' max_rows 1000 MIN_ROWs 0"
                   " TABLESPACE table_space_name";
    MySQLParser::ParserDeclareSubPartition p_subpartition;
    ASTPtr ast = parseQuery(p_subpartition, input.data(), input.data() + input.size(), "", 0, 0);

    ASTDeclareSubPartition * declare_subpartition = ast->as<ASTDeclareSubPartition>();
    EXPECT_EQ(declare_subpartition->logical_name, "subpartition_name");
    ASTDeclareOptions * declare_options = declare_subpartition->options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options->changes["engine"]->as<ASTIdentifier>()->name(), "engine_name");
    EXPECT_EQ(declare_options->changes["comment"]->as<ASTLiteral>()->value.safeGet<String>(), "subpartition comment");
    EXPECT_EQ(declare_options->changes["data_directory"]->as<ASTLiteral>()->value.safeGet<String>(), "data_directory");
    EXPECT_EQ(declare_options->changes["index_directory"]->as<ASTLiteral>()->value.safeGet<String>(), "index_directory");
    EXPECT_EQ(declare_options->changes["min_rows"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 0);
    EXPECT_EQ(declare_options->changes["max_rows"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1000);
    EXPECT_EQ(declare_options->changes["tablespace"]->as<ASTIdentifier>()->name(), "table_space_name");
}

TEST(ParserSubpartition, OptionalSubpatitionOptions)
{
    String input = "SUBPARTITION subpartition_name STORAGE engine = engine_name max_rows 1000 min_rows 0 tablespace table_space_name";
    MySQLParser::ParserDeclareSubPartition p_subpartition;
    ASTPtr ast = parseQuery(p_subpartition, input.data(), input.data() + input.size(), "", 0, 0);

    ASTDeclareSubPartition * declare_subpartition = ast->as<ASTDeclareSubPartition>();
    EXPECT_EQ(declare_subpartition->logical_name, "subpartition_name");
    ASTDeclareOptions * declare_options = declare_subpartition->options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options->changes["engine"]->as<ASTIdentifier>()->name(), "engine_name");
    EXPECT_EQ(declare_options->changes["min_rows"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 0);
    EXPECT_EQ(declare_options->changes["max_rows"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1000);
    EXPECT_EQ(declare_options->changes["tablespace"]->as<ASTIdentifier>()->name(), "table_space_name");
}

