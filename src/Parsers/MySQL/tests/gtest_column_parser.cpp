#include <gtest/gtest.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/MySQL/ASTDeclareColumn.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/MySQL/ASTDeclareReference.h>
#include <Parsers/MySQL/ASTDeclareConstraint.h>

using namespace DB;
using namespace DB::MySQLParser;

TEST(ParserColumn, AllNonGeneratedColumnOption)
{
    ParserDeclareColumn p_column;

    String input = "col_01 VARCHAR(100) NOT NULL DEFAULT NULL AUTO_INCREMENT UNIQUE KEY PRIMARY KEY COMMENT 'column comment' COLLATE utf8 "
                   "COLUMN_FORMAT FIXED STORAGE MEMORY REFERENCES tbl_name (col_01) CHECK 1";
    ASTPtr ast = parseQuery(p_column, input.data(), input.data() + input.size(), "", 0, 0);
    EXPECT_EQ(ast->as<ASTDeclareColumn>()->name, "col_01");
    EXPECT_EQ(ast->as<ASTDeclareColumn>()->data_type->as<ASTFunction>()->name, "VARCHAR");
    EXPECT_EQ(ast->as<ASTDeclareColumn>()->data_type->as<ASTFunction>()->arguments->children.front()->as<ASTLiteral>()->value.safeGet<UInt64>(), 100);

    ASTDeclareOptions * declare_options = ast->as<ASTDeclareColumn>()->column_options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options->changes["is_null"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 0);
    EXPECT_TRUE(declare_options->changes["default"]->as<ASTLiteral>()->value.isNull());
    EXPECT_EQ(declare_options->changes["auto_increment"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
    EXPECT_EQ(declare_options->changes["unique_key"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
    EXPECT_EQ(declare_options->changes["primary_key"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
    EXPECT_EQ(declare_options->changes["comment"]->as<ASTLiteral>()->value.safeGet<DB::String>(), "column comment");
    EXPECT_EQ(declare_options->changes["collate"]->as<ASTIdentifier>()->name(), "utf8");
    EXPECT_EQ(declare_options->changes["column_format"]->as<ASTIdentifier>()->name(), "FIXED");
    EXPECT_EQ(declare_options->changes["storage"]->as<ASTIdentifier>()->name(), "MEMORY");
    EXPECT_TRUE(declare_options->changes["reference"]->as<ASTDeclareReference>());
    EXPECT_TRUE(declare_options->changes["constraint"]->as<ASTDeclareConstraint>());
}

TEST(ParserColumn, AllGeneratedColumnOption)
{
    ParserDeclareColumn p_column;

    String input = "col_01 VARCHAR(100) NULL UNIQUE KEY PRIMARY KEY COMMENT 'column comment' COLLATE utf8 "
                   "REFERENCES tbl_name (col_01) CHECK 1 GENERATED ALWAYS AS (1) STORED";
    ASTPtr ast = parseQuery(p_column, input.data(), input.data() + input.size(), "", 0, 0);
    EXPECT_EQ(ast->as<ASTDeclareColumn>()->name, "col_01");
    EXPECT_EQ(ast->as<ASTDeclareColumn>()->data_type->as<ASTFunction>()->name, "VARCHAR");
    EXPECT_EQ(ast->as<ASTDeclareColumn>()->data_type->as<ASTFunction>()->arguments->children.front()->as<ASTLiteral>()->value.safeGet<UInt64>(), 100);

    ASTDeclareOptions * declare_options = ast->as<ASTDeclareColumn>()->column_options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options->changes["is_null"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
    EXPECT_EQ(declare_options->changes["unique_key"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
    EXPECT_EQ(declare_options->changes["primary_key"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
    EXPECT_EQ(declare_options->changes["comment"]->as<ASTLiteral>()->value.safeGet<DB::String>(), "column comment");
    EXPECT_EQ(declare_options->changes["collate"]->as<ASTIdentifier>()->name(), "utf8");
    EXPECT_EQ(declare_options->changes["generated"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
    EXPECT_EQ(declare_options->changes["is_stored"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
    EXPECT_TRUE(declare_options->changes["reference"]->as<ASTDeclareReference>());
    EXPECT_TRUE(declare_options->changes["constraint"]->as<ASTDeclareConstraint>());
}
