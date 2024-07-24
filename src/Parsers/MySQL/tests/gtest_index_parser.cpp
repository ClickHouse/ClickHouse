#include <gtest/gtest.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Parsers/MySQL/ASTDeclareOption.h>

using namespace DB;
using namespace DB::MySQLParser;

TEST(ParserIndex, AllIndexOptions)
{
    String input = "INDEX (col_01, col_02(100), col_03 DESC) KEY_BLOCK_SIZE 3 USING HASH WITH PARSER parser_name COMMENT 'index comment' VISIBLE";

    ParserDeclareIndex p_index;
    ASTPtr ast = parseQuery(p_index, input.data(), input.data() + input.size(), "", 0, 0);

    ASTDeclareIndex * declare_index = ast->as<ASTDeclareIndex>();
    EXPECT_EQ(declare_index->index_columns->children[0]->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ(declare_index->index_columns->children[1]->as<ASTFunction>()->name, "col_02");
    EXPECT_EQ(declare_index->index_columns->children[1]->as<ASTFunction>()->arguments->children[0]->as<ASTLiteral>()->value.safeGet<UInt64>(), 100);
    EXPECT_EQ(declare_index->index_columns->children[2]->as<ASTIdentifier>()->name(), "col_03");
    ASTDeclareOptions * declare_options = declare_index->index_options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options->changes["key_block_size"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 3);
    EXPECT_EQ(declare_options->changes["index_type"]->as<ASTIdentifier>()->name(), "HASH");
    EXPECT_EQ(declare_options->changes["comment"]->as<ASTLiteral>()->value.safeGet<String>(), "index comment");
    EXPECT_EQ(declare_options->changes["visible"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
}

TEST(ParserIndex, OptionalIndexOptions)
{
    String input = "INDEX (col_01, col_02(100), col_03 DESC) USING HASH INVISIBLE KEY_BLOCK_SIZE 3";

    ParserDeclareIndex p_index;
    ASTPtr ast = parseQuery(p_index, input.data(), input.data() + input.size(), "", 0, 0);

    ASTDeclareIndex * declare_index = ast->as<ASTDeclareIndex>();
    EXPECT_EQ(declare_index->index_columns->children[0]->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ(declare_index->index_columns->children[1]->as<ASTFunction>()->name, "col_02");
    EXPECT_EQ(declare_index->index_columns->children[1]->as<ASTFunction>()->arguments->children[0]->as<ASTLiteral>()->value.safeGet<UInt64>(), 100);
    EXPECT_EQ(declare_index->index_columns->children[2]->as<ASTIdentifier>()->name(), "col_03");
    ASTDeclareOptions * declare_options = declare_index->index_options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options->changes["index_type"]->as<ASTIdentifier>()->name(), "HASH");
    EXPECT_EQ(declare_options->changes["visible"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 0);
    EXPECT_EQ(declare_options->changes["key_block_size"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 3);
}

TEST(ParserIndex, OrdinaryIndex)
{
    ParserDeclareIndex p_index;
    String non_unique_index_01 = "KEY index_name USING HASH (col_01) INVISIBLE";
    parseQuery(p_index, non_unique_index_01.data(), non_unique_index_01.data() + non_unique_index_01.size(), "", 0, 0);

    String non_unique_index_02 = "INDEX index_name USING HASH (col_01) INVISIBLE";
    parseQuery(p_index, non_unique_index_02.data(), non_unique_index_02.data() + non_unique_index_02.size(), "", 0, 0);

    String fulltext_index_01 = "FULLTEXT index_name (col_01) INVISIBLE";
    parseQuery(p_index, fulltext_index_01.data(), fulltext_index_01.data() + fulltext_index_01.size(), "", 0, 0);

    String fulltext_index_02 = "FULLTEXT INDEX index_name (col_01) INVISIBLE";
    parseQuery(p_index, fulltext_index_02.data(), fulltext_index_02.data() + fulltext_index_02.size(), "", 0, 0);

    String fulltext_index_03 = "FULLTEXT KEY index_name (col_01) INVISIBLE";
    parseQuery(p_index, fulltext_index_03.data(), fulltext_index_03.data() + fulltext_index_03.size(), "", 0, 0);

    String spatial_index_01 = "SPATIAL index_name (col_01) INVISIBLE";
    parseQuery(p_index, spatial_index_01.data(), spatial_index_01.data() + spatial_index_01.size(), "", 0, 0);

    String spatial_index_02 = "SPATIAL INDEX index_name (col_01) INVISIBLE";
    parseQuery(p_index, spatial_index_02.data(), spatial_index_02.data() + spatial_index_02.size(), "", 0, 0);

    String spatial_index_03 = "SPATIAL KEY index_name (col_01) INVISIBLE";
    parseQuery(p_index, spatial_index_03.data(), spatial_index_03.data() + spatial_index_03.size(), "", 0, 0);
}

TEST(ParserIndex, ConstraintIndex)
{
    ParserDeclareIndex p_index;

    String primary_key_01 = "PRIMARY KEY (col_01) INVISIBLE";
    parseQuery(p_index, primary_key_01.data(), primary_key_01.data() + primary_key_01.size(), "", 0, 0);

    String primary_key_02 = "PRIMARY KEY USING BTREE (col_01) INVISIBLE";
    parseQuery(p_index, primary_key_02.data(), primary_key_02.data() + primary_key_02.size(), "", 0, 0);

    String primary_key_03 = "CONSTRAINT PRIMARY KEY USING BTREE (col_01) INVISIBLE";
    parseQuery(p_index, primary_key_03.data(), primary_key_03.data() + primary_key_03.size(), "", 0, 0);

    String primary_key_04 = "CONSTRAINT index_name PRIMARY KEY USING BTREE (col_01) INVISIBLE";
    parseQuery(p_index, primary_key_04.data(), primary_key_04.data() + primary_key_04.size(), "", 0, 0);

    String unique_key_01 = "UNIQUE (col_01) INVISIBLE";
    parseQuery(p_index, unique_key_01.data(), unique_key_01.data() + unique_key_01.size(), "", 0, 0);

    String unique_key_02 = "UNIQUE INDEX (col_01) INVISIBLE";
    parseQuery(p_index, unique_key_02.data(), unique_key_02.data() + unique_key_02.size(), "", 0, 0);

    String unique_key_03 = "UNIQUE KEY (col_01) INVISIBLE";
    parseQuery(p_index, unique_key_03.data(), unique_key_03.data() + unique_key_03.size(), "", 0, 0);

    String unique_key_04 = "UNIQUE KEY index_name (col_01) INVISIBLE";
    parseQuery(p_index, unique_key_04.data(), unique_key_04.data() + unique_key_04.size(), "", 0, 0);

    String unique_key_05 = "UNIQUE KEY index_name USING HASH (col_01) INVISIBLE";
    parseQuery(p_index, unique_key_05.data(), unique_key_05.data() + unique_key_05.size(), "", 0, 0);

    String unique_key_06 = "CONSTRAINT UNIQUE KEY index_name USING HASH (col_01) INVISIBLE";
    parseQuery(p_index, unique_key_06.data(), unique_key_06.data() + unique_key_06.size(), "", 0, 0);

    String unique_key_07 = "CONSTRAINT index_name UNIQUE KEY index_name_1 USING HASH (col_01) INVISIBLE";
    parseQuery(p_index, unique_key_07.data(), unique_key_07.data() + unique_key_07.size(), "", 0, 0);

    String foreign_key_01 = "FOREIGN KEY (col_01) REFERENCES tbl_name (col_01)";
    parseQuery(p_index, foreign_key_01.data(), foreign_key_01.data() + foreign_key_01.size(), "", 0, 0);

    String foreign_key_02 = "FOREIGN KEY index_name (col_01) REFERENCES tbl_name (col_01)";
    parseQuery(p_index, foreign_key_02.data(), foreign_key_02.data() + foreign_key_02.size(), "", 0, 0);

    String foreign_key_03 = "CONSTRAINT FOREIGN KEY index_name (col_01) REFERENCES tbl_name (col_01)";
    parseQuery(p_index, foreign_key_03.data(), foreign_key_03.data() + foreign_key_03.size(), "", 0, 0);

    String foreign_key_04 = "CONSTRAINT index_name FOREIGN KEY index_name_01 (col_01) REFERENCES tbl_name (col_01)";
    parseQuery(p_index, foreign_key_04.data(), foreign_key_04.data() + foreign_key_04.size(), "", 0, 0);
}
