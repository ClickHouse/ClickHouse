#include <gtest/gtest.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/MySQL/ASTDeclarePartition.h>
#include <Parsers/MySQL/ASTDeclareSubPartition.h>

using namespace DB;
using namespace DB::MySQLParser;

TEST(ParserPartition, AllPatitionOptions)
{
    String input = "PARTITION partition_name ENGINE = engine_name COMMENT 'partition comment'"
                   " INDEX DIRECTORY 'index_directory' DATA DIRECTORY 'data_directory' max_rows 1000 MIN_ROWs 0"
                   " TABLESPACE table_space_name";

    ParserDeclarePartition p_partition;
    ASTPtr ast = parseQuery(p_partition, input.data(), input.data() + input.size(), "", 0, 0);

    ASTDeclarePartition * declare_partition = ast->as<ASTDeclarePartition>();
    EXPECT_EQ(declare_partition->partition_name, "partition_name");
    ASTDeclareOptions * declare_options = declare_partition->options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options->changes["engine"]->as<ASTIdentifier>()->name(), "engine_name");
    EXPECT_EQ(declare_options->changes["comment"]->as<ASTLiteral>()->value.safeGet<String>(), "partition comment");
    EXPECT_EQ(declare_options->changes["data_directory"]->as<ASTLiteral>()->value.safeGet<String>(), "data_directory");
    EXPECT_EQ(declare_options->changes["index_directory"]->as<ASTLiteral>()->value.safeGet<String>(), "index_directory");
    EXPECT_EQ(declare_options->changes["min_rows"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 0);
    EXPECT_EQ(declare_options->changes["max_rows"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1000);
    EXPECT_EQ(declare_options->changes["tablespace"]->as<ASTIdentifier>()->name(), "table_space_name");
}

TEST(ParserPartition, OptionalPatitionOptions)
{
    String input = "PARTITION partition_name STORAGE engine = engine_name max_rows 1000 min_rows 0 tablespace table_space_name";
    ParserDeclarePartition p_partition;
    ASTPtr ast = parseQuery(p_partition, input.data(), input.data() + input.size(), "", 0, 0);

    ASTDeclarePartition * declare_partition = ast->as<ASTDeclarePartition>();
    EXPECT_EQ(declare_partition->partition_name, "partition_name");
    ASTDeclareOptions * declare_options = declare_partition->options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options->changes["engine"]->as<ASTIdentifier>()->name(), "engine_name");
    EXPECT_EQ(declare_options->changes["min_rows"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 0);
    EXPECT_EQ(declare_options->changes["max_rows"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1000);
    EXPECT_EQ(declare_options->changes["tablespace"]->as<ASTIdentifier>()->name(), "table_space_name");
}

TEST(ParserPartition, PatitionOptionsWithLessThan)
{
    ParserDeclarePartition p_partition;
    String partition_01 = "PARTITION partition_01 VALUES LESS THAN (1991) STORAGE engine = engine_name";
    ASTPtr ast_partition_01 = parseQuery(p_partition, partition_01.data(), partition_01.data() + partition_01.size(), "", 0, 0);

    ASTDeclarePartition * declare_partition_01 = ast_partition_01->as<ASTDeclarePartition>();
    EXPECT_EQ(declare_partition_01->partition_name, "partition_01");
    EXPECT_EQ(declare_partition_01->less_than->as<ASTLiteral>()->value.safeGet<UInt64>(), 1991);
    ASTDeclareOptions * declare_options_01 = declare_partition_01->options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options_01->changes["engine"]->as<ASTIdentifier>()->name(), "engine_name");

    String partition_02 = "PARTITION partition_02 VALUES LESS THAN MAXVALUE STORAGE engine = engine_name";
    ASTPtr ast_partition_02 = parseQuery(p_partition, partition_02.data(), partition_02.data() + partition_02.size(), "", 0, 0);

    ASTDeclarePartition * declare_partition_02 = ast_partition_02->as<ASTDeclarePartition>();
    EXPECT_EQ(declare_partition_02->partition_name, "partition_02");
    EXPECT_EQ(declare_partition_02->less_than->as<ASTIdentifier>()->name(), "MAXVALUE");
    ASTDeclareOptions * declare_options_02 = declare_partition_02->options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options_02->changes["engine"]->as<ASTIdentifier>()->name(), "engine_name");

    String partition_03 = "PARTITION partition_03 VALUES LESS THAN (50, MAXVALUE) STORAGE engine = engine_name";
    ASTPtr ast_partition_03 = parseQuery(p_partition, partition_03.data(), partition_03.data() + partition_03.size(), "", 0, 0);

    ASTDeclarePartition * declare_partition_03 = ast_partition_03->as<ASTDeclarePartition>();
    EXPECT_EQ(declare_partition_03->partition_name, "partition_03");
    ASTPtr declare_partition_03_argument = declare_partition_03->less_than->as<ASTFunction>()->arguments;
    EXPECT_EQ(declare_partition_03_argument->children[0]->as<ASTLiteral>()->value.safeGet<UInt64>(), 50);
    EXPECT_EQ(declare_partition_03_argument->children[1]->as<ASTIdentifier>()->name(), "MAXVALUE");
    ASTDeclareOptions * declare_options_03 = declare_partition_03->options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options_03->changes["engine"]->as<ASTIdentifier>()->name(), "engine_name");

    String partition_04 = "PARTITION partition_04 VALUES LESS THAN (MAXVALUE, MAXVALUE) STORAGE engine = engine_name";
    ASTPtr ast_partition_04 = parseQuery(p_partition, partition_04.data(), partition_04.data() + partition_04.size(), "", 0, 0);

    ASTDeclarePartition * declare_partition_04 = ast_partition_04->as<ASTDeclarePartition>();
    EXPECT_EQ(declare_partition_04->partition_name, "partition_04");
    ASTPtr declare_partition_04_argument = declare_partition_04->less_than->as<ASTFunction>()->arguments;
    EXPECT_EQ(declare_partition_04_argument->children[0]->as<ASTIdentifier>()->name(), "MAXVALUE");
    EXPECT_EQ(declare_partition_04_argument->children[1]->as<ASTIdentifier>()->name(), "MAXVALUE");
    ASTDeclareOptions * declare_options_04 = declare_partition_04->options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options_04->changes["engine"]->as<ASTIdentifier>()->name(), "engine_name");
}

TEST(ParserPartition, PatitionOptionsWithInExpression)
{
    ParserDeclarePartition p_partition;
    String partition_01 = "PARTITION partition_01 VALUES IN (NULL, 1991, MAXVALUE) STORAGE engine = engine_name";
    ASTPtr ast_partition_01 = parseQuery(p_partition, partition_01.data(), partition_01.data() + partition_01.size(), "", 0, 0);

    ASTDeclarePartition * declare_partition_01 = ast_partition_01->as<ASTDeclarePartition>();
    EXPECT_EQ(declare_partition_01->partition_name, "partition_01");
    ASTPtr declare_partition_01_argument = declare_partition_01->in_expression->as<ASTFunction>()->arguments;
    EXPECT_TRUE(declare_partition_01_argument->children[0]->as<ASTLiteral>()->value.isNull());
    EXPECT_EQ(declare_partition_01_argument->children[1]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1991);
    EXPECT_EQ(declare_partition_01_argument->children[2]->as<ASTIdentifier>()->name(), "MAXVALUE");
    ASTDeclareOptions * declare_options_01 = declare_partition_01->options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options_01->changes["engine"]->as<ASTIdentifier>()->name(), "engine_name");

    String partition_02 = "PARTITION partition_02 VALUES IN ((NULL, 1991), (1991, NULL), (MAXVALUE, MAXVALUE)) STORAGE engine = engine_name";
    ASTPtr ast_partition_02 = parseQuery(p_partition, partition_02.data(), partition_02.data() + partition_02.size(), "", 0, 0);

    ASTDeclarePartition * declare_partition_02 = ast_partition_02->as<ASTDeclarePartition>();
    EXPECT_EQ(declare_partition_02->partition_name, "partition_02");
    ASTPtr declare_partition_02_argument = declare_partition_02->in_expression->as<ASTFunction>()->arguments;

    ASTPtr argument_01 = declare_partition_02_argument->children[0];
    EXPECT_TRUE(argument_01->as<ASTLiteral>()->value.safeGet<Tuple>()[0].isNull());
    EXPECT_EQ(argument_01->as<ASTLiteral>()->value.safeGet<Tuple>()[1].safeGet<UInt64>(), 1991);

    ASTPtr argument_02 = declare_partition_02_argument->children[1];
    EXPECT_EQ(argument_02->as<ASTLiteral>()->value.safeGet<Tuple>()[0].safeGet<UInt64>(), 1991);
    EXPECT_TRUE(argument_02->as<ASTLiteral>()->value.safeGet<Tuple>()[1].isNull());

    ASTPtr argument_03 = declare_partition_02_argument->children[2]->as<ASTFunction>()->arguments;
    EXPECT_EQ(argument_03->as<ASTExpressionList>()->children[0]->as<ASTIdentifier>()->name(), "MAXVALUE");
    EXPECT_EQ(argument_03->as<ASTExpressionList>()->children[1]->as<ASTIdentifier>()->name(), "MAXVALUE");

    ASTDeclareOptions * declare_options_02 = declare_partition_02->options->as<ASTDeclareOptions>();
    EXPECT_EQ(declare_options_02->changes["engine"]->as<ASTIdentifier>()->name(), "engine_name");
}

TEST(ParserPartition, PatitionOptionsWithSubpartitions)
{
    ParserDeclarePartition p_partition;
    String partition_01 = "PARTITION partition_01 VALUES IN (NULL, 1991, MAXVALUE) STORAGE engine = engine_name (SUBPARTITION s_p01)";
    ASTPtr ast_partition_01 = parseQuery(p_partition, partition_01.data(), partition_01.data() + partition_01.size(), "", 0, 0);

    ASTDeclarePartition * declare_partition_01 = ast_partition_01->as<ASTDeclarePartition>();
    EXPECT_EQ(declare_partition_01->partition_name, "partition_01");
    EXPECT_TRUE(declare_partition_01->subpartitions->as<ASTExpressionList>()->children[0]->as<ASTDeclareSubPartition>());

    String partition_02 = "PARTITION partition_02 VALUES IN (NULL, 1991, MAXVALUE) STORAGE engine = engine_name (SUBPARTITION s_p01, SUBPARTITION s_p02)";
    ASTPtr ast_partition_02 = parseQuery(p_partition, partition_02.data(), partition_02.data() + partition_02.size(), "", 0, 0);

    ASTDeclarePartition * declare_partition_02 = ast_partition_02->as<ASTDeclarePartition>();
    EXPECT_EQ(declare_partition_02->partition_name, "partition_02");
    EXPECT_TRUE(declare_partition_02->subpartitions->as<ASTExpressionList>()->children[0]->as<ASTDeclareSubPartition>());
    EXPECT_TRUE(declare_partition_02->subpartitions->as<ASTExpressionList>()->children[1]->as<ASTDeclareSubPartition>());
}

