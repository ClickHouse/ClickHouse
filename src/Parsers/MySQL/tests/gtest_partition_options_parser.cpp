#include <gtest/gtest.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/MySQL/ASTDeclarePartition.h>
#include <Parsers/MySQL/ASTDeclarePartitionOptions.h>

using namespace DB;
using namespace DB::MySQLParser;

TEST(ParserPartitionOptions, HashPatitionOptions)
{
    String hash_partition = "PARTITION BY HASH(col_01)";

    ParserDeclarePartitionOptions p_partition_options;
    ASTPtr ast_01 = parseQuery(p_partition_options, hash_partition.data(), hash_partition.data() + hash_partition.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options_01 = ast_01->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options_01->partition_type, "hash");
    EXPECT_EQ(declare_partition_options_01->partition_expression->as<ASTIdentifier>()->name(), "col_01");

    String linear_hash_partition = "PARTITION BY LINEAR HASH(col_01)";
    ASTPtr ast_02 = parseQuery(p_partition_options, linear_hash_partition.data(), linear_hash_partition.data() + linear_hash_partition.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options_02 = ast_02->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options_02->partition_type, "linear_hash");
    EXPECT_EQ(declare_partition_options_02->partition_expression->as<ASTIdentifier>()->name(), "col_01");
}

TEST(ParserPartitionOptions, KeyPatitionOptions)
{
    String key_partition = "PARTITION BY KEY(col_01)";

    ParserDeclarePartitionOptions p_partition_options;
    ASTPtr ast_01 = parseQuery(p_partition_options, key_partition.data(), key_partition.data() + key_partition.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options_01 = ast_01->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options_01->partition_type, "key");
    EXPECT_EQ(declare_partition_options_01->partition_expression->as<ASTIdentifier>()->name(), "col_01");

    String linear_key_partition = "PARTITION BY LINEAR KEY(col_01, col_02)";
    ASTPtr ast_02 = parseQuery(p_partition_options, linear_key_partition.data(), linear_key_partition.data() + linear_key_partition.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options_02 = ast_02->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options_02->partition_type, "linear_key");
    ASTPtr columns_list = declare_partition_options_02->partition_expression->as<ASTFunction>()->arguments;
    EXPECT_EQ(columns_list->children.front()->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ((*++columns_list->children.begin())->as<ASTIdentifier>()->name(), "col_02");

    String key_partition_with_algorithm = "PARTITION BY KEY ALGORITHM=1 (col_01)";
    ASTPtr ast_03 = parseQuery(p_partition_options, key_partition_with_algorithm.data(), key_partition_with_algorithm.data() + key_partition_with_algorithm.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options_03 = ast_03->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options_03->partition_type, "key_1");
    EXPECT_EQ(declare_partition_options_03->partition_expression->as<ASTIdentifier>()->name(), "col_01");
}

TEST(ParserPartitionOptions, RangePatitionOptions)
{
    String range_partition = "PARTITION BY RANGE(col_01)";

    ParserDeclarePartitionOptions p_partition_options;
    ASTPtr ast_01 = parseQuery(p_partition_options, range_partition.data(), range_partition.data() + range_partition.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options_01 = ast_01->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options_01->partition_type, "range");
    EXPECT_EQ(declare_partition_options_01->partition_expression->as<ASTIdentifier>()->name(), "col_01");

    String range_columns_partition = "PARTITION BY RANGE COLUMNS(col_01, col_02)";
    ASTPtr ast_02 = parseQuery(p_partition_options, range_columns_partition.data(), range_columns_partition.data() + range_columns_partition.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options_02 = ast_02->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options_02->partition_type, "range");
    ASTPtr columns_list = declare_partition_options_02->partition_expression->as<ASTFunction>()->arguments;
    EXPECT_EQ(columns_list->children.front()->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ((*++columns_list->children.begin())->as<ASTIdentifier>()->name(), "col_02");
}

TEST(ParserPartitionOptions, ListPatitionOptions)
{
    String range_partition = "PARTITION BY LIST(col_01)";

    ParserDeclarePartitionOptions p_partition_options;
    ASTPtr ast_01 = parseQuery(p_partition_options, range_partition.data(), range_partition.data() + range_partition.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options_01 = ast_01->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options_01->partition_type, "list");
    EXPECT_EQ(declare_partition_options_01->partition_expression->as<ASTIdentifier>()->name(), "col_01");

    String range_columns_partition = "PARTITION BY LIST COLUMNS(col_01, col_02)";
    ASTPtr ast_02 = parseQuery(p_partition_options, range_columns_partition.data(), range_columns_partition.data() + range_columns_partition.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options_02 = ast_02->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options_02->partition_type, "list");
    ASTPtr columns_list = declare_partition_options_02->partition_expression->as<ASTFunction>()->arguments;
    EXPECT_EQ(columns_list->children.front()->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ((*++columns_list->children.begin())->as<ASTIdentifier>()->name(), "col_02");
}

TEST(ParserPartitionOptions, PatitionNumberOptions)
{
    String numbers_partition = "PARTITION BY KEY(col_01) PARTITIONS 2";

    ParserDeclarePartitionOptions p_partition_options;
    ASTPtr ast = parseQuery(p_partition_options, numbers_partition.data(), numbers_partition.data() + numbers_partition.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options = ast->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options->partition_type, "key");
    EXPECT_EQ(declare_partition_options->partition_expression->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ(declare_partition_options->partition_numbers->as<ASTLiteral>()->value.safeGet<UInt64>(), 2);
}

TEST(ParserPartitionOptions, PatitionWithSubpartitionOptions)
{
    String partition_with_subpartition = "PARTITION BY KEY(col_01) PARTITIONS 3 SUBPARTITION BY HASH(col_02) SUBPARTITIONS 4";

    ParserDeclarePartitionOptions p_partition_options;
    ASTPtr ast = parseQuery(p_partition_options, partition_with_subpartition.data(), partition_with_subpartition.data() + partition_with_subpartition.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options = ast->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options->partition_type, "key");
    EXPECT_EQ(declare_partition_options->partition_expression->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ(declare_partition_options->partition_numbers->as<ASTLiteral>()->value.safeGet<UInt64>(), 3);
    EXPECT_EQ(declare_partition_options->subpartition_type, "hash");
    EXPECT_EQ(declare_partition_options->subpartition_expression->as<ASTIdentifier>()->name(), "col_02");
    EXPECT_EQ(declare_partition_options->subpartition_numbers->as<ASTLiteral>()->value.safeGet<UInt64>(), 4);
}

TEST(ParserPartitionOptions, PatitionOptionsWithDeclarePartition)
{
    String partition_options_with_declare = "PARTITION BY KEY(col_01) PARTITIONS 3 SUBPARTITION BY HASH(col_02) SUBPARTITIONS 4 (PARTITION partition_name)";

    ParserDeclarePartitionOptions p_partition_options;
    ASTPtr ast = parseQuery(p_partition_options,
        partition_options_with_declare.data(),
        partition_options_with_declare.data() + partition_options_with_declare.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options = ast->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options->partition_type, "key");
    EXPECT_EQ(declare_partition_options->partition_expression->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ(declare_partition_options->partition_numbers->as<ASTLiteral>()->value.safeGet<UInt64>(), 3);
    EXPECT_EQ(declare_partition_options->subpartition_type, "hash");
    EXPECT_EQ(declare_partition_options->subpartition_expression->as<ASTIdentifier>()->name(), "col_02");
    EXPECT_EQ(declare_partition_options->subpartition_numbers->as<ASTLiteral>()->value.safeGet<UInt64>(), 4);
    EXPECT_TRUE(declare_partition_options->declare_partitions->as<ASTExpressionList>()->children.front()->as<ASTDeclarePartition>());
}

TEST(ParserPartitionOptions, PatitionOptionsWithDeclarePartitions)
{
    String partition_options_with_declare = "PARTITION BY KEY(col_01) PARTITIONS 3 SUBPARTITION BY HASH(col_02) SUBPARTITIONS 4 (PARTITION partition_01, PARTITION partition_02)";

    ParserDeclarePartitionOptions p_partition_options;
    ASTPtr ast = parseQuery(p_partition_options,
                            partition_options_with_declare.data(),
                            partition_options_with_declare.data() + partition_options_with_declare.size(), "", 0, 0);

    ASTDeclarePartitionOptions * declare_partition_options = ast->as<ASTDeclarePartitionOptions>();
    EXPECT_EQ(declare_partition_options->partition_type, "key");
    EXPECT_EQ(declare_partition_options->partition_expression->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ(declare_partition_options->partition_numbers->as<ASTLiteral>()->value.safeGet<UInt64>(), 3);
    EXPECT_EQ(declare_partition_options->subpartition_type, "hash");
    EXPECT_EQ(declare_partition_options->subpartition_expression->as<ASTIdentifier>()->name(), "col_02");
    EXPECT_EQ(declare_partition_options->subpartition_numbers->as<ASTLiteral>()->value.safeGet<UInt64>(), 4);
    EXPECT_TRUE(declare_partition_options->declare_partitions->as<ASTExpressionList>()->children.front()->as<ASTDeclarePartition>());
    EXPECT_TRUE((*++declare_partition_options->declare_partitions->as<ASTExpressionList>()->children.begin())->as<ASTDeclarePartition>());
}
