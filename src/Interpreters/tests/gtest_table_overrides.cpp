#include <Interpreters/applyTableOverride.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>

#include <iostream>
#include <gtest/gtest.h>

namespace
{
using namespace DB;
using namespace std::literals;
}


struct TableOverrideTestCase
{
    String create_database_query;
    String create_table_query;
    String expected_create_table_query;
};

std::ostream & operator<<(std::ostream & ostr, const TableOverrideTestCase & test_case)
{
    return ostr << "database: " << test_case.create_database_query << ", table: " << test_case.create_table_query
                << ", expected: " << test_case.expected_create_table_query;
}

class TableOverrideTest : public ::testing::TestWithParam<TableOverrideTestCase>
{};

TEST_P(TableOverrideTest, applyOverrides)
{
    const auto & [database_query, table_query, expected_query] = GetParam();
    ParserCreateQuery parser;
    ASTPtr database_ast;
    ASSERT_NO_THROW(database_ast = parseQuery(parser, database_query, 0, 0));
    auto * database = database_ast->as<ASTCreateQuery>();
    ASSERT_NE(nullptr, database);
    ASTPtr table_ast;
    ASSERT_NO_THROW(table_ast = parseQuery(parser, table_query, 0, 0));
    auto * table = table_ast->as<ASTCreateQuery>();
    ASSERT_NE(nullptr, table);
    auto table_name = table->table->as<ASTIdentifier>()->name();
    if (database->table_overrides)
    {
        auto override_ast = database->table_overrides->tryGetTableOverride(table_name);
        ASSERT_NE(nullptr, override_ast);
        auto * override_table_ast = override_ast->as<ASTTableOverride>();
        ASSERT_NE(nullptr, override_table_ast);
        applyTableOverrideToCreateQuery(*override_table_ast, table);
    }
    EXPECT_EQ(expected_query, serializeAST(*table));
}

INSTANTIATE_TEST_SUITE_P(ApplyTableOverrides, TableOverrideTest,
    ::testing::ValuesIn(std::initializer_list<TableOverrideTestCase>{
    {
        "CREATE DATABASE db",
        "CREATE TABLE db.t (id Int64) ENGINE=Log",
        "CREATE TABLE db.t (`id` Int64) ENGINE = Log"
    },
    {
        "CREATE DATABASE db TABLE OVERRIDE t (PARTITION BY tuple())",
        "CREATE TABLE db.t (id Int64) ENGINE=MergeTree",
        "CREATE TABLE db.t (`id` Int64) ENGINE = MergeTree PARTITION BY tuple()"
    },
    {
        "CREATE DATABASE db TABLE OVERRIDE t (COLUMNS (id UInt64 CODEC(Delta), shard UInt8 ALIAS modulo(id, 16)) PARTITION BY shard)",
        "CREATE TABLE db.t (id Int64) ENGINE=MergeTree",
        "CREATE TABLE db.t (`id` UInt64 CODEC(Delta), `shard` UInt8 ALIAS id % 16) ENGINE = MergeTree PARTITION BY shard"
    },
    {
        "CREATE DATABASE db TABLE OVERRIDE a (PARTITION BY modulo(id, 3)), TABLE OVERRIDE b (PARTITION BY modulo(id, 5))",
        "CREATE TABLE db.a (id Int64) ENGINE=MergeTree",
        "CREATE TABLE db.a (`id` Int64) ENGINE = MergeTree PARTITION BY id % 3"
    },
    {
        "CREATE DATABASE db TABLE OVERRIDE a (PARTITION BY modulo(id, 3)), TABLE OVERRIDE b (PARTITION BY modulo(id, 5))",
        "CREATE TABLE db.b (id Int64) ENGINE=MergeTree",
        "CREATE TABLE db.b (`id` Int64) ENGINE = MergeTree PARTITION BY id % 5"
    },
    {
        "CREATE DATABASE db TABLE OVERRIDE `tbl` (PARTITION BY toYYYYMM(created))",
        "CREATE TABLE db.tbl (id Int64, created DateTime) ENGINE=Foo",
        "CREATE TABLE db.tbl (`id` Int64, `created` DateTime) ENGINE = Foo PARTITION BY toYYYYMM(created)",
    }
}));
